package repl

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// DefaultKeyAttr is the attribute a row's primary key is stored under when none
// is configured.
const DefaultKeyAttr = "Key"

// DefaultTable is the table the shell starts on and targets when none is named.
const DefaultTable = dbrpc.DefaultTable

// ExecConfig configures an Executor.
type ExecConfig struct {
	// KeyAttr is the ad attribute that carries a row's primary key (the db key).
	// Defaults to DefaultKeyAttr. INSERT stamps the key here; UPDATE and DELETE
	// recover a matched row's key from it.
	KeyAttr string

	// GenKey generates a key for an INSERT that does not supply the key column.
	// Defaults to a monotonic "row-<n>" generator.
	GenKey func() string

	// ApplyBatch, if set, replaces the local dbrpc transaction as the write path:
	// INSERT/UPDATE/DELETE build a batch of WriteOps and hand it to ApplyBatch
	// instead of committing over the dbrpc session. This routes writes through a
	// consistent-mode cluster (the CLI wires it to a consistent.ControlClient that
	// proposes the batch to raft and follows leader redirects). Reads still use
	// the dbrpc client. When nil, writes commit locally over dbrpc.
	ApplyBatch func([]WriteOp) error

	// Resync, if set, backs the `.resync <target>` command: it asks the daemon to re-read/re-export
	// a sync source (a schedd-sync tailer "jobs"/"history", or an exporter by name) from the start.
	// The CLI wires it to a DBSyncControl dial. When nil, `.resync` reports it is unavailable.
	Resync func(target string) error
}

// WriteKind identifies a mutation in a write batch.
type WriteKind int

const (
	// WNewClassAd stores Value (old-ClassAd text) under Key.
	WNewClassAd WriteKind = iota
	// WSetAttribute sets Key's attribute Name to the ClassAd expression Value.
	WSetAttribute
	// WDestroyClassAd removes Key.
	WDestroyClassAd
)

// WriteOp is one mutation produced by an INSERT/UPDATE/DELETE.
type WriteOp struct {
	Kind  WriteKind
	Key   string
	Name  string
	Value string
}

// Executor runs parsed statements against a dbrpc client.
type Executor struct {
	c          *dbrpc.Client
	keyAttr    string
	genKey     func() string
	applyBatch func([]WriteOp) error
	resync     func(target string) error

	// wireRowsOff forces reads onto the old-ClassAd text path even where the wire-form
	// relay would serve them. It exists so a test can run the same query over both
	// transports and compare, and as an escape hatch if a wire row ever decodes wrong in
	// the field. Not settable from config.
	wireRowsOff bool

	// archives caches the set of append-only (history) table names, so a SELECT can be routed
	// to the archive query path -- archives are not mutable tables and the regular query op
	// does not resolve them. Loaded lazily; archivesOK gates a successful load so a transient
	// list error just retries next time.
	archives   map[string]bool
	archivesOK bool

	// Explicit-transaction state, set by BEGIN and cleared by COMMIT/ROLLBACK. txActive
	// is the flag BEGIN sets; tx is the server-side transaction, opened lazily on the
	// first write because a dbrpc transaction needs a table and BEGIN does not name one;
	// txTable is the table it bound to. txBuf accumulates ops in consistent mode, where
	// there is no server-side transaction to hold open. See stage.
	txActive bool
	tx       *dbrpc.Tx
	txTable  string
	txBuf    []WriteOp
}

// isArchive reports whether table is an append-only history table, loading (and caching) the
// archive-table set from the server on first use.
func (e *Executor) isArchive(table string) bool {
	if !e.archivesOK {
		names, err := e.c.ArchiveTables(context.Background())
		if err != nil {
			return false // couldn't list; treat as a normal table (retry next call)
		}
		e.archives = make(map[string]bool, len(names))
		for _, n := range names {
			e.archives[n] = true
		}
		e.archivesOK = true
	}
	return e.archives[table]
}

// NewExecutor builds an Executor over an established dbrpc client.
func NewExecutor(c *dbrpc.Client, cfg ExecConfig) *Executor {
	keyAttr := cfg.KeyAttr
	if keyAttr == "" {
		keyAttr = DefaultKeyAttr
	}
	genKey := cfg.GenKey
	if genKey == nil {
		var seq atomic.Uint64
		genKey = func() string { return fmt.Sprintf("row-%d", seq.Add(1)) }
	}
	return &Executor{c: c, keyAttr: keyAttr, genKey: genKey, applyBatch: cfg.ApplyBatch, resync: cfg.Resync}
}

// Resync asks the daemon to re-read/re-export the named sync source ("jobs", "history", or an
// exporter name) from the start. Returns an error if no resync transport is configured.
func (e *Executor) Resync(target string) error {
	if e.resync == nil {
		return fmt.Errorf("resync is not available in this session")
	}
	return e.resync(target)
}

// commit applies a batch of write ops to table. Inside an explicit transaction (BEGIN)
// the ops are staged and applied at COMMIT; otherwise each statement commits on its own,
// through ApplyBatch (consistent mode) if configured, else as one local dbrpc transaction.
func (e *Executor) commit(table string, ops []WriteOp) error {
	if e.txActive {
		return e.stage(table, ops)
	}
	if e.applyBatch != nil {
		return e.applyBatch(ops)
	}
	tx, err := e.c.BeginTable(context.Background(), table)
	if err != nil {
		return err
	}
	if err := applyOps(tx, ops); err != nil {
		_ = tx.Abort(context.Background())
		return err
	}
	return tx.Commit(context.Background())
}

// applyOps issues a batch of write ops against an open transaction, without committing.
func applyOps(tx *dbrpc.Tx, ops []WriteOp) error {
	for _, op := range ops {
		var err error
		switch op.Kind {
		case WNewClassAd:
			err = tx.NewClassAd(context.Background(), op.Key, op.Value)
		case WSetAttribute:
			err = tx.SetAttribute(context.Background(), op.Key, op.Name, op.Value)
		case WDestroyClassAd:
			err = tx.DestroyClassAd(context.Background(), op.Key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- explicit transactions (BEGIN / COMMIT / ROLLBACK) ---

// stage adds a statement's writes to the open transaction.
//
// The transaction binds to the first table written, not to BEGIN: a dbrpc transaction is
// scoped to one table (opBegin carries the table name), and BEGIN does not name one. A
// later statement against a different table is refused rather than silently split across
// two transactions, which would not be atomic.
func (e *Executor) stage(table string, ops []WriteOp) error {
	if e.txTable == "" {
		e.txTable = table
	} else if !strings.EqualFold(e.txTable, table) {
		return fmt.Errorf(
			"this transaction is writing to table %q; a transaction cannot span tables "+
				"(COMMIT or ROLLBACK before writing to %q)", e.txTable, table)
	}

	// Consistent mode proposes a whole batch to raft at once, so there is no server-side
	// transaction to hold open; accumulate and propose the lot at COMMIT instead.
	if e.applyBatch != nil {
		e.txBuf = append(e.txBuf, ops...)
		return nil
	}

	if e.tx == nil {
		tx, err := e.c.BeginTable(context.Background(), table)
		if err != nil {
			return err
		}
		e.tx = tx
	}
	return applyOps(e.tx, ops)
}

// execBegin opens an explicit transaction.
func (e *Executor) execBegin() (*Result, error) {
	if e.txActive {
		return nil, fmt.Errorf("a transaction is already open (COMMIT or ROLLBACK it first); nested transactions are not supported")
	}
	e.txActive = true
	return &Result{Note: "BEGIN"}, nil
}

// execCommit applies the open transaction's writes.
//
// A transaction that wrote nothing commits successfully without any server round trip --
// there is no transaction to apply, and failing would penalize the common
// BEGIN/SELECT/COMMIT shape a connection in non-autocommit mode produces.
func (e *Executor) execCommit() (*Result, error) {
	if !e.txActive {
		return nil, fmt.Errorf("no transaction is open (COMMIT without BEGIN)")
	}
	tx, buf := e.tx, e.txBuf
	e.resetTxn()

	switch {
	case tx != nil:
		if err := tx.Commit(context.Background()); err != nil {
			return nil, err
		}
	case len(buf) > 0:
		if err := e.applyBatch(buf); err != nil {
			return nil, err
		}
	}
	return &Result{Note: "COMMIT"}, nil
}

// execRollback discards the open transaction's writes.
func (e *Executor) execRollback() (*Result, error) {
	if !e.txActive {
		return nil, fmt.Errorf("no transaction is open (ROLLBACK without BEGIN)")
	}
	tx := e.tx
	e.resetTxn()

	if tx != nil {
		// The transaction is discarded either way: a failed Abort leaves it for the
		// server's idle reaper (or the connection close), and reporting the error would
		// only make a caller believe its writes might still land.
		_ = tx.Abort(context.Background())
	}
	return &Result{Note: "ROLLBACK"}, nil
}

// txReads reports whether a read of table should go through the open transaction.
//
// Only the transaction's own table: a dbrpc transaction is scoped to one table, and it
// binds to the first table WRITTEN, so a SELECT against any other table has to read the
// committed store. Consistent mode buffers writes client-side rather than opening a
// server-side transaction (e.tx stays nil), so a read there also sees committed state --
// a statement cannot observe writes the raft proposal has not carried yet.
func (e *Executor) txReads(table string) bool {
	return e.tx != nil && strings.EqualFold(e.txTable, table)
}

// InTransaction reports whether an explicit transaction is open.
func (e *Executor) InTransaction() bool { return e.txActive }

// resetTxn clears transaction state, leaving the executor in autocommit mode.
func (e *Executor) resetTxn() {
	e.txActive = false
	e.tx = nil
	e.txTable = ""
	e.txBuf = nil
}

// Result is the outcome of executing a statement.
type Result struct {
	IsSelect bool
	Columns  []string   // SELECT column headers
	Rows     [][]string // SELECT rows (cells aligned to Columns)
	Affected int        // rows written by INSERT/UPDATE/DELETE
	Note     string     // human-readable summary line (e.g. "UPDATE 3")

	// Ads are the matched ads of a plain (non-aggregate) SELECT, in result
	// order and after LIMIT. They back the JSON / ClassAd output formats, which
	// serialize whole ads rather than a projected table. Nil for aggregates.
	Ads []*classad.ClassAd
	// Star is true when the SELECT was `SELECT *`.
	Star bool
	// Duration is the wall-clock time to execute the statement (set by ExecString).
	Duration time.Duration
}

// Exec executes one statement.
func (e *Executor) Exec(st *Statement) (*Result, error) {
	switch st.Kind {
	case StmtSelect:
		return e.execSelect(st)
	case StmtInsert:
		return e.execInsert(st)
	case StmtUpdate:
		return e.execUpdate(st)
	case StmtDelete:
		return e.execDelete(st)
	case StmtCreateTable:
		return e.execCreateTable(st)
	case StmtDropTable:
		return e.execDropTable(st)
	case StmtCreateIndex:
		return e.execCreateIndex(st)
	case StmtDropIndex:
		return e.execDropIndex(st)
	case StmtCreateView:
		return e.execCreateView(st)
	case StmtDropView:
		return e.execDropView(st)
	case StmtMatch:
		return e.execMatch(st)
	case StmtBegin:
		return e.execBegin()
	case StmtCommit:
		return e.execCommit()
	case StmtRollback:
		return e.execRollback()
	default:
		return nil, fmt.Errorf("unknown statement kind")
	}
}

// --- DDL ---

func (e *Executor) execCreateTable(st *Statement) (*Result, error) {
	if st.InMemory {
		if err := e.c.CreateTableInMemory(context.Background(), st.Table); err != nil {
			return nil, err
		}
		return &Result{Note: "CREATE TABLE " + st.Table + " MEMORY"}, nil
	}
	if err := e.c.CreateTable(context.Background(), st.Table); err != nil {
		return nil, err
	}
	return &Result{Note: "CREATE TABLE " + st.Table}, nil
}

func (e *Executor) execDropTable(st *Statement) (*Result, error) {
	if err := e.c.DropTable(context.Background(), st.Table); err != nil {
		return nil, err
	}
	return &Result{Note: "DROP TABLE " + st.Table}, nil
}

// defaultViewCardinality caps the number of distinct groups (label combinations) a
// materialized view may hold when the definition omits an explicit MAXSERIES clause.
const defaultViewCardinality = 10000

func (e *Executor) execCreateView(st *Statement) (*Result, error) {
	spec, err := viewSpecFromSelect(st)
	if err != nil {
		return nil, err
	}
	if err := e.c.CreateView(context.Background(), st.ViewName, spec); err != nil {
		return nil, err
	}
	return &Result{Note: "CREATE MATERIALIZED VIEW " + st.ViewName}, nil
}

func (e *Executor) execDropView(st *Statement) (*Result, error) {
	if err := e.c.DropView(context.Background(), st.ViewName); err != nil {
		return nil, err
	}
	return &Result{Note: "DROP MATERIALIZED VIEW " + st.ViewName}, nil
}

// viewSpecFromSelect turns the embedded SELECT of a CREATE MATERIALIZED VIEW into a
// db.ViewSpec. Non-aggregate projected columns become the grouping labels and must match the
// GROUP BY set; aggregate columns (COUNT/SUM/AVG only) become the maintained metrics. The
// column names carry their aliases (e.g. label_owner, metric_jobs), which the Prometheus
// exporter interprets by prefix.
func viewSpecFromSelect(st *Statement) (db.ViewSpec, error) {
	sel := st.ViewSelect
	if sel == nil || sel.Kind != StmtSelect {
		return db.ViewSpec{}, fmt.Errorf("a materialized view must be defined by a SELECT")
	}
	if sel.Table == "" {
		return db.ViewSpec{}, fmt.Errorf("a materialized view requires a FROM table")
	}
	if len(sel.GroupBy) == 0 {
		return db.ViewSpec{}, fmt.Errorf("a materialized view requires GROUP BY")
	}
	if sel.Where != "" || len(sel.OrderBy) != 0 || sel.Limit != 0 {
		return db.ViewSpec{}, fmt.Errorf("a materialized view SELECT may not use WHERE, ORDER BY, or LIMIT")
	}

	// A view stores raw metrics that the reader combines later, so a metric must be a plain
	// aggregate -- there is nowhere to put the surrounding arithmetic.
	if err := rejectAggExprs(sel.Items, "a materialized view"); err != nil {
		return db.ViewSpec{}, err
	}
	// A view is refreshed incrementally into stored metrics; there is no point at which a
	// post-aggregation filter could be applied without changing what the view means.
	if sel.Having != "" {
		return db.ViewSpec{}, fmt.Errorf("a materialized view does not support HAVING; " +
			"filter when querying the view instead")
	}
	var groups []db.ViewGroupCol
	var metrics []db.ViewMetric
	for _, it := range sel.Items {
		if it.Star {
			return db.ViewSpec{}, fmt.Errorf("SELECT * is not allowed in a materialized view; project explicit columns")
		}
		if it.IsAggregate() {
			fn, err := viewAggFunc(it.Agg)
			if err != nil {
				return db.ViewSpec{}, err
			}
			metrics = append(metrics, db.ViewMetric{Func: fn, Arg: it.Col, Alias: it.header()})
			continue
		}
		// A time_bucket(attr, 'w') group column carries its width, so the view groups
		// by the floored timestamp -- a continuous aggregate (time series) rather than a
		// current-state gauge.
		groups = append(groups, db.ViewGroupCol{Attr: it.Col, Alias: it.header(), BucketWidth: it.BucketWidth})
	}
	if len(metrics) == 0 {
		return db.ViewSpec{}, fmt.Errorf("a materialized view requires at least one aggregate (COUNT, SUM, or AVG)")
	}
	// Every non-aggregate projected column must be a GROUP BY column, and vice versa, so the
	// view's grouping and its labels agree.
	if err := groupsMatchGroupBy(groups, sel.GroupBy); err != nil {
		return db.ViewSpec{}, err
	}

	card := st.ViewMaxSeries
	if card <= 0 {
		card = defaultViewCardinality
	}
	spec := db.ViewSpec{
		BaseTable:   sel.Table,
		Groups:      groups,
		Metrics:     metrics,
		Cardinality: card,
		SelectText:  renderViewSelect(sel),
		Grace:       st.ViewGrace,
		Retention:   st.ViewRetention,
	}
	// grace/retention only apply to a continuous aggregate (a view with a time_bucket).
	if (spec.Grace > 0 || spec.Retention > 0) && !spec.IsContinuous() {
		return db.ViewSpec{}, fmt.Errorf("WITH (grace/retention) applies only to a continuous aggregate (a view with a time_bucket GROUP BY)")
	}
	return spec, nil
}

// groupsMatchGroupBy verifies the projected non-aggregate columns are exactly the GROUP BY
// columns (order-independent).
func groupsMatchGroupBy(groups []db.ViewGroupCol, groupBy []string) error {
	if len(groups) != len(groupBy) {
		return fmt.Errorf("every non-aggregate column must appear in GROUP BY and vice versa")
	}
	want := make(map[string]bool, len(groupBy))
	for _, g := range groupBy {
		want[g] = true
	}
	for _, g := range groups {
		// A time-bucketed column appears in GROUP BY as its canonical time_bucket key,
		// not its raw attribute name.
		key := g.Attr
		if g.BucketWidth > 0 {
			key = canonicalBucketKey(g.Attr, g.BucketWidth)
		}
		if !want[key] {
			return fmt.Errorf("column %q is projected but not in GROUP BY", g.Attr)
		}
	}
	return nil
}

func viewAggFunc(name string) (db.ViewAggFunc, error) {
	switch strings.ToUpper(name) {
	case "COUNT":
		return db.ViewCount, nil
	case "SUM":
		return db.ViewSum, nil
	case "AVG":
		return db.ViewAvg, nil
	case "MIN", "MAX":
		return "", fmt.Errorf("%s is not supported in a materialized view: the change stream has no before-image, so it cannot be maintained on delete; use COUNT, SUM, or AVG", strings.ToUpper(name))
	default:
		return "", fmt.Errorf("unsupported aggregate %q in a materialized view", name)
	}
}

// renderViewSelect reconstructs a readable form of the view's defining SELECT for display
// (the parser does not retain the raw source text).
func renderViewSelect(sel *Statement) string {
	items := make([]string, 0, len(sel.Items))
	for _, it := range sel.Items {
		items = append(items, it.header())
	}
	return fmt.Sprintf("SELECT %s FROM %s GROUP BY %s",
		strings.Join(items, ", "), sel.Table, strings.Join(sel.GroupBy, ", "))
}

func (e *Executor) execCreateIndex(st *Statement) (*Result, error) {
	action := "index.add.value"
	if st.IndexKind == "categorical" {
		action = "index.add.categorical"
	}
	msg, err := e.c.AdminTable(context.Background(), st.Table, action, st.Columns...)
	if err != nil {
		return nil, err
	}
	return &Result{Note: msg}, nil
}

func (e *Executor) execDropIndex(st *Statement) (*Result, error) {
	msg, err := e.c.AdminTable(context.Background(), st.Table, "index.drop", st.Columns...)
	if err != nil {
		return nil, err
	}
	return &Result{Note: msg}, nil
}

// execMatch runs cross-table matchmaking as a greedy assignment: walking the
// requests in st.Table matching the request-side WHERE (or the single KEY), it
// gives each one the best-ranked bilaterally-matching resource in st.MatchResource
// that no earlier request has claimed, with the resource-side filter (WHERE TARGET)
// pushed down. LIMIT bounds the number of requests assigned. One row per request
// (Resource empty when it could not be placed).
func (e *Executor) execMatch(st *Statement) (*Result, error) {
	reqWhere := st.Where
	if st.Key != "" {
		kf := fmt.Sprintf("%s == %s", e.keyAttr, quoteClassAd(st.Key))
		if reqWhere == "" {
			reqWhere = kf
		} else {
			reqWhere = "(" + reqWhere + ") && " + kf
		}
	}
	// NOPREEMPT excludes resources already claimed by a job (so a placement never
	// requires preempting a running job), as an extra resource-side filter.
	targetWhere := st.TargetWhere
	if st.NoPreempt {
		const free = `State =!= "Claimed"`
		if targetWhere == "" {
			targetWhere = free
		} else {
			targetWhere = "(" + targetWhere + ") && (" + free + ")"
		}
	}
	rows, err := e.c.MatchTables(context.Background(), st.Table, st.MatchResource, e.keyAttr, reqWhere, targetWhere, st.Limit, st.MatchUsing)
	if err != nil {
		return nil, err
	}
	res := &Result{IsSelect: true, Columns: []string{"Request", "Resource", "Rank"}}
	for _, m := range rows {
		res.Rows = append(res.Rows, []string{m.Request, m.Resource, m.Rank})
	}
	return res, nil
}

// ExecString parses then executes a single statement, timing the execution
// (Result.Duration).
func (e *Executor) ExecString(s string) (*Result, error) {
	st, err := Parse(s)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	res, err := e.Exec(st)
	if res != nil {
		res.Duration = time.Since(start)
	}
	return res, err
}

// Diagnostics returns a table's storage stats, hot set, indexes, and tuning
// suggestions (the .stats/.indexes/.hot commands).
func (e *Executor) Diagnostics(table string) (*dbrpc.Diagnostics, error) {
	return e.c.DiagnosticsTable(context.Background(), table)
}

// Explain reports how a table would execute a constraint query (.explain).
func (e *Executor) Explain(table, constraint string) (*db.QueryExplain, error) {
	return e.c.ExplainTable(context.Background(), table, constraint)
}

// MatchExplain reports the matchmaking plan for the request st identifies (its KEY,
// or the first ad matching its WHERE) in st.Table against st.MatchResource: how the
// job's Requirements rewrite over the slot and which probes an index prunes.
func (e *Executor) MatchExplain(st *Statement) (*db.MatchExplain, error) {
	selector := st.Where
	if st.Key != "" {
		kf := fmt.Sprintf("%s == %s", e.keyAttr, quoteClassAd(st.Key))
		if selector == "" {
			selector = kf
		} else {
			selector = "(" + selector + ") && " + kf
		}
	}
	// The resource-side filter shown in the explain must match execMatch: WHERE TARGET
	// plus NOPREEMPT's `State =!= "Claimed"`.
	targetWhere := st.TargetWhere
	if st.NoPreempt {
		const free = `State =!= "Claimed"`
		if targetWhere == "" {
			targetWhere = free
		} else {
			targetWhere = "(" + targetWhere + ") && (" + free + ")"
		}
	}
	return e.c.MatchExplain(context.Background(), st.Table, selector, st.MatchResource, targetWhere)
}

// Admin runs an index/hot-set management action on a table, returning the
// server's message.
func (e *Executor) Admin(table, action string, args ...string) (string, error) {
	return e.c.AdminTable(context.Background(), table, action, args...)
}

// Tables lists the catalog's table names.
func (e *Executor) Tables() ([]string, error) {
	tables, err := e.c.Tables(context.Background())
	if err != nil {
		return nil, err
	}
	// Include append-only history tables so `.tables` and completion surface them -- they are a
	// distinct namespace the regular table list omits, and are otherwise invisible.
	archives, aerr := e.c.ArchiveTables(context.Background())
	if aerr == nil {
		tables = append(tables, archives...)
	}
	return tables, nil
}

// tableExists reports whether name is a known table -- a mutable table or an append-only
// archive (history) table. Used to peel an optional leading table name off a maintenance
// meta-command (e.g. `.retrain history`).
func (e *Executor) tableExists(name string) bool {
	if e.isArchive(name) { // cached
		return true
	}
	names, err := e.c.Tables(context.Background())
	if err != nil {
		return false
	}
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

// ListViews returns the materialized view names.
func (e *Executor) ListViews() ([]string, error) { return e.c.ListViews(context.Background()) }

// ViewRows returns the current rows of a materialized view (one ad per group). Views are
// read like tables, so this reuses the ordinary query path against the view's backing.
func (e *Executor) ViewRows(name string) ([]*classad.ClassAd, error) {
	return e.queryAds(name, "", 0)
}

// ListExporters returns the registered external-sink exporters (name + kind). This is safe
// for an unprivileged connection; the config (which may hold credentials) is not returned.
func (e *Executor) ListExporters() ([]dbrpc.ExporterInfo, error) {
	return e.c.ListExporters(context.Background())
}

// Exporter returns a single exporter's full definition (including its opaque config). The
// server gates this to DAEMON connections, so an unprivileged client gets an error.
func (e *Executor) Exporter(name string) (db.ExporterDef, bool, error) {
	return e.c.GetExporter(context.Background(), name)
}

// ExporterStateSize reports whether an exporter has checkpointed resume state and, if so,
// its size in bytes. The blob itself is opaque to the CLI (owned by the exporter process).
func (e *Executor) ExporterStateSize(name string) (int, bool, error) {
	blob, ok, err := e.c.GetExporterState(context.Background(), name)
	return len(blob), ok, err
}

// CreateTable creates a table (used by load auto-routing).
func (e *Executor) CreateTable(name string) error { return e.c.CreateTable(context.Background(), name) }

// CreateTableInMemory creates a RAM-only table (data not persisted across a server restart).
func (e *Executor) CreateTableInMemory(name string) error {
	return e.c.CreateTableInMemory(context.Background(), name)
}

// ConvertTableToMemory drops an existing table's on-disk backing (DAEMON-only), keeping its
// current contents in RAM only.
func (e *Executor) ConvertTableToMemory(name string) error {
	return e.c.ConvertTableToMemory(context.Background(), name)
}

// WatchStream opens a live change stream on a table from cursor (nil ⇒ replay the current
// contents first), returning the event channel and a stop function (which cancels the
// server-side watch; also called on connection close).
func (e *Executor) WatchStream(table string, cursor []byte) (<-chan dbrpc.WatchEvent, func(), error) {
	return e.c.WatchTable(context.Background(), table, cursor)
}

// WatchHead returns an opaque cursor at the table's current change-log head, so a watch
// from it streams only subsequent changes (SINCE NOW) with no replay of current contents.
func (e *Executor) WatchHead(table string) ([]byte, error) {
	return e.c.WatchHead(context.Background(), table)
}

// constraint returns the WHERE constraint, defaulting to match-all.
func constraint(where string) string {
	if strings.TrimSpace(where) == "" {
		return "true"
	}
	return where
}

// queryAds runs the WHERE query against table and parses each returned ad.
// limit > 0 pushes a row cap to the server so it stops the scan early (0 = all).
func (e *Executor) queryAds(table, where string, limit int) ([]*classad.ClassAd, error) {
	return e.queryAdsAsOf(table, where, limit, "")
}

// queryAdsAsOf is queryAds with an optional point-in-time instant (asOf). When asOf is
// empty it reads the current state; otherwise it parses asOf and issues a time-travel
// query. asOf accepts RFC3339, "2006-01-02 15:04:05", or a relative look-back ("-1h").
func (e *Executor) queryAdsAsOf(table, where string, limit int, asOf string) ([]*classad.ClassAd, error) {
	var texts []string
	var err error
	switch {
	case e.isArchive(table):
		// History (append-only) tables are not mutable tables; the regular query op cannot
		// resolve them. Route to the archive query path (newest-first, limit-capped), which is
		// how the archived job history becomes visible to SELECT at all.
		if asOf != "" {
			return nil, fmt.Errorf("AS OF is not supported on the append-only %q table", table)
		}
		texts, err = e.c.ArchiveQuery(context.Background(), table, constraint(where), limit)
	case asOf == "":
		// Inside a transaction, read through it so the statement sees the transaction's
		// own uncommitted writes -- the connection-level op reads the committed store and
		// would report an INSERT made moments earlier as missing.
		if e.txReads(table) {
			texts, err = e.tx.Query(context.Background(), constraint(where), limit)
			break
		}
		// Whole ads over the wire relay when the table can serve them: no projection, so
		// the row is self-contained by construction and the ref question does not arise.
		if e.wireEligible(table) {
			ads, werr := e.queryAdsWire(table, where, nil, limit)
			if werr == nil {
				return ads, nil
			}
			if !wireFallback(werr) {
				return nil, werr
			}
		}
		texts, err = e.c.QueryTable(context.Background(), table, constraint(where), limit)
	default:
		var at time.Time
		if at, err = parseAsOf(asOf); err != nil {
			return nil, err
		}
		texts, err = e.c.QueryAsOfTable(context.Background(), table, constraint(where), limit, at)
	}
	if err != nil {
		return nil, err
	}
	ads := make([]*classad.ClassAd, 0, len(texts))
	for _, t := range texts {
		// The server streams ads in the bracketed new-ClassAd format (ClassAd.String).
		ad, err := classad.Parse(t)
		if err != nil {
			return nil, fmt.Errorf("parsing a returned ad: %w", err)
		}
		ads = append(ads, ad)
	}
	return ads, nil
}

// projectionAttrs returns the attribute set to push to the server for a plain-column SELECT,
// or nil to fetch whole ads. Projecting sends (and decodes) only the needed attributes rather
// than every attribute of a wide job ad -- a large wire + CPU saving for narrow SELECTs.
//
// It applies only to the safe, common case: a current-time (non AS OF), non-DISTINCT,
// non-star SELECT of plain columns over a mutable table or a history archive (the server
// projection op serves both, an archive newest-first). The WHERE is evaluated server-side,
// so only the SELECT and ORDER BY columns are read client-side and need to cross the wire.
// SELECT *, expression columns, and time_bucket columns may reference arbitrary attributes
// (fetch the whole ad); AS OF has no projected query variant.
//
// Projected columns are evaluated against the projected attributes (HTCondor's projection
// semantics): an attribute whose stored value references a sibling that was not projected
// evaluates to undefined. In practice job/history display columns are literal-valued, so the
// result is identical to a whole-ad fetch.
func (e *Executor) projectionAttrs(st *Statement) []string {
	if st.AsOf != "" || st.Distinct {
		return nil
	}
	// Inside a transaction, no projection: the projected query op is connection-level and
	// reads the COMMITTED store, so a SELECT after an INSERT in the same transaction would
	// not see its own write -- and would report it as missing rather than fail. The
	// transactional read op has no projected variant, so the transaction gets whole ads.
	// The projection is an optimization; correctness comes first. (The streaming path and
	// the aggregate path already make this same call.)
	if e.txReads(st.Table) {
		return nil
	}
	if len(st.Items) == 0 || (len(st.Items) == 1 && st.Items[0].Star) {
		return nil
	}
	seen := map[string]bool{}
	var attrs []string
	add := func(name string) {
		if k := strings.ToLower(name); name != "" && !seen[k] {
			seen[k] = true
			attrs = append(attrs, name)
		}
	}
	plainCol := func(it SelectItem) bool {
		// A window column's Col is the call as written, not an attribute, and the window
		// itself reads whatever PARTITION BY / ORDER BY name -- so a windowed SELECT fetches
		// whole ads rather than a projection.
		return !it.Star && !it.IsAggregate() && !it.Bucket && it.Window == "" &&
			it.Expr == "" && it.Col != ""
	}
	for _, it := range st.Items {
		if !plainCol(it) {
			return nil // may reference arbitrary attributes: fetch the whole ad
		}
		add(it.Col)
	}
	for _, t := range st.OrderBy {
		if !plainCol(t.Item) {
			return nil // ordering by an expression/aggregate needs the whole ad
		}
		add(t.Item.Col)
	}
	return attrs
}

// scanAttrs returns the attributes a client-side scan actually reads from each fetched row --
// the group keys, the aggregate arguments and their filters, the window's partition/order
// terms -- or nil to fetch whole ads.
//
// The client-side paths (a computed GROUP BY key, a time_bucket fallback, a ranking window)
// cannot push their work to the server, so every matching row crosses the wire and is parsed
// here. On a wide job ad that parse IS the query: grouping 20k ads by a CASE expression spent
// 90% of its time in the ClassAd parser, ~50x the cost of the same grouping over a plain
// attribute (which the server does). Projecting to the few attributes actually read shrinks
// both the wire and the parse.
//
// Returning nil is always safe -- it is the old whole-ad fetch -- so anything not analyzable
// (SELECT *, AS OF, an expression this can't parse) falls back rather than guessing at an
// attribute set. Guessing short would not fail loudly; it would quietly group by undefined.
func (e *Executor) scanAttrs(st *Statement) []string {
	if st.AsOf != "" {
		return nil // the point-in-time query has no projected variant
	}
	seen := map[string]bool{}
	var attrs []string
	add := func(name string) {
		if k := strings.ToLower(name); name != "" && name != "*" && !seen[k] {
			seen[k] = true
			attrs = append(attrs, name)
		}
	}
	analyzable := true
	addExpr := func(src string) {
		q, err := vm.Parse(src)
		if err != nil {
			analyzable = false // let the whole-ad path parse it and report the error
			return
		}
		for _, a := range q.ReadAttrs() {
			add(a)
		}
	}
	addItem := func(it SelectItem) {
		switch {
		case it.Star:
			analyzable = false // every attribute
		case it.Expr != "":
			addExpr(it.Expr)
		default:
			add(it.Col)
		}
	}
	for _, it := range st.Items {
		if it.Window != "" {
			for _, p := range it.WinPartition {
				add(p)
			}
			for _, t := range it.WinOrder {
				addItem(t.Item)
			}
			continue
		}
		if it.IsAggregate() {
			continue // its argument and filter come from aggCallOrder below
		}
		addItem(it)
	}
	// Aggregates from the items AND from HAVING, in the one canonical order.
	for _, a := range aggCallOrder(st) {
		add(a.Arg)
		if a.Filter != "" {
			addExpr(a.Filter)
		}
	}
	for _, t := range st.OrderBy {
		// A window column is ordered by its computed value; naming it here would only add an
		// attribute no ad has. QUALIFY is window columns only (validateQualify), likewise.
		if t.Item.Window == "" {
			addItem(t.Item)
		}
	}
	if !analyzable || len(attrs) == 0 {
		return nil
	}
	return attrs
}

// queryAdsForClientScan fetches the rows a client-side reduction will consume, projected to
// the attributes it reads when scanAttrs can determine them.
func (e *Executor) queryAdsForClientScan(st *Statement) ([]*classad.ClassAd, error) {
	if attrs := e.scanAttrs(st); attrs != nil {
		return e.queryAdsProjected(st.Table, st.Where, attrs, 0)
	}
	return e.queryAdsAsOf(st.Table, st.Where, 0, st.AsOf)
}

// queryAdsProjected fetches only the projection attributes of each matching row via the
// server-side projection op (QueryRawProject), which streams the old-ClassAd render of the
// projected subset. limit > 0 caps the scan server-side.
func (e *Executor) queryAdsProjected(table, where string, attrs []string, limit int) ([]*classad.ClassAd, error) {
	// Wire-form rows first: the row leaves storage as wire bytes and is rebuilt into an AST
	// here, so rendering it to old-ClassAd text in between costs a render plus a parse for
	// nothing. queryAdsWire hands back a fallback error -- an older server, a table that
	// cannot serve wire rows, or a projected row whose expressions need a sibling the
	// projection dropped -- and the text path below answers those.
	if e.wireEligible(table) {
		ads, werr := e.queryAdsWire(table, where, attrs, limit)
		if werr == nil {
			return ads, nil
		}
		if !wireFallback(werr) {
			return nil, werr
		}
	}
	// The refs-chasing projection, not the plain one: a projected attribute may hold an
	// expression over its siblings (Requirements, Rank, ...), and projecting to exactly
	// the named attributes drops those siblings, so the expression evaluates to undefined
	// here. Against a server without the opcode, fall back to the plain projection --
	// same results for literal attributes, the old undefined for expression ones.
	texts, err := e.c.QueryRawProjectRefs(context.Background(), table, constraint(where), attrs, limit)
	if errors.Is(err, dbrpc.ErrProjectRefsUnsupported) {
		texts, err = e.c.QueryRawProject(context.Background(), table, constraint(where), attrs, limit)
	}
	if err != nil {
		return nil, err
	}
	ads := make([]*classad.ClassAd, 0, len(texts))
	for _, t := range texts {
		// The projection op streams the old-ClassAd text form (unlike QueryTable's bracketed
		// new-ClassAd form), so parse with ParseOld.
		ad, perr := classad.ParseOld(t)
		if perr != nil {
			return nil, fmt.Errorf("parsing a projected ad: %w", perr)
		}
		ads = append(ads, ad)
	}
	return ads, nil
}

func (e *Executor) execSelect(st *Statement) (*Result, error) {
	// GROUP BY / aggregates -- and DISTINCT over explicit columns, which is just
	// GROUP BY those columns -- are computed server-side (hash-map aggregation):
	// only the grouped result crosses the wire, not every matched ad.
	groupBy := effectiveGroupBy(st)
	if len(groupBy) > 0 || hasAggregate(st) {
		// time_bucket grouping: push the bucketing to the server for a current-time
		// read (only the grouped rows cross the wire). Fall back to client-side
		// bucketing for an AS OF read (the server aggregate has no time-travel
		// variant) or against a server too old to implement the bucketed opcode.
		if hasBucket(st) || groupByHasBucket(st) || groupByHasExpr(st) {
			// Both table kinds push the bucketing down; only an AS OF read has no server-side
			// aggregate variant and must bucket client-side. On an archive this is what makes
			// "jobs per group per day" answerable from the per-segment indexes instead of by
			// decompressing every record.
			//
			// A computed group key is the exception whichever table it is: the server groups
			// by raw attribute values, so an expression key cannot be pushed down at all.
			if st.AsOf == "" && !groupByHasExpr(st) {
				res, err := e.execAggregateBucketServer(st)
				if err == nil {
					return res, nil
				}
				if !bucketPushdownUnsupported(err) {
					return nil, err
				}
			}
			return e.execAggregateBucket(st)
		}
		// Archives use their own server-side aggregate op (with a client-side fallback for an
		// older server), so COUNT/GROUP BY over history streams only the grouped result instead
		// of every matching row to the client.
		if e.isArchive(st.Table) {
			return e.execArchiveAggregate(st, groupBy)
		}
		return e.execAggregate(st, groupBy)
	}

	if hasWindow(st) {
		return e.execSelectWindow(st)
	}

	// Push LIMIT to the server only when the final row set is a prefix of the scan
	// order -- i.e. no client-side reordering (ORDER BY) or row-reduction
	// (DISTINCT) happens after the fetch. Otherwise fetch all and cap last.
	pushLimit := 0
	if st.Limit > 0 && len(st.OrderBy) == 0 && !st.Distinct {
		pushLimit = st.Limit
	}
	// Push the column projection to the server for a plain-column SELECT so only the needed
	// attributes cross the wire (and are decoded), instead of every attribute of a wide job
	// ad. nil ⇒ fetch whole ads (SELECT *, expression columns, archives, AS OF -- see
	// projectionAttrs).
	var ads []*classad.ClassAd
	var err error
	if proj := e.projectionAttrs(st); proj != nil {
		ads, err = e.queryAdsProjected(st.Table, st.Where, proj, pushLimit)
	} else {
		ads, err = e.queryAdsAsOf(st.Table, st.Where, pushLimit, st.AsOf)
	}
	if err != nil {
		return nil, err
	}
	if st.Distinct { // DISTINCT * : de-duplicate whole ads
		ads = dedupeAds(ads)
	}
	if len(st.OrderBy) > 0 {
		if err := sortAds(ads, st.OrderBy); err != nil {
			return nil, err
		}
	}

	limited := applyLimit(ads, st.Limit)
	res := &Result{IsSelect: true, Ads: limited}

	// SELECT * : every attribute, each rendered by name.
	if len(st.Items) == 1 && st.Items[0].Star {
		res.Columns = e.starColumns(limited)
		res.Star = true
		for _, ad := range limited {
			row := make([]string, len(res.Columns))
			for j, col := range res.Columns {
				row[j] = valueDisplay(ad.EvaluateAttr(col))
			}
			res.Rows = append(res.Rows, row)
		}
		return res, nil
	}

	// Explicit items. Precompile any expression columns once (e.g.
	// "CurrentTime - EnteredHistoryTime"); a plain column is rendered by attribute name.
	compiled := make([]*classad.Expr, len(st.Items))
	for j, it := range st.Items {
		res.Columns = append(res.Columns, it.header())
		if it.Expr != "" {
			ex, perr := classad.ParseExpr(it.Expr)
			if perr != nil {
				return nil, fmt.Errorf("SELECT expression %q: %w", it.Expr, perr)
			}
			compiled[j] = ex
		}
	}
	for _, ad := range limited {
		row := make([]string, len(st.Items))
		for j, it := range st.Items {
			if compiled[j] != nil {
				row[j] = valueDisplay(compiled[j].Eval(ad))
			} else {
				row[j] = valueDisplay(ad.EvaluateAttr(it.Col))
			}
		}
		res.Rows = append(res.Rows, row)
	}
	return res, nil
}

// hasAggregate reports whether any selected item is computed per group -- a bare aggregate
// or an expression with aggregates lifted out of it -- so the statement takes an aggregate
// execution path rather than a row scan.
func hasAggregate(st *Statement) bool {
	// A HAVING makes the statement group-level even with no aggregate in the projection
	// and no GROUP BY: it filters the single implicit group (`SELECT 1 FROM t HAVING
	// COUNT(*) > 3`).
	if st.Having != "" {
		return true
	}
	for _, it := range st.Items {
		if it.GroupLevel() {
			return true
		}
	}
	return false
}

// effectiveGroupBy is st.GroupBy, or -- for a DISTINCT over explicit columns --
// the projected column names (DISTINCT a, b == GROUP BY a, b).
func effectiveGroupBy(st *Statement) []string {
	if len(st.GroupBy) > 0 {
		return st.GroupBy
	}
	if st.Distinct && !hasAggregate(st) && !(len(st.Items) == 1 && st.Items[0].Star) {
		cols := make([]string, 0, len(st.Items))
		for _, it := range st.Items {
			cols = append(cols, it.Col)
		}
		return cols
	}
	return nil
}

// execAggregate runs a GROUP BY / aggregate query on the server and assembles the
// tabular result in the SELECT's column order, then applies ORDER BY and LIMIT.
func (e *Executor) execAggregate(st *Statement, groupBy []string) (*Result, error) {
	// Point-in-time aggregates are computed client-side over the AS OF rows (the
	// server aggregate pushdown is current-time only). Fetch the historical rows and
	// group/reduce locally.
	// Likewise inside a transaction: the server aggregate reduces over the COMMITTED
	// store, so a COUNT(*) after an INSERT in the same transaction would not count it.
	// Fetch through the transaction and reduce locally instead -- the same fetch-and-
	// reduce path AS OF uses.
	if st.AsOf != "" || e.txReads(st.Table) {
		return e.execAggregateAsOf(st, groupBy)
	}
	aggs, groupIdx := aggSpecs(st, groupBy)
	rows, err := e.c.AggregateTable(context.Background(), st.Table, constraint(st.Where), groupBy, aggs)
	if err != nil {
		return nil, err
	}
	return formatAggResult(st, groupIdx, rows)
}

// execArchiveAggregate computes a GROUP BY / aggregate SELECT over an append-only history
// table using the server-side archive aggregate (only the grouped result crosses the wire,
// instead of every matched row). A server too old to know the op reports
// ErrArchiveAggregateUnsupported, so we fall back to client-side aggregation over the rows.
func (e *Executor) execArchiveAggregate(st *Statement, groupBy []string) (*Result, error) {
	aggs, groupIdx := aggSpecs(st, groupBy)
	rows, err := e.c.ArchiveAggregate(context.Background(), st.Table, constraint(st.Where), groupBy, aggs)
	if errors.Is(err, dbrpc.ErrArchiveAggregateUnsupported) {
		return e.execAggregateAsOf(st, groupBy) // client-side fetch-and-reduce fallback
	}
	if err != nil {
		return nil, err
	}
	return formatAggResult(st, groupIdx, rows)
}

// archiveRowCount returns the number of rows in an append-only history table via the
// server-side count aggregate (falling back to a client-side count against an older server).
func (e *Executor) archiveRowCount(table string) (int, error) {
	rows, err := e.c.ArchiveAggregate(context.Background(), table, "true", nil,
		[]dbrpc.AggSpec{{Func: dbrpc.AggCount, Arg: "*"}})
	if errors.Is(err, dbrpc.ErrArchiveAggregateUnsupported) {
		ads, aerr := e.queryAds(table, "", 0) // client-side fallback (queryAds routes archives)
		if aerr != nil {
			return 0, aerr
		}
		return len(ads), nil
	}
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 || len(rows[0].Values) == 0 {
		return 0, nil
	}
	return strconv.Atoi(rows[0].Values[0])
}

// aggSpecs builds the aggregate specs (flattened in item order, so an expression over
// several aggregates contributes each of them) and the group-column index map for a
// GROUP BY / aggregate SELECT.
func aggSpecs(st *Statement, groupBy []string) ([]dbrpc.AggSpec, map[string]int) {
	groupIdx := map[string]int{}
	for i, g := range groupBy {
		groupIdx[strings.ToLower(g)] = i
	}
	return aggSpecsFor(st), groupIdx
}

// namedGroupProjector wires the SELECT items to a group tuple addressed BY NAME (the server
// aggregate returns the GROUP BY columns in their declared order).
func namedGroupProjector(st *Statement, groupBy []string, groupIdx map[string]int) (*aggProjector, error) {
	groupAt := make([]int, len(st.Items))
	for i, it := range st.Items {
		groupAt[i] = -1
		if !it.GroupLevel() {
			if idx, ok := groupIdx[strings.ToLower(it.Col)]; ok {
				groupAt[i] = idx
			}
		}
	}
	return newAggProjector(st, groupAt, groupBy)
}

// positionalGroupProjector wires the SELECT items to a group tuple addressed BY POSITION:
// the bucketed aggregate groups by the projected non-aggregate items in order, so the i'th
// such item is the i'th group column.
func positionalGroupProjector(st *Statement) (*aggProjector, error) {
	groupAt := make([]int, len(st.Items))
	var groupCol []string
	for i, it := range st.Items {
		groupAt[i] = -1
		if it.GroupLevel() {
			continue
		}
		groupAt[i] = len(groupCol)
		groupCol = append(groupCol, it.Col)
	}
	return newAggProjector(st, groupAt, groupCol)
}

// formatAggResult assembles server-side aggregate rows into the SELECT's column order, then
// applies ORDER BY and LIMIT.
func formatAggResult(st *Statement, groupIdx map[string]int, rows []dbrpc.AggRow) (*Result, error) {
	proj, err := namedGroupProjector(st, st.GroupBy, groupIdx)
	if err != nil {
		return nil, err
	}
	res := &Result{IsSelect: true, Columns: proj.columns()}
	for _, gr := range rows {
		if !proj.keep(gr.Group, gr.Values) {
			continue
		}
		res.Rows = append(res.Rows, proj.row(gr.Group, gr.Values))
	}
	if len(st.OrderBy) > 0 {
		if err := sortRows(res, st.OrderBy); err != nil {
			return nil, err
		}
	}
	if st.Limit > 0 && len(res.Rows) > st.Limit {
		res.Rows = res.Rows[:st.Limit]
	}
	return res, nil
}

// execAggregateAsOf computes a GROUP BY / aggregate SELECT over a point-in-time
// snapshot by fetching the AS OF rows and grouping/reducing them client-side (the
// server aggregate pushdown has no time-travel variant in v1).
func (e *Executor) execAggregateAsOf(st *Statement, groupBy []string) (*Result, error) {
	ads, err := e.queryAdsAsOf(st.Table, st.Where, 0, st.AsOf)
	if err != nil {
		return nil, err
	}
	groupIdx := map[string]int{}
	for i, g := range groupBy {
		groupIdx[strings.ToLower(g)] = i
	}
	// Bucket ads by their group-column tuple, preserving first-seen order.
	type bucket struct {
		group []string
		ads   []*classad.ClassAd
	}
	var order []string
	buckets := map[string]*bucket{}
	for _, ad := range ads {
		g := make([]string, len(groupBy))
		for i, col := range groupBy {
			g[i] = valueDisplay(ad.EvaluateAttr(col))
		}
		key := strings.Join(g, "\x00")
		b := buckets[key]
		if b == nil {
			b = &bucket{group: g}
			buckets[key] = b
			order = append(order, key)
		}
		b.ads = append(b.ads, ad)
	}
	// With no GROUP BY and no rows, aggregates over the empty set still yield one row
	// (e.g. COUNT(*) = 0).
	if len(groupBy) == 0 && len(order) == 0 {
		order = []string{""}
		buckets[""] = &bucket{}
	}

	proj, err := namedGroupProjector(st, groupBy, groupIdx)
	if err != nil {
		return nil, err
	}
	res := &Result{IsSelect: true, Columns: proj.columns()}
	for _, key := range order {
		b := buckets[key]
		vals := reduceAggs(st, b.ads)
		if !proj.keep(b.group, vals) {
			continue
		}
		res.Rows = append(res.Rows, proj.row(b.group, vals))
	}
	if len(st.OrderBy) > 0 {
		if err := sortRows(res, st.OrderBy); err != nil {
			return nil, err
		}
	}
	if st.Limit > 0 && len(res.Rows) > st.Limit {
		res.Rows = res.Rows[:st.Limit]
	}
	return res, nil
}

// hasBucket reports whether any selected item is a time_bucket grouping expression.
func hasBucket(st *Statement) bool {
	for _, it := range st.Items {
		if it.Bucket {
			return true
		}
	}
	return false
}

// execAggregateBucketServer runs a time_bucket GROUP BY on the server, pushing the
// bucketing down (only the grouped rows cross the wire) via the dbrpc bucketed
// aggregate. It returns an error wrapping dbrpc.ErrBucketedUnsupported when the
// server is too old, so the caller falls back to client-side bucketing. Grouping is
// driven by the projected non-aggregate items (as in execAggregateBucket), so the
// returned group tuple lines up positionally with the output columns.
// bucketPushdownUnsupported reports whether err means the server cannot do the bucketed
// aggregate itself -- an opcode it does not implement -- so the caller should reduce
// client-side instead of failing the query. Mutable tables and archives signal this with
// their own sentinels, and a filtered aggregate has a third: it must never be silently
// retried without its filters.
func bucketPushdownUnsupported(err error) bool {
	return errors.Is(err, dbrpc.ErrBucketedUnsupported) ||
		errors.Is(err, dbrpc.ErrArchiveAggregateUnsupported) ||
		errors.Is(err, dbrpc.ErrFilteredAggregateUnsupported)
}

func (e *Executor) execAggregateBucketServer(st *Statement) (*Result, error) {
	proj, err := positionalGroupProjector(st)
	if err != nil {
		return nil, err
	}
	var groups []dbrpc.GroupCol
	aggs := proj.specs()
	for _, it := range st.Items {
		if it.GroupLevel() {
			continue
		}
		// A plain group column has BucketWidth 0; a time_bucket item carries its width.
		groups = append(groups, dbrpc.GroupCol{Attr: it.Col, BucketWidth: it.BucketWidth})
	}
	// Archives and mutable tables have separate aggregate opcodes; both carry group bucket
	// widths, so the only difference here is which one to call.
	var rows []dbrpc.AggRow
	if e.isArchive(st.Table) {
		rows, err = e.c.ArchiveAggregateBucketed(context.Background(), st.Table, constraint(st.Where), groups, aggs)
	} else {
		rows, err = e.c.AggregateBucketedTable(context.Background(), st.Table, constraint(st.Where), groups, aggs)
	}
	if err != nil {
		return nil, err
	}
	res := &Result{IsSelect: true, Columns: proj.columns()}
	for _, gr := range rows {
		if !proj.keep(gr.Group, gr.Values) {
			continue
		}
		res.Rows = append(res.Rows, proj.row(gr.Group, gr.Values))
	}
	if len(st.OrderBy) > 0 {
		if err := sortRows(res, st.OrderBy); err != nil {
			return nil, err
		}
	}
	if st.Limit > 0 && len(res.Rows) > st.Limit {
		res.Rows = res.Rows[:st.Limit]
	}
	return res, nil
}

// execAggregateBucket computes a GROUP BY that includes a time_bucket(...) column.
// It fetches the matching rows (honoring an AS OF instant) and groups/reduces them
// client-side, flooring the bucket attribute -- the server aggregate can only group
// by raw attribute values, so a computed bucket key can't be pushed down (Phase 0).
// Grouping is driven by the projected non-aggregate items (validateSelect ensures
// those are exactly the GROUP BY terms when bucketing), so the group tuple lines up
// with the output columns positionally.
func (e *Executor) execAggregateBucket(st *Statement) (*Result, error) {
	ads, err := e.queryAdsForClientScan(st)
	if err != nil {
		return nil, err
	}
	// Compile each computed group key ONCE. Parsing it per ad made a grouped CASE query
	// spend most of its time in the ClassAd parser -- 50x the cost of grouping by a plain
	// attribute over the same rows.
	groupExpr := make([]*classad.Expr, len(st.Items))
	for j, it := range st.Items {
		if it.GroupLevel() || it.Bucket || it.Expr == "" {
			continue
		}
		ex, perr := classad.ParseExpr(it.Expr)
		if perr != nil {
			return nil, fmt.Errorf("GROUP BY expression %q: %w", it.Col, perr)
		}
		groupExpr[j] = ex
	}
	type group struct {
		vals []string
		ads  []*classad.ClassAd
	}
	var order []string
	groups := map[string]*group{}
	for _, ad := range ads {
		vals := make([]string, 0, len(st.Items))
		drop := false
		for j, it := range st.Items {
			if it.IsAggregate() {
				continue
			}
			if it.Bucket {
				sec, ok := ad.EvaluateAttrNumber(it.Col)
				if !ok {
					drop = true // undefined bucket timestamp: row falls out of the series
					break
				}
				vals = append(vals, bucketFloor(sec, it.BucketWidth))
				continue
			}
			if groupExpr[j] != nil {
				vals = append(vals, valueDisplay(groupExpr[j].Eval(ad)))
				continue
			}
			vals = append(vals, valueDisplay(ad.EvaluateAttr(it.Col)))
		}
		if drop {
			continue
		}
		key := strings.Join(vals, "\x00")
		g := groups[key]
		if g == nil {
			g = &group{vals: vals}
			groups[key] = g
			order = append(order, key)
		}
		g.ads = append(g.ads, ad)
	}

	proj, err := positionalGroupProjector(st)
	if err != nil {
		return nil, err
	}
	res := &Result{IsSelect: true, Columns: proj.columns()}
	for _, key := range order {
		g := groups[key]
		vals := reduceAggs(st, g.ads)
		if !proj.keep(g.vals, vals) {
			continue
		}
		res.Rows = append(res.Rows, proj.row(g.vals, vals))
	}
	if len(st.OrderBy) > 0 {
		if err := sortRows(res, st.OrderBy); err != nil {
			return nil, err
		}
	}
	if st.Limit > 0 && len(res.Rows) > st.Limit {
		res.Rows = res.Rows[:st.Limit]
	}
	return res, nil
}

// bucketFloor floors unix-epoch seconds to a width-aligned bucket (aligned to the
// epoch, so bucket boundaries are stable across queries) and returns it as a decimal
// seconds string -- the same shape the frame layer reads as a time field.
func bucketFloor(sec float64, width int64) string {
	if width <= 0 {
		return ""
	}
	b := int64(math.Floor(sec/float64(width))) * width
	return strconv.FormatInt(b, 10)
}

// aggregateAds reduces one aggregate call over a group's ads (client-side, for AS OF and
// time_bucket). It takes the call rather than the SELECT item so a bare aggregate and one
// lifted out of an expression reduce through the same code.
func aggregateAds(it AggCall, ads []*classad.ClassAd) string {
	// A conditional aggregate sees only its own rows. The client-side paths reduce over ads
	// rather than projected values, so the filter is applied here -- if it were skipped the
	// AS OF and time_bucket paths would quietly answer the unfiltered question.
	if it.Filter != "" {
		ads = filterAds(it.Filter, ads)
	}
	switch strings.ToUpper(it.Func) {
	case "COUNT":
		if it.Arg == "*" || it.Arg == "" {
			return strconv.Itoa(len(ads))
		}
		n := 0
		for _, ad := range ads {
			if v := ad.EvaluateAttr(it.Arg); !v.IsUndefined() && !v.IsError() {
				n++
			}
		}
		return strconv.Itoa(n)
	case aggCountDistinct:
		// Keyed on the rendered value, as the server's reducer and the group tuple both are,
		// so all three agree on what "the same value" is. Undefined is not a value.
		seen := map[string]struct{}{}
		for _, ad := range ads {
			if v := ad.EvaluateAttr(it.Arg); !v.IsUndefined() && !v.IsError() {
				seen[valueDisplay(v)] = struct{}{}
			}
		}
		return strconv.Itoa(len(seen))
	case "SUM", "AVG", "MIN", "MAX":
		var sum, min, max float64
		n := 0
		for _, ad := range ads {
			f, ok := numValue(ad.EvaluateAttr(it.Arg))
			if !ok {
				continue
			}
			if n == 0 || f < min {
				min = f
			}
			if n == 0 || f > max {
				max = f
			}
			sum += f
			n++
		}
		if n == 0 {
			return "" // no numeric values
		}
		switch strings.ToUpper(it.Func) {
		case "SUM":
			return trimFloat(sum)
		case "AVG":
			return trimFloat(sum / float64(n))
		case "MIN":
			return trimFloat(min)
		default:
			return trimFloat(max)
		}
	}
	return ""
}

// numValue extracts a float from an integer/real ClassAd value.
func numValue(v classad.Value) (float64, bool) {
	if v.IsInteger() {
		i, _ := v.IntValue()
		return float64(i), true
	}
	if v.IsReal() {
		r, _ := v.RealValue()
		return r, true
	}
	return 0, false
}

// parseAsOf parses a point-in-time instant for FOR SYSTEM_TIME AS OF: RFC3339, a
// "2006-01-02 15:04:05" datetime (interpreted in local time), a bare "2006-01-02"
// date, or a relative look-back like "-1h" / "-30m" (subtracted from now).
func parseAsOf(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty AS OF timestamp")
	}
	if s[0] == '-' || s[0] == '+' {
		d, err := time.ParseDuration(s)
		if err != nil {
			return time.Time{}, fmt.Errorf("AS OF %q: bad relative duration: %w", s, err)
		}
		return time.Now().Add(d), nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("AS OF %q: not a recognized timestamp (use RFC3339, \"2006-01-02 15:04:05\", or a relative \"-1h\")", s)
}

// aggFunc maps a SQL aggregate name to the dbrpc function code.
func aggFunc(name string) dbrpc.AggFunc {
	switch name {
	case "SUM":
		return dbrpc.AggSum
	case "AVG":
		return dbrpc.AggAvg
	case "MIN":
		return dbrpc.AggMin
	case "MAX":
		return dbrpc.AggMax
	case aggCountDistinct:
		return dbrpc.AggCountDistinct
	default:
		return dbrpc.AggCount
	}
}

// applyLimit returns the first limit ads (0 = all).
func applyLimit(ads []*classad.ClassAd, limit int) []*classad.ClassAd {
	if limit > 0 && len(ads) > limit {
		return ads[:limit]
	}
	return ads
}

// dedupeAds returns ads with duplicate whole-ad values removed (SELECT DISTINCT *),
// preserving first-seen order.
func dedupeAds(ads []*classad.ClassAd) []*classad.ClassAd {
	seen := make(map[string]struct{}, len(ads))
	out := ads[:0:0]
	for _, ad := range ads {
		k := ad.StringWithPrivate()
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, ad)
	}
	return out
}

// sortAds sorts ads by the ORDER BY terms (which must be plain columns for a
// non-aggregate query). Undefined/error values sort after concrete values.
func sortAds(ads []*classad.ClassAd, terms []OrderTerm) error {
	for _, t := range terms {
		if t.Item.IsAggregate() {
			return fmt.Errorf("cannot ORDER BY the aggregate %s in a non-aggregate query", t.Item.header())
		}
	}
	sort.SliceStable(ads, func(i, j int) bool {
		for _, t := range terms {
			c := compareValues(ads[i].EvaluateAttr(t.Item.Col), ads[j].EvaluateAttr(t.Item.Col))
			if c != 0 {
				if t.Desc {
					return c > 0
				}
				return c < 0
			}
		}
		return false
	})
	return nil
}

// sortRows sorts an aggregate result's rows by the ORDER BY terms, each of which
// must reference an output column (a group column or an aggregate).
func sortRows(res *Result, terms []OrderTerm) error {
	idxs := make([]int, len(terms))
	for k, t := range terms {
		idx := columnIndex(res.Columns, t.Item.header())
		if idx < 0 {
			return fmt.Errorf("ORDER BY %s is not a selected column", t.Item.header())
		}
		idxs[k] = idx
	}
	sort.SliceStable(res.Rows, func(i, j int) bool {
		for k, t := range terms {
			c := compareCells(res.Rows[i][idxs[k]], res.Rows[j][idxs[k]])
			if c != 0 {
				if t.Desc {
					return c > 0
				}
				return c < 0
			}
		}
		return false
	})
	return nil
}

func columnIndex(cols []string, name string) int {
	for i, c := range cols {
		if strings.EqualFold(c, name) {
			return i
		}
	}
	return -1
}

// compareValues orders two ClassAd values: numbers before strings before other
// (undefined/error/bool), then by natural order within a kind.
func compareValues(a, b classad.Value) int {
	ra, rb := valueRank(a), valueRank(b)
	if ra != rb {
		return sign(ra - rb)
	}
	switch ra {
	case rankNumber:
		fa, _ := numOf(a)
		fb, _ := numOf(b)
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		default:
			return 0
		}
	case rankString:
		sa, _ := a.StringValue()
		sb, _ := b.StringValue()
		return strings.Compare(sa, sb)
	default:
		return 0
	}
}

const (
	rankNumber = 0
	rankString = 1
	rankOther  = 2
)

func valueRank(v classad.Value) int {
	switch {
	case v.IsNumber():
		return rankNumber
	case v.IsString():
		return rankString
	default:
		return rankOther
	}
}

func numOf(v classad.Value) (float64, bool) {
	if v.IsInteger() {
		i, _ := v.IntValue()
		return float64(i), true
	}
	if v.IsReal() {
		r, _ := v.RealValue()
		return r, true
	}
	return 0, false
}

// compareCells orders two rendered cells: numerically when both parse as numbers,
// else lexically.
func compareCells(a, b string) int {
	fa, ea := strconv.ParseFloat(a, 64)
	fb, eb := strconv.ParseFloat(b, 64)
	if ea == nil && eb == nil {
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(a, b)
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

// starColumns computes the column set for SELECT *: the key attribute first,
// then the sorted union of every other attribute across the result set.
func (e *Executor) starColumns(ads []*classad.ClassAd) []string {
	seen := map[string]bool{}
	var others []string
	for _, ad := range ads {
		for _, name := range ad.GetAttributes() {
			if strings.EqualFold(name, e.keyAttr) || seen[name] {
				continue
			}
			seen[name] = true
			others = append(others, name)
		}
	}
	sort.Strings(others)
	return append([]string{e.keyAttr}, others...)
}

func (e *Executor) execInsert(st *Statement) (*Result, error) {
	// Build the ad text and resolve the key.
	var sb strings.Builder
	key := ""
	haveKeyAttr := false
	for i, col := range st.Columns {
		val := st.Values[i]
		if strings.EqualFold(col, e.keyAttr) {
			key = keyFromLiteral(val)
			haveKeyAttr = true
		}
		fmt.Fprintf(&sb, "%s = %s\n", col, val)
	}
	if !haveKeyAttr {
		key = e.genKey()
		lit, ok := quoteClassAdOld(key)
		if !ok {
			return nil, fmt.Errorf("INSERT: generated key %q cannot be written in old-ClassAd format", key)
		}
		fmt.Fprintf(&sb, "%s = %s\n", e.keyAttr, lit)
	}
	if key == "" {
		return nil, fmt.Errorf("INSERT: empty primary key")
	}

	if err := e.commit(st.Table, []WriteOp{{Kind: WNewClassAd, Key: key, Value: sb.String()}}); err != nil {
		return nil, err
	}
	return &Result{Affected: 1, Note: "INSERT 1 (key " + key + ")"}, nil
}

func (e *Executor) execUpdate(st *Statement) (*Result, error) {
	for _, a := range st.Assignments {
		if strings.EqualFold(a.Col, e.keyAttr) {
			return nil, fmt.Errorf("cannot UPDATE the key attribute %q", e.keyAttr)
		}
	}
	keys, err := e.matchedKeys(st.Table, st.Where)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return &Result{Affected: 0, Note: "UPDATE 0"}, nil
	}

	var ops []WriteOp
	for _, key := range keys {
		for _, a := range st.Assignments {
			ops = append(ops, WriteOp{Kind: WSetAttribute, Key: key, Name: a.Col, Value: a.Expr})
		}
	}
	if err := e.commit(st.Table, ops); err != nil {
		return nil, fmt.Errorf("updating: %w", err)
	}
	return &Result{Affected: len(keys), Note: fmt.Sprintf("UPDATE %d", len(keys))}, nil
}

func (e *Executor) execDelete(st *Statement) (*Result, error) {
	// Prefer the server-side bulk delete: the store addresses each matching row by its real
	// storage key, so DELETE works regardless of whether the row carries a "Key" attribute.
	// The client-side path (matchedKeys) reads a "Key" attribute off each matched ad, which
	// rows written before key-stamping -- and Owner/User records -- do not have, so
	// `DELETE FROM jobs WHERE MyType =?= "Owner"` failed to address them. It also avoids
	// round-tripping every matched ad just to delete it.
	//
	// Not inside a transaction, though: the bulk op commits on its own (it carries no
	// transaction id), so taking it there would apply the delete immediately and let a
	// later ROLLBACK silently fail to undo it. Staging keys is slower but is the only
	// path the transaction can discard.
	if e.applyBatch == nil && !e.txActive {
		n, err := e.c.DeleteWhereTable(context.Background(), st.Table, constraint(st.Where))
		if err != nil {
			return nil, fmt.Errorf("deleting: %w", err)
		}
		return &Result{Affected: n, Note: fmt.Sprintf("DELETE %d", n)}, nil
	}
	// Batch/embedded mode, or an open transaction: resolve keys and stage a destroy per row.
	keys, err := e.matchedKeys(st.Table, st.Where)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return &Result{Affected: 0, Note: "DELETE 0"}, nil
	}
	ops := make([]WriteOp, 0, len(keys))
	for _, key := range keys {
		ops = append(ops, WriteOp{Kind: WDestroyClassAd, Key: key})
	}
	if err := e.commit(st.Table, ops); err != nil {
		return nil, fmt.Errorf("deleting: %w", err)
	}
	return &Result{Affected: len(keys), Note: fmt.Sprintf("DELETE %d", len(keys))}, nil
}

// matchedKeys returns the real storage keys of every row matching where, resolved server-side
// (dbrpc QueryKeys) rather than read from a self-reported key attribute on each ad. This is what
// lets UPDATE/DELETE address rows that carry no "Key" attribute -- crufty Owner/User records, or
// rows written before key-stamping -- which the old attribute-recovery path could not target. It
// also avoids fetching every matching ad just to read one attribute.
func (e *Executor) matchedKeys(table, where string) ([]string, error) {
	// Inside a transaction, match through it: an UPDATE or DELETE must be able to address
	// a row the same transaction created, which the committed-state op cannot see.
	if e.txReads(table) {
		return e.tx.KeysWhere(context.Background(), constraint(where))
	}
	return e.c.QueryKeysTable(context.Background(), table, constraint(where)) // UPDATE/DELETE act on every matching row
}

// --- value helpers ---

// valueDisplay renders a Value for tabular output.
func valueDisplay(v classad.Value) string {
	switch {
	case v.IsUndefined():
		return "undefined"
	case v.IsError():
		return "error"
	case v.IsBool():
		b, _ := v.BoolValue()
		return strconv.FormatBool(b)
	case v.IsString():
		s, _ := v.StringValue()
		return s
	case v.IsInteger():
		i, _ := v.IntValue()
		return strconv.FormatInt(i, 10)
	case v.IsReal():
		r, _ := v.RealValue()
		return trimFloat(r)
	default:
		return v.String()
	}
}

// keyString renders a key Value as the db key string.
func keyString(v classad.Value) string {
	if v.IsString() {
		s, _ := v.StringValue()
		return s
	}
	return valueDisplay(v)
}

// keyFromLiteral extracts the db key from a ClassAd literal value expression (as
// produced by the parser): a quoted string yields its content, anything else its
// literal text.
func keyFromLiteral(lit string) string {
	lit = strings.TrimSpace(lit)
	if len(lit) >= 2 && lit[0] == '"' && lit[len(lit)-1] == '"' {
		// INSERT values are quoted for old-ClassAd text (quoteClassAdOld), so the key is
		// recovered with the matching inverse -- unquoteClassAd would eat backslashes the
		// value legitimately contains.
		return unquoteClassAdOld(lit)
	}
	return lit
}

func unquoteClassAd(lit string) string {
	inner := lit[1 : len(lit)-1]
	var sb strings.Builder
	for i := 0; i < len(inner); i++ {
		if inner[i] == '\\' && i+1 < len(inner) {
			i++
			switch inner[i] {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			default:
				sb.WriteByte(inner[i])
			}
			continue
		}
		sb.WriteByte(inner[i])
	}
	return sb.String()
}

// trimFloat formats a float without a trailing ".0" for whole numbers.
func trimFloat(f float64) string {
	s := strconv.FormatFloat(f, 'g', -1, 64)
	return s
}
