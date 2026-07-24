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

	// archives caches the set of append-only (history) table names, so a SELECT can be routed
	// to the archive query path -- archives are not mutable tables and the regular query op
	// does not resolve them. Loaded lazily; archivesOK gates a successful load so a transient
	// list error just retries next time.
	archives   map[string]bool
	archivesOK bool
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
	return &Executor{c: c, keyAttr: keyAttr, genKey: genKey, applyBatch: cfg.ApplyBatch}
}

// commit applies a batch of write ops to table: through ApplyBatch (consistent
// mode) if configured, else as one local dbrpc transaction on that table.
func (e *Executor) commit(table string, ops []WriteOp) error {
	if e.applyBatch != nil {
		return e.applyBatch(ops)
	}
	tx, err := e.c.BeginTable(context.Background(), table)
	if err != nil {
		return err
	}
	for _, op := range ops {
		switch op.Kind {
		case WNewClassAd:
			err = tx.NewClassAd(context.Background(), op.Key, op.Value)
		case WSetAttribute:
			err = tx.SetAttribute(context.Background(), op.Key, op.Name, op.Value)
		case WDestroyClassAd:
			err = tx.DestroyClassAd(context.Background(), op.Key)
		}
		if err != nil {
			_ = tx.Abort(context.Background())
			return err
		}
	}
	return tx.Commit(context.Background())
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
		if hasBucket(st) || groupByHasBucket(st) {
			// The server aggregate pushdown is current-time, mutable-table only. Archives (and
			// AS OF) bucket client-side.
			if st.AsOf == "" && !e.isArchive(st.Table) {
				res, err := e.execAggregateBucketServer(st)
				if err == nil {
					return res, nil
				}
				if !errors.Is(err, dbrpc.ErrBucketedUnsupported) {
					return nil, err
				}
			}
			return e.execAggregateBucket(st)
		}
		// Archives have no server-side aggregate op; compute the aggregate client-side over the
		// fetched rows (the same path AS OF uses -- queryAdsAsOf already routes archives).
		if e.isArchive(st.Table) {
			return e.execAggregateAsOf(st, groupBy)
		}
		return e.execAggregate(st, groupBy)
	}

	// Push LIMIT to the server only when the final row set is a prefix of the scan
	// order -- i.e. no client-side reordering (ORDER BY) or row-reduction
	// (DISTINCT) happens after the fetch. Otherwise fetch all and cap last.
	pushLimit := 0
	if st.Limit > 0 && len(st.OrderBy) == 0 && !st.Distinct {
		pushLimit = st.Limit
	}
	ads, err := e.queryAdsAsOf(st.Table, st.Where, pushLimit, st.AsOf)
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
	// Determine columns.
	if len(st.Items) == 1 && st.Items[0].Star {
		res.Columns = e.starColumns(limited)
		res.Star = true
	} else {
		for _, it := range st.Items {
			res.Columns = append(res.Columns, it.header())
		}
	}

	for _, ad := range limited {
		row := make([]string, len(res.Columns))
		for j, col := range res.Columns {
			row[j] = valueDisplay(ad.EvaluateAttr(col))
		}
		res.Rows = append(res.Rows, row)
	}
	return res, nil
}

// hasAggregate reports whether any selected item is an aggregate.
func hasAggregate(st *Statement) bool {
	for _, it := range st.Items {
		if it.IsAggregate() {
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
	if st.AsOf != "" {
		return e.execAggregateAsOf(st, groupBy)
	}
	// Build the aggregate specs (in item order) and the group-column index map.
	var aggs []dbrpc.AggSpec
	groupIdx := map[string]int{}
	for i, g := range groupBy {
		groupIdx[strings.ToLower(g)] = i
	}
	for _, it := range st.Items {
		if it.IsAggregate() {
			aggs = append(aggs, dbrpc.AggSpec{Func: aggFunc(it.Agg), Arg: it.Col})
		}
	}

	rows, err := e.c.AggregateTable(context.Background(), st.Table, constraint(st.Where), groupBy, aggs)
	if err != nil {
		return nil, err
	}

	res := &Result{IsSelect: true}
	for _, it := range st.Items {
		res.Columns = append(res.Columns, it.header())
	}
	for _, gr := range rows {
		row := make([]string, 0, len(st.Items))
		aggN := 0
		for _, it := range st.Items {
			if it.IsAggregate() {
				if aggN < len(gr.Values) {
					row = append(row, gr.Values[aggN])
				} else {
					row = append(row, "")
				}
				aggN++
				continue
			}
			// Plain group column: pull from the group tuple by its position.
			if idx, ok := groupIdx[strings.ToLower(it.Col)]; ok && idx < len(gr.Group) {
				row = append(row, gr.Group[idx])
			} else {
				row = append(row, "")
			}
		}
		res.Rows = append(res.Rows, row)
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

	res := &Result{IsSelect: true}
	for _, it := range st.Items {
		res.Columns = append(res.Columns, it.header())
	}
	for _, key := range order {
		b := buckets[key]
		row := make([]string, 0, len(st.Items))
		for _, it := range st.Items {
			if it.IsAggregate() {
				row = append(row, aggregateAds(it, b.ads))
			} else if idx, ok := groupIdx[strings.ToLower(it.Col)]; ok && idx < len(b.group) {
				row = append(row, b.group[idx])
			} else {
				row = append(row, "")
			}
		}
		res.Rows = append(res.Rows, row)
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
func (e *Executor) execAggregateBucketServer(st *Statement) (*Result, error) {
	var groups []dbrpc.GroupCol
	var aggs []dbrpc.AggSpec
	for _, it := range st.Items {
		if it.IsAggregate() {
			aggs = append(aggs, dbrpc.AggSpec{Func: aggFunc(it.Agg), Arg: it.Col})
			continue
		}
		// A plain group column has BucketWidth 0; a time_bucket item carries its width.
		groups = append(groups, dbrpc.GroupCol{Attr: it.Col, BucketWidth: it.BucketWidth})
	}
	rows, err := e.c.AggregateBucketedTable(context.Background(), st.Table, constraint(st.Where), groups, aggs)
	if err != nil {
		return nil, err
	}
	res := &Result{IsSelect: true}
	for _, it := range st.Items {
		res.Columns = append(res.Columns, it.header())
	}
	for _, gr := range rows {
		row := make([]string, 0, len(st.Items))
		gi, ai := 0, 0
		for _, it := range st.Items {
			if it.IsAggregate() {
				if ai < len(gr.Values) {
					row = append(row, gr.Values[ai])
				} else {
					row = append(row, "")
				}
				ai++
				continue
			}
			if gi < len(gr.Group) {
				row = append(row, gr.Group[gi])
			} else {
				row = append(row, "")
			}
			gi++
		}
		res.Rows = append(res.Rows, row)
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
	ads, err := e.queryAdsAsOf(st.Table, st.Where, 0, st.AsOf)
	if err != nil {
		return nil, err
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
		for _, it := range st.Items {
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

	res := &Result{IsSelect: true}
	for _, it := range st.Items {
		res.Columns = append(res.Columns, it.header())
	}
	for _, key := range order {
		g := groups[key]
		row := make([]string, 0, len(st.Items))
		gi := 0
		for _, it := range st.Items {
			if it.IsAggregate() {
				row = append(row, aggregateAds(it, g.ads))
				continue
			}
			row = append(row, g.vals[gi])
			gi++
		}
		res.Rows = append(res.Rows, row)
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

// aggregateAds reduces one aggregate item over a group's ads (client-side, for AS OF).
func aggregateAds(it SelectItem, ads []*classad.ClassAd) string {
	switch strings.ToUpper(it.Agg) {
	case "COUNT":
		if it.Col == "*" || it.Col == "" {
			return strconv.Itoa(len(ads))
		}
		n := 0
		for _, ad := range ads {
			if v := ad.EvaluateAttr(it.Col); !v.IsUndefined() && !v.IsError() {
				n++
			}
		}
		return strconv.Itoa(n)
	case "SUM", "AVG", "MIN", "MAX":
		var sum, min, max float64
		n := 0
		for _, ad := range ads {
			f, ok := numValue(ad.EvaluateAttr(it.Col))
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
		switch strings.ToUpper(it.Agg) {
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
		fmt.Fprintf(&sb, "%s = %s\n", e.keyAttr, quoteClassAd(key))
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

// matchedKeys returns the primary keys of every row matching where, recovered
// from the key attribute. It errors if a matched row lacks the key attribute
// (UPDATE/DELETE cannot address it).
func (e *Executor) matchedKeys(table, where string) ([]string, error) {
	ads, err := e.queryAds(table, where, 0) // UPDATE/DELETE act on every matching row
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(ads))
	for _, ad := range ads {
		v := ad.EvaluateAttr(e.keyAttr)
		if v.IsUndefined() || v.IsError() {
			return nil, fmt.Errorf("a matched row has no %q attribute; cannot address it for UPDATE/DELETE", e.keyAttr)
		}
		keys = append(keys, keyString(v))
	}
	return keys, nil
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
		return unquoteClassAd(lit)
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
