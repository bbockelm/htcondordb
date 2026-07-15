package repl

import (
	"fmt"
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
	tx, err := e.c.BeginTable(table)
	if err != nil {
		return err
	}
	for _, op := range ops {
		switch op.Kind {
		case WNewClassAd:
			err = tx.NewClassAd(op.Key, op.Value)
		case WSetAttribute:
			err = tx.SetAttribute(op.Key, op.Name, op.Value)
		case WDestroyClassAd:
			err = tx.DestroyClassAd(op.Key)
		}
		if err != nil {
			_ = tx.Abort()
			return err
		}
	}
	return tx.Commit()
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
	case StmtMatch:
		return e.execMatch(st)
	default:
		return nil, fmt.Errorf("unknown statement kind")
	}
}

// --- DDL ---

func (e *Executor) execCreateTable(st *Statement) (*Result, error) {
	if err := e.c.CreateTable(st.Table); err != nil {
		return nil, err
	}
	return &Result{Note: "CREATE TABLE " + st.Table}, nil
}

func (e *Executor) execDropTable(st *Statement) (*Result, error) {
	if err := e.c.DropTable(st.Table); err != nil {
		return nil, err
	}
	return &Result{Note: "DROP TABLE " + st.Table}, nil
}

func (e *Executor) execCreateIndex(st *Statement) (*Result, error) {
	action := "index.add.value"
	if st.IndexKind == "categorical" {
		action = "index.add.categorical"
	}
	msg, err := e.c.AdminTable(st.Table, action, st.Columns...)
	if err != nil {
		return nil, err
	}
	return &Result{Note: msg}, nil
}

func (e *Executor) execDropIndex(st *Statement) (*Result, error) {
	msg, err := e.c.AdminTable(st.Table, "index.drop", st.Columns...)
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
	rows, err := e.c.MatchTables(st.Table, st.MatchResource, e.keyAttr, reqWhere, st.TargetWhere, st.Limit, st.MatchUsing)
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
	return e.c.DiagnosticsTable(table)
}

// Explain reports how a table would execute a constraint query (.explain).
func (e *Executor) Explain(table, constraint string) (*db.QueryExplain, error) {
	return e.c.ExplainTable(table, constraint)
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
	return e.c.MatchExplain(st.Table, selector, st.MatchResource)
}

// Admin runs an index/hot-set management action on a table, returning the
// server's message.
func (e *Executor) Admin(table, action string, args ...string) (string, error) {
	return e.c.AdminTable(table, action, args...)
}

// Tables lists the catalog's table names.
func (e *Executor) Tables() ([]string, error) { return e.c.Tables() }

// CreateTable creates a table (used by load auto-routing).
func (e *Executor) CreateTable(name string) error { return e.c.CreateTable(name) }

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
	texts, err := e.c.QueryTable(table, constraint(where), limit)
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
		return e.execAggregate(st, groupBy)
	}

	// Push LIMIT to the server only when the final row set is a prefix of the scan
	// order -- i.e. no client-side reordering (ORDER BY) or row-reduction
	// (DISTINCT) happens after the fetch. Otherwise fetch all and cap last.
	pushLimit := 0
	if st.Limit > 0 && len(st.OrderBy) == 0 && !st.Distinct {
		pushLimit = st.Limit
	}
	ads, err := e.queryAds(st.Table, st.Where, pushLimit)
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

	rows, err := e.c.AggregateTable(st.Table, constraint(st.Where), groupBy, aggs)
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
