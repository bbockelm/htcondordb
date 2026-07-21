package plugin

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// queryModel is the JSON payload the QueryEditor sends per Grafana query. In
// "builder" mode the structured fields are assembled into htcondordb SQL; in
// "code" mode RawSQL is used verbatim (after macro expansion). The builder is the
// friendly default; code mode is the expert escape hatch.
type queryModel struct {
	EditorMode string `json:"editorMode"` // "builder" (default) | "code"
	RawSQL     string `json:"rawSql"`

	// Builder fields.
	Table     string      `json:"table"`
	Columns   []string    `json:"columns"` // plain projected attributes (non-aggregate)
	Metrics   []metricDef `json:"metrics"` // aggregate expressions
	GroupBy   []string    `json:"groupBy"`
	Filters   []filterDef `json:"filters"`
	TimeField string      `json:"timeField"` // attr constrained to the dashboard time range
	OrderBy   string      `json:"orderBy"`
	OrderDesc bool        `json:"orderDesc"`
	Limit     int         `json:"limit"`

	// Format hints frame shaping: "timeseries" or "table" (default).
	Format string `json:"format"`

	// Stream requests a live tail of the table's change stream (htcondordb WATCH)
	// instead of a one-shot query. Builder-only: Table (and optional Columns) select
	// what to watch. QueryData returns a live channel; the StreamHandler drives it.
	Stream bool `json:"stream"`
}

// metricDef is one aggregate in the builder, e.g. {Func:"AVG", Attr:"Cpus"} or
// {Func:"COUNT", Attr:"*"}.
type metricDef struct {
	Func string `json:"func"`
	Attr string `json:"attr"`
}

func (m metricDef) expr() string {
	fn := strings.ToUpper(strings.TrimSpace(m.Func))
	attr := strings.TrimSpace(m.Attr)
	if fn == "" {
		return attr
	}
	if fn == "COUNT" && (attr == "" || attr == "*") {
		return "COUNT(*)"
	}
	if attr == "" {
		attr = "*"
	}
	return fn + "(" + attr + ")"
}

// filterDef is one WHERE clause term. Op is a ClassAd comparison (==, !=, >, >=,
// <, <=, =~, !~); Value is auto-quoted when it is not numeric/boolean and not
// already quoted, so string comparisons form valid ClassAd expressions.
type filterDef struct {
	Attr  string `json:"attr"`
	Op    string `json:"op"`
	Value string `json:"value"`
}

func (f filterDef) expr() string {
	attr := strings.TrimSpace(f.Attr)
	op := strings.TrimSpace(f.Op)
	if attr == "" || op == "" {
		return ""
	}
	return attr + " " + op + " " + quoteValue(f.Value)
}

// quoteValue leaves numbers, booleans, and ClassAd keywords bare, passes through
// values the user already quoted, and double-quotes everything else so a string
// comparison is a valid ClassAd expression.
func quoteValue(v string) string {
	t := strings.TrimSpace(v)
	if t == "" {
		return `""`
	}
	if len(t) >= 2 && ((t[0] == '"' && t[len(t)-1] == '"') || (t[0] == '\'' && t[len(t)-1] == '\'')) {
		return t
	}
	if _, err := strconv.ParseFloat(t, 64); err == nil {
		return t
	}
	switch strings.ToLower(t) {
	case "true", "false", "undefined", "error":
		return t
	}
	return `"` + strings.ReplaceAll(t, `"`, `\"`) + `"`
}

// timeRange is the dashboard's selected window, in unix seconds (HTCondor stores
// timestamps such as QDate/EnteredCurrentStatus as unix epoch integers).
type timeRange struct {
	fromUnix int64
	toUnix   int64
}

func newTimeRange(from, to time.Time) timeRange {
	return timeRange{fromUnix: from.Unix(), toUnix: to.Unix()}
}

// toSQL renders the query to an htcondordb SQL statement for the given time range.
// interval is Grafana's suggested step (from the panel width / time range); it sets
// the bucket width for a time-series builder query.
func (q *queryModel) toSQL(tr timeRange, interval time.Duration) (string, error) {
	if strings.EqualFold(q.EditorMode, "code") {
		sql := strings.TrimSpace(applyMacros(q.RawSQL, tr))
		if sql == "" {
			return "", fmt.Errorf("empty SQL query")
		}
		return sql, nil
	}

	table := strings.TrimSpace(q.Table)
	if table == "" {
		return "", fmt.Errorf("no table selected")
	}

	// Time-series builder: when the format is Time series and a time field is set,
	// floor it into interval-wide buckets so the result is a series (one row per
	// bucket). The bucket is the leading projected + grouped column, aliased `time`.
	tsField := ""
	if strings.EqualFold(q.Format, "timeseries") {
		tsField = strings.TrimSpace(q.TimeField)
	}
	bucketExpr := ""
	if tsField != "" {
		bucketExpr = fmt.Sprintf("time_bucket(%s, '%ds')", tsField, bucketWidthSeconds(interval))
	}

	sel := make([]string, 0, len(q.GroupBy)+len(q.Metrics)+len(q.Columns)+1)
	if bucketExpr != "" {
		sel = append(sel, bucketExpr+" AS time") // time axis leads the projection
	}
	sel = append(sel, q.GroupBy...) // then group keys
	for _, m := range q.Metrics {
		if e := m.expr(); e != "" {
			sel = append(sel, e)
		}
	}
	sel = append(sel, q.Columns...)
	if len(sel) == 0 {
		sel = []string{"*"}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s FROM %s", strings.Join(sel, ", "), table)

	conds := make([]string, 0, len(q.Filters)+1)
	for _, f := range q.Filters {
		if e := f.expr(); e != "" {
			conds = append(conds, e)
		}
	}
	if tf := strings.TrimSpace(q.TimeField); tf != "" {
		conds = append(conds, timeFilterExpr(tf, tr))
	}
	if len(conds) > 0 {
		fmt.Fprintf(&b, " WHERE %s", strings.Join(conds, " && "))
	}

	groupCols := q.GroupBy
	if bucketExpr != "" {
		groupCols = append([]string{bucketExpr}, q.GroupBy...) // group by the bucket too
	}
	if len(groupCols) > 0 {
		fmt.Fprintf(&b, " GROUP BY %s", strings.Join(groupCols, ", "))
	}

	if ob := strings.TrimSpace(q.OrderBy); ob != "" {
		b.WriteString(" ORDER BY " + ob)
		if q.OrderDesc {
			b.WriteString(" DESC")
		}
	} else if bucketExpr != "" {
		b.WriteString(" ORDER BY time") // chronological series by default
	}
	if q.Limit > 0 {
		fmt.Fprintf(&b, " LIMIT %d", q.Limit)
	}
	return b.String(), nil
}

// bucketWidthSeconds converts Grafana's suggested interval into a whole-second
// time_bucket width, rounding to the nearest second (minimum 1s) and defaulting to
// 60s when no interval was supplied.
func bucketWidthSeconds(interval time.Duration) int64 {
	if interval <= 0 {
		return 60
	}
	s := int64((interval + time.Second/2) / time.Second)
	if s < 1 {
		s = 1
	}
	return s
}

// timeFilterExpr constrains a unix-epoch attribute to [from, to] as a ClassAd
// expression.
func timeFilterExpr(field string, tr timeRange) string {
	return fmt.Sprintf("(%s >= %d && %s <= %d)", field, tr.fromUnix, field, tr.toUnix)
}

var timeFilterMacro = regexp.MustCompile(`\$__timeFilter\(\s*([^)]+?)\s*\)`)

// applyMacros expands the Grafana time macros this datasource supports in raw SQL:
//
//	$__timeFilter(col)  -> (col >= <from> && col <= <to>)
//	$__timeFrom()       -> <from unix seconds>
//	$__timeTo()         -> <to unix seconds>
//	$__unixEpochFrom()  -> <from unix seconds>   (alias)
//	$__unixEpochTo()    -> <to unix seconds>     (alias)
func applyMacros(sql string, tr timeRange) string {
	sql = timeFilterMacro.ReplaceAllStringFunc(sql, func(m string) string {
		col := strings.TrimSpace(timeFilterMacro.FindStringSubmatch(m)[1])
		return timeFilterExpr(col, tr)
	})
	from := strconv.FormatInt(tr.fromUnix, 10)
	to := strconv.FormatInt(tr.toUnix, 10)
	replacer := strings.NewReplacer(
		"$__timeFrom()", from,
		"$__unixEpochFrom()", from,
		"$__timeTo()", to,
		"$__unixEpochTo()", to,
	)
	return replacer.Replace(sql)
}
