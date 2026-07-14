package repl

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// DefaultKeyAttr is the attribute a row's primary key is stored under when none
// is configured.
const DefaultKeyAttr = "Key"

// ExecConfig configures an Executor.
type ExecConfig struct {
	// KeyAttr is the ad attribute that carries a row's primary key (the db key).
	// Defaults to DefaultKeyAttr. INSERT stamps the key here; UPDATE and DELETE
	// recover a matched row's key from it.
	KeyAttr string

	// GenKey generates a key for an INSERT that does not supply the key column.
	// Defaults to a monotonic "row-<n>" generator.
	GenKey func() string
}

// Executor runs parsed statements against a dbrpc client.
type Executor struct {
	c       *dbrpc.Client
	keyAttr string
	genKey  func() string
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
	return &Executor{c: c, keyAttr: keyAttr, genKey: genKey}
}

// Result is the outcome of executing a statement.
type Result struct {
	IsSelect bool
	Columns  []string   // SELECT column headers
	Rows     [][]string // SELECT rows (cells aligned to Columns)
	Affected int        // rows written by INSERT/UPDATE/DELETE
	Note     string     // human-readable summary line (e.g. "UPDATE 3")
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
	default:
		return nil, fmt.Errorf("unknown statement kind")
	}
}

// ExecString parses then executes a single statement.
func (e *Executor) ExecString(s string) (*Result, error) {
	st, err := Parse(s)
	if err != nil {
		return nil, err
	}
	return e.Exec(st)
}

// constraint returns the WHERE constraint, defaulting to match-all.
func constraint(where string) string {
	if strings.TrimSpace(where) == "" {
		return "true"
	}
	return where
}

// queryAds runs the WHERE query and parses each returned ad.
func (e *Executor) queryAds(where string) ([]*classad.ClassAd, error) {
	texts, err := e.c.Query(constraint(where))
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
	ads, err := e.queryAds(st.Where)
	if err != nil {
		return nil, err
	}

	// Aggregate query: one summary row.
	if len(st.Items) > 0 && st.Items[0].IsAggregate() {
		return aggregate(st.Items, ads)
	}

	res := &Result{IsSelect: true}
	// Determine columns.
	if len(st.Items) == 1 && st.Items[0].Star {
		res.Columns = e.starColumns(ads)
	} else {
		for _, it := range st.Items {
			res.Columns = append(res.Columns, it.header())
		}
	}

	for i, ad := range ads {
		if st.Limit > 0 && i >= st.Limit {
			break
		}
		row := make([]string, len(res.Columns))
		for j, col := range res.Columns {
			row[j] = valueDisplay(ad.EvaluateAttr(col))
		}
		res.Rows = append(res.Rows, row)
	}
	return res, nil
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

	tx, err := e.c.Begin()
	if err != nil {
		return nil, err
	}
	if err := tx.NewClassAd(key, sb.String()); err != nil {
		_ = tx.Abort()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
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
	keys, err := e.matchedKeys(st.Where)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return &Result{Affected: 0, Note: "UPDATE 0"}, nil
	}

	tx, err := e.c.Begin()
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		for _, a := range st.Assignments {
			if err := tx.SetAttribute(key, a.Col, a.Expr); err != nil {
				_ = tx.Abort()
				return nil, fmt.Errorf("updating %s.%s: %w", key, a.Col, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Result{Affected: len(keys), Note: fmt.Sprintf("UPDATE %d", len(keys))}, nil
}

func (e *Executor) execDelete(st *Statement) (*Result, error) {
	keys, err := e.matchedKeys(st.Where)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return &Result{Affected: 0, Note: "DELETE 0"}, nil
	}
	tx, err := e.c.Begin()
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		if err := tx.DestroyClassAd(key); err != nil {
			_ = tx.Abort()
			return nil, fmt.Errorf("deleting %s: %w", key, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Result{Affected: len(keys), Note: fmt.Sprintf("DELETE %d", len(keys))}, nil
}

// matchedKeys returns the primary keys of every row matching where, recovered
// from the key attribute. It errors if a matched row lacks the key attribute
// (UPDATE/DELETE cannot address it).
func (e *Executor) matchedKeys(where string) ([]string, error) {
	ads, err := e.queryAds(where)
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

// --- aggregates ---

func aggregate(items []SelectItem, ads []*classad.ClassAd) (*Result, error) {
	res := &Result{IsSelect: true}
	row := make([]string, len(items))
	for i, it := range items {
		res.Columns = append(res.Columns, it.header())
		v, err := computeAggregate(it, ads)
		if err != nil {
			return nil, err
		}
		row[i] = v
	}
	res.Rows = [][]string{row}
	return res, nil
}

func computeAggregate(it SelectItem, ads []*classad.ClassAd) (string, error) {
	switch it.Agg {
	case "COUNT":
		if it.Col == "*" {
			return strconv.Itoa(len(ads)), nil
		}
		n := 0
		for _, ad := range ads {
			if v := ad.EvaluateAttr(it.Col); !v.IsUndefined() && !v.IsError() {
				n++
			}
		}
		return strconv.Itoa(n), nil
	case "SUM", "AVG":
		var sum float64
		var n int
		for _, ad := range ads {
			if f, ok := asFloat(ad.EvaluateAttr(it.Col)); ok {
				sum += f
				n++
			}
		}
		if it.Agg == "SUM" {
			return trimFloat(sum), nil
		}
		if n == 0 {
			return "undefined", nil
		}
		return trimFloat(sum / float64(n)), nil
	case "MIN", "MAX":
		return minMax(it, ads)
	default:
		return "", fmt.Errorf("unknown aggregate %s", it.Agg)
	}
}

func minMax(it SelectItem, ads []*classad.ClassAd) (string, error) {
	var (
		haveNum bool
		numAcc  float64
		haveStr bool
		strAcc  string
	)
	want := func(better bool) bool {
		return (it.Agg == "MIN") == better // MIN wants smaller; MAX wants larger
	}
	for _, ad := range ads {
		v := ad.EvaluateAttr(it.Col)
		if f, ok := asFloat(v); ok {
			if !haveNum || want(f < numAcc) {
				numAcc, haveNum = f, true
			}
			continue
		}
		if v.IsString() {
			s, _ := v.StringValue()
			if !haveStr || want(s < strAcc) {
				strAcc, haveStr = s, true
			}
		}
	}
	switch {
	case haveNum:
		return trimFloat(numAcc), nil
	case haveStr:
		return strAcc, nil
	default:
		return "undefined", nil
	}
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

func asFloat(v classad.Value) (float64, bool) {
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

// trimFloat formats a float without a trailing ".0" for whole numbers.
func trimFloat(f float64) string {
	s := strconv.FormatFloat(f, 'g', -1, 64)
	return s
}
