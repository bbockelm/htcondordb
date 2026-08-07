package repl

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// StreamSelect runs a SELECT and hands each result row to yield as a ClassAd, returning
// when the result is exhausted or yield returns false.
//
// Unlike Exec, a row-returning statement does not have to be materialized first: a plain
// SELECT streams straight from the server's result frames, so a caller can walk a large
// table at one ad in flight rather than the whole set. Two shapes still have to collect
// everything before the first row can be correct, and say so by collecting -- ORDER BY,
// which cannot know the first row until it has seen the last, and DISTINCT, which cannot
// know a row is new until it has seen every earlier one.
//
// An aggregate has no ads behind it: the server reduces rows to group tuples, and the
// ClassAds here are synthesized from those (see aggregateAd). The result is small by
// construction -- one row per group -- so it is computed whole and then streamed.
//
// Yielded ads are the caller's; the stream does not retain or reuse them.
func (e *Executor) StreamSelect(st *Statement, yield func(*classad.ClassAd) bool) error {
	if st.Kind != StmtSelect {
		return fmt.Errorf("only SELECT produces ClassAds")
	}

	// Aggregates, ORDER BY and DISTINCT all need the whole result in hand. Run the
	// ordinary executor and stream what it produced.
	if !e.streamable(st) {
		res, err := e.Exec(st)
		if err != nil {
			return err
		}
		return streamResult(res, yield)
	}

	limit := st.Limit
	n := 0
	deliver := func(text string, parse func(string) (*classad.ClassAd, error)) bool {
		ad, err := parse(text)
		if err != nil {
			return false // surfaced by the error the stream helpers return
		}
		n++
		if !yield(ad) {
			return false
		}
		return limit <= 0 || n < limit
	}

	ctx := context.Background()
	// Inside a transaction, stream through it so the caller sees its own uncommitted
	// writes -- the same rule the row-returning path follows. The transactional read op
	// has no projected variant, so a transaction gets whole ads; the projection is an
	// optimization and correctness comes first. This is also what makes a read-modify-write
	// safe: the read joins the transaction's snapshot, so a concurrent writer conflicts at
	// commit instead of being silently overwritten.
	if e.txReads(st.Table) {
		return e.tx.QueryStream(ctx, constraint(st.Where), limit,
			func(row string) bool { return deliver(row, classad.Parse) })
	}
	// Projected when every selected column is a plain attribute -- the same decision the
	// materializing path makes, so a streamed row carries exactly what a collected one
	// would, references chased and all.
	if proj := e.projectionAttrs(st); proj != nil {
		err := e.c.QueryRawProjectRefsStream(ctx, st.Table, constraint(st.Where), proj, limit,
			func(row string) bool { return deliver(row, classad.ParseOld) })
		if errors.Is(err, dbrpc.ErrProjectRefsUnsupported) {
			n = 0
			return e.c.QueryRawProjectStream(ctx, st.Table, constraint(st.Where), proj, limit,
				func(row string) bool { return deliver(row, classad.ParseOld) })
		}
		return err
	}
	// Whole ads: the server streams the bracketed new-ClassAd form.
	return e.c.QueryTableStream(ctx, st.Table, constraint(st.Where), limit,
		func(row string) bool { return deliver(row, classad.Parse) })
}

// streamable reports whether a SELECT's rows can be delivered as they arrive.
//
// ORDER BY and DISTINCT are row-set operations: neither can emit a correct first row
// before it has seen the last. An aggregate is excluded because its rows are synthesized,
// not streamed. An archive or AS OF read has no streaming client op.
func (e *Executor) streamable(st *Statement) bool {
	if len(st.OrderBy) > 0 || st.Distinct || st.AsOf != "" {
		return false
	}
	if len(effectiveGroupBy(st)) > 0 || hasAggregate(st) {
		return false
	}
	if e.isArchive(st.Table) {
		return false
	}
	return true
}

// streamResult hands a materialized Result's rows to yield as ClassAds: its own ads for a
// plain SELECT, synthesized ones for an aggregate.
func streamResult(res *Result, yield func(*classad.ClassAd) bool) error {
	if !res.IsSelect {
		return fmt.Errorf("statement returned no rows")
	}
	if res.Ads != nil {
		for _, ad := range res.Ads {
			if !yield(ad) {
				return nil
			}
		}
		return nil
	}
	names := aggregateAttrNames(res.Columns)
	for _, row := range res.Rows {
		if !yield(aggregateAd(names, row)) {
			return nil
		}
	}
	return nil
}

// aggregateAd builds one ClassAd from an aggregate result row, using names derived from
// the column headers. Cells are typed the way the display strings read -- an aggregate's
// values reach the client already reduced to text, so this is the same recovery the
// tabular and JSON renderings do.
func aggregateAd(names []string, row []string) *classad.ClassAd {
	ad := classad.New()
	for i, name := range names {
		if i >= len(row) {
			break
		}
		setAggregateAttr(ad, name, row[i])
	}
	return ad
}

// attrNameSafe matches a legal ClassAd attribute name.
var attrNameSafe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// aggregateAttrNames turns SELECT headers into legal ClassAd attribute names.
//
// A header is whatever the user wrote, and an aggregate's is a call: "COUNT(*)" and
// "SUM(RequestMemory)" are not attribute names. An AS alias is used as-is when it is
// legal; otherwise the header is folded to one -- the function and its argument run
// together ("COUNT(*)" -> "Count", "SUM(RequestMemory)" -> "SumRequestMemory"), and
// anything else has its illegal characters dropped. Empty or colliding results get a
// positional suffix, so the ad always has one attribute per column.
func aggregateAttrNames(columns []string) []string {
	out := make([]string, len(columns))
	seen := map[string]int{}
	for i, col := range columns {
		name := deriveAttrName(col)
		if name == "" {
			name = fmt.Sprintf("Col%d", i+1)
		}
		if k := strings.ToLower(name); seen[k] > 0 {
			seen[k]++
			name = fmt.Sprintf("%s_%d", name, seen[k])
		} else {
			seen[k] = 1
		}
		out[i] = name
	}
	return out
}

// deriveAttrName folds one column header into a legal attribute name, or "" if nothing
// usable is left.
func deriveAttrName(col string) string {
	col = strings.TrimSpace(col)
	if attrNameSafe.MatchString(col) {
		return col // a plain column, or an AS alias the user already made legal
	}
	// A function call: join the name and its argument, dropping "*" (COUNT(*) -> Count).
	if open := strings.IndexByte(col, '('); open > 0 && strings.HasSuffix(col, ")") {
		fn := titleFold(col[:open])
		arg := strings.TrimSpace(col[open+1 : len(col)-1])
		if arg == "*" {
			arg = ""
		}
		if name := scrubName(fn + titleFold(arg)); name != "" {
			return name
		}
	}
	return scrubName(col)
}

// titleFold upper-cases the first rune and lower-cases nothing else, so COUNT -> Count
// while an attribute argument keeps the spelling the user wrote.
func titleFold(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if s == strings.ToUpper(s) { // an all-caps function name reads better folded
		return strings.ToUpper(s[:1]) + strings.ToLower(s[1:])
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// scrubName drops characters a ClassAd attribute name cannot hold, and any leading digits.
func scrubName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r == '_':
			b.WriteRune(r)
		case r >= '0' && r <= '9' && b.Len() > 0:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// setAggregateAttr inserts one aggregate cell, typed from its display text.
func setAggregateAttr(ad *classad.ClassAd, name, cell string) {
	switch cell {
	case "undefined", "":
		return // leaving it out is how a ClassAd spells undefined
	case "error":
		if e, err := classad.ParseExpr("error"); err == nil {
			ad.InsertExpr(name, e)
		}
		return
	case "true":
		ad.InsertAttrBool(name, true)
		return
	case "false":
		ad.InsertAttrBool(name, false)
		return
	}
	if n, err := parseAggInt(cell); err == nil {
		ad.InsertAttr(name, n)
		return
	}
	if f, err := parseAggFloat(cell); err == nil {
		ad.InsertAttrFloat(name, f)
		return
	}
	ad.InsertAttrString(name, cell)
}

// parseAggInt and parseAggFloat recover a number from an aggregate's display text. They
// reject the spellings ClassAd never renders (inf, NaN, hex), so a string cell that looks
// numeric is not silently turned into one.
func parseAggInt(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }

func parseAggFloat(s string) (float64, error) {
	for _, r := range s {
		if (r < '0' || r > '9') && r != '.' && r != '-' && r != '+' && r != 'e' && r != 'E' {
			return 0, strconv.ErrSyntax
		}
	}
	return strconv.ParseFloat(s, 64)
}
