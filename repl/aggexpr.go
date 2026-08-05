package repl

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// Expressions over aggregates: `SELECT 2 * COUNT(*)`, `SELECT SUM(Cpus) / COUNT(*)`,
// `SELECT SUM(x) > 1000 ? "over" : "under"`.
//
// The parser lifts each aggregate call out of the SELECT expression and leaves a placeholder
// attribute behind (see captureSelectExpr), so execution splits cleanly in two: the store
// still computes only plain aggregates -- nothing about the wire protocol or the server's
// GROUP BY changes -- and the surrounding arithmetic is evaluated here, once per group, by
// the ClassAd engine. Because the residual really is a ClassAd expression, the whole language
// comes with it: ?:, string functions, member(), comparisons, and so on.
//
// aggProjector holds the per-item wiring that turns one aggregate group -- a group tuple plus
// a vector of aggregate values -- into an output row.
type aggProjector struct {
	items    []SelectItem
	compiled []*classad.Expr // per item; nil unless the item has lifted aggregates
	aggAt    []int           // per item, index of its first value in the aggregate vector
	groupAt  []int           // per item, index into the group tuple; -1 if not a group column
	groupCol []string        // group tuple position -> attribute name, for the expression scope
}

// newAggProjector wires each SELECT item to its source. groupAt maps an item to its position
// in the group tuple (-1 for items that are not plain group columns), and groupCol names the
// group tuple's columns so an expression can reference them by attribute name.
func newAggProjector(items []SelectItem, groupAt []int, groupCol []string) (*aggProjector, error) {
	p := &aggProjector{
		items:    items,
		compiled: make([]*classad.Expr, len(items)),
		aggAt:    make([]int, len(items)),
		groupAt:  groupAt,
		groupCol: groupCol,
	}
	next := 0
	for i, it := range items {
		p.aggAt[i] = next
		next += len(it.aggCalls())
		if len(it.Aggs) == 0 {
			continue
		}
		ex, err := classad.ParseExpr(it.Expr)
		if err != nil {
			return nil, fmt.Errorf("SELECT expression %q: %w", it.Col, err)
		}
		p.compiled[i] = ex
	}
	return p, nil
}

// specs returns the aggregates to request, flattened in item order. aggAt[i] indexes each
// item's first result, so the projector can find its own values again in the reply.
func (p *aggProjector) specs() []dbrpc.AggSpec {
	var out []dbrpc.AggSpec
	for _, it := range p.items {
		for _, a := range it.aggCalls() {
			out = append(out, dbrpc.AggSpec{Func: aggFunc(a.Func), Arg: a.Arg})
		}
	}
	return out
}

func (p *aggProjector) columns() []string {
	cols := make([]string, len(p.items))
	for i, it := range p.items {
		cols[i] = it.header()
	}
	return cols
}

// row renders one output row from a group tuple and its aggregate values.
func (p *aggProjector) row(group, values []string) []string {
	row := make([]string, len(p.items))
	for i, it := range p.items {
		switch {
		case p.compiled[i] != nil:
			row[i] = valueDisplay(p.compiled[i].Eval(p.scope(i, group, values)))
		case it.IsAggregate():
			row[i] = at(values, p.aggAt[i])
		default:
			row[i] = at(group, p.groupAt[i])
		}
	}
	return row
}

// scope builds the ClassAd an expression item is evaluated against: its own aggregates bound
// to __agg_0, __agg_1, ... in the order they appeared, plus every group column bound by
// attribute name, so `SUM(Cpus) / COUNT(*)` and `Owner` can both appear in one expression.
func (p *aggProjector) scope(i int, group, values []string) *classad.ClassAd {
	ad := classad.New()
	for k := range p.items[i].Aggs {
		setLiteral(ad, fmt.Sprintf("%s%d", aggPlaceholderPrefix, k), at(values, p.aggAt[i]+k))
	}
	for gi, name := range p.groupCol {
		if name != "" {
			setLiteral(ad, name, at(group, gi))
		}
	}
	return ad
}

// setLiteral binds one wire value into the evaluation scope. Aggregate and group values
// arrive as display text, so a numeric one is bound as a number and anything else as a
// string -- an empty value is left undefined, which is what a missing group is.
func setLiteral(ad *classad.ClassAd, name, v string) {
	if v == "" {
		return
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		ad.InsertAttr(name, n)
		return
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		ad.InsertAttrFloat(name, f)
		return
	}
	if b, err := strconv.ParseBool(v); err == nil {
		ad.InsertAttrBool(name, b)
		return
	}
	ad.InsertAttrString(name, strings.Trim(v, `"`))
}

func at(vals []string, i int) string {
	if i < 0 || i >= len(vals) {
		return ""
	}
	return vals[i]
}

// rejectAggExprs refuses expressions over aggregates where only a plain aggregate will do --
// a materialized view stores raw metrics, and a WATCH streams rows rather than groups.
func rejectAggExprs(items []SelectItem, what string) error {
	for _, it := range items {
		if len(it.Aggs) > 0 {
			return fmt.Errorf("%s does not support expressions over aggregates (%s); "+
				"select the aggregate itself", what, it.Col)
		}
	}
	return nil
}

// reduceAggs computes every aggregate the SELECT needs over one group's ads, flattened in
// the same item order aggSpecs uses -- so a client-side group produces the value vector the
// projector expects from the server.
func reduceAggs(st *Statement, ads []*classad.ClassAd) []string {
	var vals []string
	for _, it := range st.Items {
		for _, a := range it.aggCalls() {
			vals = append(vals, aggregateAds(a, ads))
		}
	}
	return vals
}
