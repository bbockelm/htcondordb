package plugin

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/bbockelm/htcondordb/arrowtype"
	"github.com/bbockelm/htcondordb/repl"
)

func typedDecision(name string, dt arrow.DataType) arrowtype.Decision {
	return arrowtype.Decision{
		Name:    name,
		Verdict: arrowtype.Typed,
		Field:   arrow.Field{Name: name, Type: dt, Nullable: true},
	}
}

// TestOneBadCellNoLongerRetypesTheColumn is the bug the declaration replaces, and it is worth
// stating as a before/after because the inference looks reasonable until you see what it does.
//
// numberFieldFrom bails on the FIRST non-numeric cell, so a column that is a number in every
// row but one becomes a STRING field -- and a Grafana graph of a string field is an empty
// panel. Which cell poisons it depends on the time range the panel happens to be showing, so
// the same dashboard renders on Monday and not on Tuesday.
func TestOneBadCellNoLongerRetypesTheColumn(t *testing.T) {
	res := &repl.Result{
		IsSelect: true,
		Columns:  []string{"Cpus"},
		Rows:     [][]string{{"8"}, {"16"}, {"n/a"}, {"4"}},
	}

	// Before: one bad cell and the whole column is text.
	inferred := resultToFrame("A", res, nil, "", "")
	if got := inferred.Fields[0].Type(); got != data.FieldTypeNullableString {
		t.Fatalf("inference gave %v; this test does not pose the problem it claims to", got)
	}

	// After: the declared type stands and the bad cell is a gap.
	declared := resultToFrame("A", res,
		[]arrowtype.Decision{typedDecision("Cpus", arrow.PrimitiveTypes.Int64)}, "", "")
	if got := declared.Fields[0].Type(); got != data.FieldTypeNullableInt64 {
		t.Fatalf("declared gave %v, want nullable int64", got)
	}
	if v := declared.Fields[0].At(0).(*int64); v == nil || *v != 8 {
		t.Errorf("row 0 = %v, want 8", v)
	}
	if v := declared.Fields[0].At(2).(*int64); v != nil {
		t.Errorf("the unparseable cell is %v, want a gap", *v)
	}
	if v := declared.Fields[0].At(3).(*int64); v == nil || *v != 4 {
		t.Errorf("row 3 = %v, want 4 -- a bad cell must not consume the rows after it", v)
	}
}

// TestDeclaredTypesCoverEveryKind walks the mapping, because a kind with no Grafana equivalent
// has to fall back rather than be invented, and the one that is easy to get wrong is the
// timestamp: it is stored as epoch seconds and must arrive as a time, or it graphs as a large
// integer on the Y axis.
func TestDeclaredTypesCoverEveryKind(t *testing.T) {
	res := &repl.Result{
		IsSelect: true,
		Columns:  []string{"I", "R", "B", "S", "T"},
		Rows:     [][]string{{"7", "1.5", "true", "hello", "1609459200"}},
	}
	decisions := []arrowtype.Decision{
		typedDecision("I", arrow.PrimitiveTypes.Int64),
		typedDecision("R", arrow.PrimitiveTypes.Float64),
		typedDecision("B", arrow.FixedWidthTypes.Boolean),
		typedDecision("S", arrow.BinaryTypes.String),
		typedDecision("T", arrow.FixedWidthTypes.Timestamp_s),
	}
	frame := resultToFrame("A", res, decisions, "", "")

	want := []data.FieldType{
		data.FieldTypeNullableInt64,
		data.FieldTypeNullableFloat64,
		data.FieldTypeNullableBool,
		data.FieldTypeNullableString,
		data.FieldTypeNullableTime,
	}
	for i, w := range want {
		if got := frame.Fields[i].Type(); got != w {
			t.Errorf("field %d (%s) is %v, want %v", i, res.Columns[i], got, w)
		}
	}
	if v := frame.Fields[4].At(0).(*time.Time); v == nil || v.Unix() != 1609459200 {
		t.Errorf("the timestamp is %v, want 2021-01-01T00:00:00Z", v)
	}
}

// TestAnExplicitTimeFieldOutranksTheDeclaration: the panel's own TimeField is a choice somebody
// made in the UI, and it has to beat both the declaration and the guess. An integer column the
// user has nominated as the time axis is the whole reason that control exists.
func TestAnExplicitTimeFieldOutranksTheDeclaration(t *testing.T) {
	res := &repl.Result{
		IsSelect: true,
		Columns:  []string{"Started"},
		Rows:     [][]string{{"1609459200"}},
	}
	frame := resultToFrame("A", res,
		[]arrowtype.Decision{typedDecision("Started", arrow.PrimitiveTypes.Int64)}, "Started", "")
	if got := frame.Fields[0].Type(); got != data.FieldTypeNullableTime {
		t.Errorf("Started is %v, want a time field: the panel named it as the time axis", got)
	}
}

// TestAnUndeclaredColumnStillInfers: a declaration that covers some columns and not others must
// not leave the rest untyped. An aggregate's output, a column added since the cache was filled,
// or a shape repl's parser spells differently all reach this.
func TestAnUndeclaredColumnStillInfers(t *testing.T) {
	res := &repl.Result{
		IsSelect: true,
		Columns:  []string{"Known", "Unknown"},
		Rows:     [][]string{{"7", "42"}},
	}
	frame := resultToFrame("A", res,
		[]arrowtype.Decision{typedDecision("Known", arrow.PrimitiveTypes.Int64)}, "", "")
	if got := frame.Fields[0].Type(); got != data.FieldTypeNullableInt64 {
		t.Errorf("the declared column is %v, want int64", got)
	}
	if got := frame.Fields[1].Type(); got != data.FieldTypeNullableFloat64 {
		t.Errorf("the undeclared column is %v, want the inferred float64", got)
	}
}

// TestDecisionCacheExpires: the cache is what keeps a dashboard from putting a sampling scan
// through the daemon per panel per refresh, and the TTL is what keeps a new column from being
// invisible until a restart. Both halves matter, so both are asserted.
func TestDecisionCacheExpires(t *testing.T) {
	c := newDecisionCache()
	now := time.Now()
	c.now = func() time.Time { return now }

	c.put("jobs", []arrowtype.Decision{typedDecision("Owner", arrow.BinaryTypes.String)})
	if _, ok := c.get("jobs"); !ok {
		t.Fatal("a fresh entry missed, so every panel pays a sampling scan")
	}
	// Case-insensitively, since a table name reaches this from a dashboard somebody typed.
	if _, ok := c.get("JOBS"); !ok {
		t.Error("the cache is case-sensitive about table names")
	}
	now = now.Add(decisionTTL)
	if _, ok := c.get("jobs"); ok {
		t.Error("an expired entry was served; a column added to the table would never appear")
	}
}
