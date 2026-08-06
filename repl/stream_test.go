package repl

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// collectStream runs a SELECT through StreamSelect and returns the yielded ads.
func collectStream(t *testing.T, e *Executor, sql string) []*classad.ClassAd {
	t.Helper()
	st, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	var out []*classad.ClassAd
	if err := e.StreamSelect(st, func(ad *classad.ClassAd) bool {
		out = append(out, ad)
		return true
	}); err != nil {
		t.Fatalf("stream %q: %v", sql, err)
	}
	return out
}

// attrOf reads one attribute's evaluated string from an ad.
func attrOf(t *testing.T, ad *classad.ClassAd, name string) string {
	t.Helper()
	return valueDisplay(ad.EvaluateAttr(name))
}

func seedMachines(t *testing.T, e *Executor) {
	t.Helper()
	for _, m := range []struct {
		name  string
		cpus  int
		start string
	}{{"slot1", 4, "true"}, {"slot2", 2, "false"}, {"slot3", 16, "true"}} {
		mustExec(t, e, "INSERT INTO ads (Key, Name, Cpus, Start, WithinResourceLimits, Requirements) "+
			"VALUES ('"+m.name+"', '"+m.name+"', "+itoa(m.cpus)+", "+m.start+
			", true, Start && WithinResourceLimits)")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// The point of the stream: an attribute holding an expression arrives as that expression,
// not as the value it evaluates to. A result cell cannot carry this.
func TestStreamSelectKeepsExpressions(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	seedMachines(t, e)

	ads := collectStream(t, e, "SELECT * FROM ads")
	if len(ads) != 3 {
		t.Fatalf("got %d ads, want 3", len(ads))
	}
	for _, ad := range ads {
		expr, ok := ad.Lookup("Requirements")
		if !ok {
			t.Fatal("Requirements missing from a streamed ad")
		}
		if !strings.Contains(expr.String(), "Start") {
			t.Errorf("Requirements arrived as %q, want the unevaluated expression", expr.String())
		}
	}
}

func TestStreamSelectWhereAndLimit(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	seedMachines(t, e)

	if got := len(collectStream(t, e, "SELECT * FROM ads WHERE Cpus > 3")); got != 2 {
		t.Errorf("WHERE matched %d ads, want 2", got)
	}
	if got := len(collectStream(t, e, "SELECT * FROM ads LIMIT 2")); got != 2 {
		t.Errorf("LIMIT yielded %d ads, want 2", got)
	}
	if got := len(collectStream(t, e, "SELECT * FROM ads WHERE Cpus > 3 LIMIT 1")); got != 1 {
		t.Errorf("WHERE + LIMIT yielded %d ads, want 1", got)
	}
}

// Returning false stops the stream, which is what lets a caller abandon a large query.
func TestStreamSelectStopsEarly(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	seedMachines(t, e)

	st, err := Parse("SELECT * FROM ads")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	if err := e.StreamSelect(st, func(*classad.ClassAd) bool { n++; return false }); err != nil {
		t.Fatalf("stream: %v", err)
	}
	if n != 1 {
		t.Errorf("yielded %d ads after stopping, want 1", n)
	}
}

// A projected SELECT streams the named columns plus the siblings its expressions read, so
// a projected ad still evaluates -- the same guarantee the materializing path gives.
func TestStreamSelectProjectedAdsEvaluate(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	seedMachines(t, e)

	for _, ad := range collectStream(t, e, "SELECT Name, Requirements FROM ads WHERE Name == \"slot1\"") {
		if got := attrOf(t, ad, "Requirements"); got != "true" {
			t.Errorf("projected Requirements evaluated to %q, want \"true\"", got)
		}
	}
}

// ORDER BY and DISTINCT cannot stream; they must still produce correct, ordered results.
func TestStreamSelectOrderByAndDistinct(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	seedMachines(t, e)

	ads := collectStream(t, e, "SELECT * FROM ads ORDER BY Cpus DESC")
	var order []string
	for _, ad := range ads {
		order = append(order, attrOf(t, ad, "Cpus"))
	}
	if want := []string{"16", "4", "2"}; strings.Join(order, ",") != strings.Join(want, ",") {
		t.Errorf("ORDER BY gave %v, want %v", order, want)
	}

	if got := len(collectStream(t, e, "SELECT DISTINCT * FROM ads")); got != 3 {
		t.Errorf("DISTINCT yielded %d ads, want 3", got)
	}
}

// An aggregate has no ads of its own; one is synthesized per group row.
func TestStreamSelectAggregateAds(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	seedMachines(t, e)

	ads := collectStream(t, e, "SELECT Start, COUNT(*), SUM(Cpus) AS total FROM ads GROUP BY Start")
	if len(ads) != 2 {
		t.Fatalf("got %d group ads, want 2", len(ads))
	}
	for _, ad := range ads {
		// COUNT(*) folds to Count; the AS alias is kept as written.
		if _, ok := ad.Lookup("Count"); !ok {
			t.Errorf("group ad has no Count attribute: %v", ad)
		}
		if _, ok := ad.Lookup("total"); !ok {
			t.Errorf("group ad has no total attribute: %v", ad)
		}
		if attrOf(t, ad, "Start") == "" {
			t.Errorf("group ad lost its group column: %v", ad)
		}
	}
}

func TestStreamSelectRejectsNonSelect(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	st, err := Parse("INSERT INTO ads (Key) VALUES ('k')")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.StreamSelect(st, func(*classad.ClassAd) bool { return true }); err == nil {
		t.Error("StreamSelect accepted an INSERT")
	}
}

// The names an aggregate's columns fold to are the ad's attribute names, so they have to
// be legal -- "COUNT(*)" is not.
func TestAggregateAttrNames(t *testing.T) {
	for _, tc := range []struct {
		in   []string
		want []string
	}{
		{[]string{"Owner", "COUNT(*)"}, []string{"Owner", "Count"}},
		{[]string{"SUM(RequestMemory)"}, []string{"SumRequestMemory"}},
		{[]string{"AVG(Cpus)", "MIN(Cpus)", "MAX(Cpus)"}, []string{"AvgCpus", "MinCpus", "MaxCpus"}},
		{[]string{"total"}, []string{"total"}},                           // an AS alias, already legal
		{[]string{"COUNT(*)", "COUNT(*)"}, []string{"Count", "Count_2"}}, // collision
		{[]string{"SUM(Cpus) / COUNT(*)"}, []string{"SumCpusCOUNT"}},     // an expression: folded, legal but ugly -- use AS
		{[]string{"*"}, []string{"Col1"}},                                // nothing usable left
	} {
		got := aggregateAttrNames(tc.in)
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("aggregateAttrNames(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
	// Every derived name must actually be a legal attribute name.
	for _, name := range aggregateAttrNames([]string{"COUNT(*)", "SUM(a)", "weird!!name", "*"}) {
		if !attrNameSafe.MatchString(name) {
			t.Errorf("derived name %q is not a legal ClassAd attribute name", name)
		}
	}
}

// Aggregate cells arrive as text; the synthesized ad types them rather than leaving
// everything a string.
func TestAggregateAdTypes(t *testing.T) {
	ad := aggregateAd([]string{"N", "Avg", "Flag", "Name", "Missing"},
		[]string{"3", "2.5", "true", "alice", "undefined"})

	if v := ad.EvaluateAttr("N"); !v.IsInteger() {
		t.Errorf("N is %v, want an integer", v)
	}
	if v := ad.EvaluateAttr("Avg"); !v.IsReal() {
		t.Errorf("Avg is %v, want a real", v)
	}
	if v := ad.EvaluateAttr("Flag"); !v.IsBool() {
		t.Errorf("Flag is %v, want a bool", v)
	}
	if v := ad.EvaluateAttr("Name"); !v.IsString() {
		t.Errorf("Name is %v, want a string", v)
	}
	if v := ad.EvaluateAttr("Missing"); !v.IsUndefined() {
		t.Errorf("Missing is %v, want undefined", v)
	}
}
