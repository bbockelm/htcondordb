package repl

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// A site-installed aggregate, registered under a name no other test uses: db's registry is
// process-global and has no unregister.
type testP95 struct{}

func (testP95) NewAccumulator() db.Accumulator { return &testP95Acc{} }

type testP95Acc struct{ n int64 }

func (a *testP95Acc) Add(classad.Value)      { a.n++ }
func (a *testP95Acc) Merge(o db.Accumulator) { a.n += o.(*testP95Acc).n }
func (a *testP95Acc) Result() classad.Value  { return classad.NewIntValue(a.n) }

func init() {
	if err := db.RegisterAggregate("replTestP95", testP95{}); err != nil {
		panic(err)
	}
}

// TestUserAggregateParsesAsAnAggregate is the whole point: before this, isAggName listed five
// names, so `replTestP95(Cpus)` was not an aggregate call at all. It was captured as an ordinary
// SELECT expression and evaluated per row, client-side -- the wrong answer, computed successfully.
func TestUserAggregateParsesAsAnAggregate(t *testing.T) {
	st, err := Parse(`SELECT Owner, replTestP95(Cpus) FROM jobs GROUP BY Owner`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var agg *SelectItem
	for i := range st.Items {
		if st.Items[i].IsAggregate() {
			agg = &st.Items[i]
		}
	}
	if agg == nil {
		t.Fatal("the installed aggregate was not recognized as an aggregate call")
	}
}

// TestUserAggregateGetsItsOwnSelector is the failure that would be invisible. aggFunc fell through
// to COUNT for anything it did not recognize, so recognizing the name without mapping it would
// have sent COUNT(Cpus) and reported the answer as the aggregate's.
func TestUserAggregateGetsItsOwnSelector(t *testing.T) {
	st, err := Parse(`SELECT Owner, replTestP95(Cpus) FROM jobs GROUP BY Owner`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	specs := aggSpecsFor(st)
	if len(specs) != 1 {
		t.Fatalf("got %d aggregate specs, want 1: %+v", len(specs), specs)
	}
	if specs[0].Func != db.AggUser {
		t.Errorf("selector = %v, want AggUser; a wrong selector is answered successfully and wrongly", specs[0].Func)
	}
	if !strings.EqualFold(specs[0].UserFunc, "replTestP95") {
		t.Errorf("UserFunc = %q, want replTestP95; the name is the only part of the request identifying which aggregate", specs[0].UserFunc)
	}
	if specs[0].Arg != "Cpus" {
		t.Errorf("Arg = %q, want Cpus", specs[0].Arg)
	}
}

// TestBuiltinAggregatesAreUnchanged -- a built-in must keep its own selector, and must not be
// shadowed by a registration of the same name.
func TestBuiltinAggregatesAreUnchanged(t *testing.T) {
	st, err := Parse(`SELECT Owner, SUM(Cpus), COUNT(*) FROM jobs GROUP BY Owner`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	specs := aggSpecsFor(st)
	if len(specs) != 2 {
		t.Fatalf("got %d specs, want 2", len(specs))
	}
	if specs[0].Func != db.AggSum || specs[0].UserFunc != "" {
		t.Errorf("SUM = %v/%q, want AggSum with no name", specs[0].Func, specs[0].UserFunc)
	}
	if specs[1].Func != db.AggCount || specs[1].UserFunc != "" {
		t.Errorf("COUNT = %v/%q, want AggCount with no name", specs[1].Func, specs[1].UserFunc)
	}
}

// TestUnregisteredNameIsNotAnAggregate -- a name nobody installed must stay whatever it was
// before, so this change cannot turn an existing scalar call into an aggregate.
func TestUnregisteredNameIsNotAnAggregate(t *testing.T) {
	if isAggName("REPLTESTNOTREGISTERED") {
		t.Error("an unregistered name was taken for an aggregate")
	}
}

// TestUserAggregateNameFoldsCase -- every ClassAd function name folds, and the parser upper-cases
// its token, so a name registered in mixed case has to be found anyway.
func TestUserAggregateNameFoldsCase(t *testing.T) {
	if !isAggName("REPLTESTP95") {
		t.Error("the upper-cased token did not match a name registered in mixed case")
	}
}
