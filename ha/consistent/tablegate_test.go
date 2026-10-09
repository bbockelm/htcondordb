package consistent

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// TestApplyRefusesReadOnlyTable: a DBControl write batch touching a table TableWritable refuses
// is rejected whole, before it is proposed, naming the table; a table-unqualified op is checked
// against the default table.
func TestApplyRefusesReadOnlyTable(t *testing.T) {
	c := &Coordinator{cfg: CoordinatorConfig{
		DefaultTable:  "ads",
		TableWritable: func(table string) bool { return table != "jobs" && table != "ads" },
	}}
	for name, b := range map[string]*Batch{
		"qualified":   NewBatch().NewClassAdIn("scratch", "1", "A = 1").SetAttributeIn("jobs", "1.0", "X", "1"),
		"unqualified": NewBatch().NewClassAd("1", "A = 1"),
	} {
		req, err := BuildApplyRequest(b)
		if err != nil {
			t.Fatal(err)
		}
		resp := classad.New()
		c.handleApply(req, resp) // a nil raft would panic if the batch got as far as Apply
		if ok, _ := resp.EvaluateAttrBool(AttrResult); ok {
			t.Fatalf("%s: batch applied, want refused", name)
		}
		if e, _ := resp.EvaluateAttrString(AttrErrorString); !strings.Contains(e, "read-only table") {
			t.Errorf("%s: error = %q", name, e)
		}
	}
}
