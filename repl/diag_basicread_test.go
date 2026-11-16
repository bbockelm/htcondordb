package repl

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestBasicReadViewsRenderIdentically is the contract that makes withBasicDiag safe: a view
// routed to it must render byte-identically from a basic report and a full one. A basic report
// leaves Suggestions, DropSuggestions, IndexSizes and the sidecar MPH/bloom breakdown at zero,
// and zero is indistinguishable from measured-and-empty at the point of printing -- so a view
// that grows a line reading one of them would quietly print nothing instead of failing.
//
// The fixture must therefore be one where the full read DOES report those fields, which the
// preconditions below assert rather than assume.
func TestBasicReadViewsRenderIdentically(t *testing.T) {
	e, cleanup := archiveExecBig(t, 4000)
	defer cleanup()

	// Demand, so the full read has suggestions to report: filter on an attribute the archive
	// does not index. Without this the comparison holds vacuously on the field that motivated
	// the whole split.
	for i := 0; i < 8; i++ {
		if _, execErr := e.ExecString("SELECT * FROM history WHERE JobStatus == 4"); execErr != nil {
			t.Fatal(execErr)
		}
	}

	full, err := e.Diagnostics("history")
	if err != nil {
		t.Fatal(err)
	}
	basic, err := e.DiagnosticsBasic("history")
	if err != nil {
		t.Fatal(err)
	}
	if !full.Detail {
		t.Fatal("the full read does not report itself as detailed")
	}
	if basic.Detail {
		t.Fatal("the basic read reports itself as detailed")
	}
	if len(full.Suggestions) == 0 && full.IndexSizes.TotalBytes == 0 && full.SidecarSizes.BloomBytes == 0 {
		t.Fatal("the full read measured none of the detail-only fields; this fixture cannot " +
			"distinguish a view that reads one from a view that does not")
	}
	if full.SidecarSizes.MappedBytes == 0 {
		t.Fatal("no sealed sidecars in the fixture")
	}

	s := &session{exec: e, table: "history"}
	for _, v := range []struct {
		name string
		fn   func(io.Writer, *dbrpc.Diagnostics)
	}{
		{".stats", s.showStats},
		{".hot", s.showHot},
		{".hot (archive)", s.showArchiveHot},
		{".schema", s.showSchema},
		{".groups", s.showGroups},
		{".require", s.showRequire},
	} {
		t.Run(v.name, func(t *testing.T) {
			var fullOut, basicOut bytes.Buffer
			v.fn(&fullOut, full)
			v.fn(&basicOut, basic)
			if fullOut.String() != basicOut.String() {
				t.Errorf("%s renders differently from a basic read, so it reads a field the "+
					"basic read does not measure -- route it through withDiag instead:\n"+
					"--- from the full read ---\n%s\n--- from the basic read ---\n%s",
					v.name, fullOut.String(), basicOut.String())
			}
			if strings.TrimSpace(fullOut.String()) == "" {
				t.Errorf("%s rendered nothing; the comparison is vacuous", v.name)
			}
		})
	}
}
