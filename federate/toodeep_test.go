package federate

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/wire"
	"github.com/PelicanPlatform/classad/db/replicate"
)

// A replicated archive record the hub refuses as too deep is skipped and counted. Failing the
// session on it would replay the same record into the same refusal on every reconnect, wedging
// that source's history at it.
func TestArchiveSkipsTooDeepRecord(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)
	m := NewMetrics()
	s := newHistSink(t, hist, "ap1.example.org", &replicate.MemCursorStore{}, m)

	deep := histRecord(t, "ap1.example.org", 2)
	e, err := classad.ParseExpr(strings.Repeat("x || ", wire.MaxDepth+10) + "x")
	if err != nil {
		t.Fatal(err)
	}
	deep.InsertExpr("Deep", e)

	s.BeginSession()
	apply(t, s, reset(),
		upsert("r1", histRecord(t, "ap1.example.org", 1)),
		upsert("r2", deep),
		upsert("r3", histRecord(t, "ap1.example.org", 3)),
		synced("c1"))
	s.EndSession()

	if n := countArchive(t, hist, `true`); n != 2 {
		t.Fatalf("hub history holds %d records, want 2 (1.0 and 3.0)", n)
	}
	if v := val(m.TooDeep.WithLabelValues(TableHistory)); v != 1 {
		t.Errorf("too_deep_total = %v, want 1", v)
	}
}
