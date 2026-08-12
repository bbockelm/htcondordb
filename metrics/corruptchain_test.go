package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// The corruption counter has to be REACHABLE, not just registered: a gauge whose family never appears in a
// scrape is a metric nobody can alert on, and that is the whole point of exporting it.
func TestCorruptChainLinksIsScrapeable(t *testing.T) {
	cat, err := db.OpenCatalogConfig(db.CatalogConfig{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	if _, err := cat.EnsureTable("jobs"); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	Handler(cat, nil, nil, nil).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()

	const name = "htcondordb_store_corrupt_chain_links_total"
	if !strings.Contains(body, name) {
		t.Fatalf("%s is absent from a scrape; a metric that does not appear cannot be alerted on", name)
	}
	// A healthy store must read zero, or an alert on "any increase" fires on every deployment.
	if !strings.Contains(body, name+" 0") {
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, name) {
				t.Errorf("healthy store reports %q, want 0", line)
			}
		}
	}
	// And the help text has to say what to do about it, since the value alone reads like a data-corruption
	// count when it is actually a lifetime bug.
	if !strings.Contains(body, "segment-lifetime") {
		t.Error("help text does not say this is a segment-lifetime bug; the number alone invites the wrong " +
			"conclusion that records on disk are corrupt")
	}
}
