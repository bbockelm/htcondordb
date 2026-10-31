package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// The server now survives a read that panics, so the daemon staying up no longer says nothing went
// wrong; this counter is what does. It has to appear in a scrape, carrying the live count, to be
// alerted on.
func TestRequestPanicsIsScrapeable(t *testing.T) {
	cat, err := db.OpenCatalogConfig(db.CatalogConfig{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cat.Close() }()
	scrape := func() string {
		rec := httptest.NewRecorder()
		Handler(t.Context(), cat, nil, nil, nil).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
		return rec.Body.String()
	}
	const name = "htcondordb_dbrpc_request_panics_total"

	// Nothing in this process has served a request, so the real counter reads zero; anything else
	// would fire an "any increase" alert on every deployment.
	body := scrape()
	if !strings.Contains(body, "\n"+name+" 0\n") {
		t.Fatalf("%s does not read 0 in a process that served nothing:\n%s", name, grepMetric(body, name))
	}
	// The value alone reads like a crash count; the help text has to say where the panic went.
	if !strings.Contains(body, "daemon log") {
		t.Error("help text does not say the panic and its stack are in the daemon log")
	}

	was := requestPanics
	t.Cleanup(func() { requestPanics = was })
	requestPanics = func() int64 { return 3 }
	if body := scrape(); !strings.Contains(body, "\n"+name+" 3\n") {
		t.Errorf("%s does not carry the live count:\n%s", name, grepMetric(body, name))
	}
}

func grepMetric(body, name string) string {
	var out []string
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, name) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
