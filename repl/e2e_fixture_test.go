package repl

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// The Grafana E2E fixture's continuous aggregate must survive a seal tick.
//
// Sealing moves a closed bucket out of a continuous aggregate's live backing and into its
// archive, so the view holds its rows in two places. A read that reaches only the backing
// returns the unsealed tail and nothing older -- which a dashboard shows as an empty panel,
// not as an error.
//
// Nothing caught that. The browser test queries the panel once, the seal cadence is half the
// bucket width (150s for the fixture's five-minute buckets), and it usually got there first;
// it failed only when a slow container build pushed it past one tick. Forcing the seal makes
// the race deterministic, and does it in a Go test rather than a browser one, so the check
// costs a second instead of a container stack.
func TestGrafanaFixtureSurvivesSeal(t *testing.T) {
	path := filepath.Join("..", "grafana", "e2e", "sample-data.sql")
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("fixture not readable: %v", err)
	}
	defer f.Close()

	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	e := NewExecutor(c, ExecConfig{})
	defer func() { c.Close(); s.Close(); cat.Close() }()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if _, xerr := e.ExecString(line); xerr != nil {
			t.Fatalf("fixture line %d failed to execute: %v\n  %.120s", n, xerr, line)
		}
	}
	if serr := sc.Err(); serr != nil {
		t.Fatal(serr)
	}

	// The query the continuous-aggregate dashboard panel issues.
	const dashboardQuery = `SELECT time, metric_events FROM demo_ca ORDER BY time`
	before, err := e.ExecString(dashboardQuery)
	if err != nil {
		t.Fatalf("dashboard query: %v", err)
	}
	if len(before.Rows) == 0 {
		t.Fatalf("the fixture's continuous aggregate is empty before any seal")
	}

	// Seal past every bucket's window, which is what the daemon's tick does on its own
	// within 150 seconds of startup.
	view, ok := cat.View("demo_ca")
	if !ok {
		t.Fatal("no demo_ca view")
	}
	view.Seal(newestEventTime(t, path) + 86400)

	after, err := e.ExecString(dashboardQuery)
	if err != nil {
		t.Fatalf("dashboard query after seal: %v", err)
	}
	if len(after.Rows) != len(before.Rows) {
		t.Errorf("sealing dropped rows from the continuous aggregate: %d before, %d after.\n"+
			"A read of a view that has sealed must reach its archive as well as its live "+
			"backing, or it returns only the buckets too recent to have sealed.",
			len(before.Rows), len(after.Rows))
	}
}

// newestEventTime returns the largest EventTime the fixture inserts, so the seal instant comes
// from the fixture rather than from a constant that has to be kept in step with it.
func newestEventTime(t *testing.T, path string) int64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var newest int64
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "INSERT INTO demo_events") {
			continue
		}
		lp, rp := strings.LastIndex(line, "("), strings.LastIndex(line, ")")
		if lp < 0 || rp < lp {
			continue
		}
		parts := strings.Split(line[lp+1:rp], ",")
		v, perr := strconv.ParseInt(strings.TrimSpace(parts[len(parts)-1]), 10, 64)
		if perr != nil {
			continue
		}
		if v > newest {
			newest = v
		}
	}
	if newest == 0 {
		t.Fatal("no demo_events EventTime found in the fixture")
	}
	return newest
}
