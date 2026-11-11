package federate

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuf collects log output written from the hub's own goroutine.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// The warning has to reach the log through a real discovery pass, not just by
// calling the summary directly: the whole failure being guarded against is a
// pass that completes and reports nothing.
func TestDiscoveryPassReportsAnEmptyMatchSet(t *testing.T) {
	dir := t.TempDir()
	disc := &fakeDiscovery{}

	// A schedd is advertising, but the constraint matched none -- what a
	// wrong expression looks like from here.
	snap := members("ap1")
	snap.Matched = map[string]bool{}
	snap.Spokes = map[string]Spoke{}
	disc.set(snap)

	buf := &syncBuf{}
	cat := openCatalog(t, dir+"/db")
	startHub(t, Config{
		Catalog: cat, Discovery: disc, Dial: unreachable, CursorDir: dir + "/cursors",
		Logger: slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})

	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(buf.String(), "matches none of the schedds") {
		if time.Now().After(deadline) {
			t.Fatalf("a discovery pass that matched nothing reported nothing; log was:\n%s", buf.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A constraint that matches nothing looks exactly like a healthy pass at
// every layer below the hub, so the warning is the only thing standing
// between a typo and a hub that silently federates nothing.
func TestSummarizeDiscoveryWarnsWhenNothingMatches(t *testing.T) {
	var buf bytes.Buffer
	h := &Hub{
		log: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		cfg: Config{Discovery: &Discovery{ScheddConstraint: `regexp("\\.example\\.com$", Name)`}},
	}

	// Forty schedds advertising, none of them wanted.
	present := map[string]bool{}
	for _, n := range []string{"ap1.example.com", "ap2.example.com"} {
		present[n] = true
	}
	snap := Snapshot{
		Present: present, PresentKnown: true,
		Matched: map[string]bool{}, MatchKnown: true,
	}

	h.summarizeDiscovery(snap)
	out := buf.String()
	if !strings.Contains(out, "matches none of the schedds") {
		t.Fatalf("a pass that matched nothing logged no warning; got:\n%s", out)
	}
	if !strings.Contains(out, "schedds_present=2") {
		t.Errorf("the warning should say how many schedds were there; got:\n%s", out)
	}
	// The constraint is the thing to go look at, so it has to be in the line.
	if !strings.Contains(out, "example") {
		t.Errorf("the warning should quote the constraint; got:\n%s", out)
	}

	// Still wrong on the next pass: saying so every minute forever is noise.
	buf.Reset()
	h.summarizeDiscovery(snap)
	if strings.Contains(buf.String(), "matches none of the schedds") {
		t.Errorf("the warning repeated on an unchanged pass; got:\n%s", buf.String())
	}

	// Fixed: the recovery has to be visible too, or the log only ever
	// records the bad state.
	buf.Reset()
	h.summarizeDiscovery(Snapshot{
		Present: present, PresentKnown: true,
		Matched: map[string]bool{"ap1.example.com": true}, MatchKnown: true,
	})
	if !strings.Contains(buf.String(), "matches again") {
		t.Errorf("recovery was not logged; got:\n%s", buf.String())
	}
}

// An empty pool is not a misconfiguration: with no schedd advertising there is
// nothing to conclude about the constraint.
func TestSummarizeDiscoveryQuietOnEmptyPool(t *testing.T) {
	var buf bytes.Buffer
	h := &Hub{
		log: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		cfg: Config{Discovery: &Discovery{ScheddConstraint: `true`}},
	}
	h.summarizeDiscovery(Snapshot{
		Present: map[string]bool{}, PresentKnown: true,
		Matched: map[string]bool{}, MatchKnown: true,
	})
	if strings.Contains(buf.String(), "matches none of the schedds") {
		t.Errorf("an empty pool was reported as a bad constraint; got:\n%s", buf.String())
	}
	// The counts still belong in the log at debug.
	if !strings.Contains(buf.String(), "discovery pass") {
		t.Errorf("the pass itself was not logged; got:\n%s", buf.String())
	}
}
