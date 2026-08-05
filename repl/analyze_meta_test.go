package repl

import (
	"strings"
	"testing"
)

// TestAnalyzeMeta verifies `.analyze` reaches the dbrpc analyze admin action for both a mutable
// table (self-tune: hot set + indexes + histograms, with an optional topN) and an append-only
// archive (reindex selectivity stats), routing by an explicit [table] argument.
func TestAnalyzeMeta(t *testing.T) {
	e, cleanup := newArchiveExec(t) // "history" archive + "jobs" mutable table, privileged
	defer cleanup()
	s := &session{exec: e, table: "jobs"}

	// Mutable: analyze refreshes the hot set / indexes / histograms.
	out := runMeta(t, s, ".analyze", "jobs")
	if strings.Contains(out, "no such table") || strings.Contains(strings.ToLower(out), "error") {
		t.Fatalf(".analyze jobs: %q", out)
	}
	if !strings.Contains(strings.ToLower(out), "analyz") {
		t.Errorf(".analyze jobs did not report analysis: %q", out)
	}
	// Optional hot-set-size override (analyze <topN>).
	if out := runMeta(t, s, ".analyze", "jobs 8"); strings.Contains(strings.ToLower(out), "error") {
		t.Errorf(".analyze jobs 8: %q", out)
	}
	// Archive: analyze reindexes selectivity statistics.
	out = runMeta(t, s, ".analyze", "history")
	if strings.Contains(out, "no such table") || strings.Contains(strings.ToLower(out), "error") {
		t.Fatalf(".analyze history: %q", out)
	}
	if !strings.Contains(strings.ToLower(out), "analyz") {
		t.Errorf(".analyze history did not report analysis: %q", out)
	}
}
