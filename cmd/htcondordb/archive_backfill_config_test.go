package main

import (
	"testing"

	"github.com/bbockelm/golang-htcondor/logging"
)

// TestArchiveIndexBackfillBudget pins the translation from HTCondor config to the library's
// spelling: unset is the library default (0), a size is the budget, 0 is off (negative), and
// a typo falls back to the default rather than to off.
func TestArchiveIndexBackfillBudget(t *testing.T) {
	logger, err := logging.New(&logging.Config{OutputPath: "stderr", SkipGlobalInstall: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		body string
		want int64
	}{
		{"", 0},
		{"HTCONDORDB_ARCHIVE_INDEX_BACKFILL_BYTES = 512 MiB\n", 512 << 20},
		{"HTCONDORDB_ARCHIVE_INDEX_BACKFILL_BYTES = 1048576\n", 1 << 20},
		{"HTCONDORDB_ARCHIVE_INDEX_BACKFILL_BYTES = 0\n", -1},
		{"HTCONDORDB_ARCHIVE_INDEX_BACKFILL_BYTES = lots\n", 0},
	} {
		if got := archiveIndexBackfillBudget(importerCfg(t, tc.body), logger); got != tc.want {
			t.Errorf("%q: %d, want %d", tc.body, got, tc.want)
		}
	}
}
