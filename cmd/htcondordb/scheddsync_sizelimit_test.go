package main

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// TestArchiveSizeLimitConfig pins how the space-limit knobs resolve: a shared default applied to
// both archives, per-table overrides that win in either direction (including an explicit 0 that
// uncaps one table while the default caps the other), unit suffixes, and unset = 0 (no limit).
func TestArchiveSizeLimitConfig(t *testing.T) {
	resolve := func(extra string) scheddSyncSettings {
		return resolveScheddSyncSettings(mkSyncCfg(t, syncOn+extra))
	}

	if s := resolve(""); s.historyMaxBytes != 0 || s.epochMaxBytes != 0 {
		t.Errorf("unset caps = %d/%d, want 0/0 (no limit)", s.historyMaxBytes, s.epochMaxBytes)
	}

	// Shared default applies to both. humanize uses SI for "GB" (10^9).
	if s := resolve("HTCONDORDB_ARCHIVE_MAX_BYTES = 10 GB\n"); s.historyMaxBytes != 10_000_000_000 || s.epochMaxBytes != 10_000_000_000 {
		t.Errorf("default caps = %d/%d, want 10e9/10e9", s.historyMaxBytes, s.epochMaxBytes)
	}

	// Per-table override wins over the default.
	if s := resolve("HTCONDORDB_ARCHIVE_MAX_BYTES = 10GB\nHTCONDORDB_HISTORY_MAX_BYTES = 500 MB\n"); s.historyMaxBytes != 500_000_000 || s.epochMaxBytes != 10_000_000_000 {
		t.Errorf("override caps = %d/%d, want 5e8 history / 1e10 epoch", s.historyMaxBytes, s.epochMaxBytes)
	}

	// An explicit per-table 0 uncaps that table even though the default is set.
	if s := resolve("HTCONDORDB_ARCHIVE_MAX_BYTES = 10GB\nHTCONDORDB_EPOCH_HISTORY_MAX_BYTES = 0\n"); s.epochMaxBytes != 0 || s.historyMaxBytes != 10_000_000_000 {
		t.Errorf("explicit-0 caps = %d/%d, want 0 epoch / 1e10 history", s.epochMaxBytes, s.historyMaxBytes)
	}

	// Binary suffix (IEC).
	if s := resolve("HTCONDORDB_HISTORY_MAX_BYTES = 512MiB\n"); s.historyMaxBytes != 512*1024*1024 {
		t.Errorf("512MiB parsed as %d, want %d", s.historyMaxBytes, 512*1024*1024)
	}

	// A malformed value caps nothing rather than wedging startup.
	if s := resolve("HTCONDORDB_HISTORY_MAX_BYTES = not-a-size\n"); s.historyMaxBytes != 0 {
		t.Errorf("malformed cap = %d, want 0 (typo caps nothing)", s.historyMaxBytes)
	}
}

// TestApplyArchiveMaxBytesEnforced covers the half config alone cannot do: putting the cap onto a
// live archive's retention and having the ordinary rotation enforce it. Retention is a runtime
// setting (not persisted), so this is what makes a puppet-driven cap actually bound an existing
// archive on start/reconfig.
func TestApplyArchiveMaxBytesEnforced(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	// Small segments so a modest append seals several of them.
	hist, err := cat.CreateArchiveTable("history", db.ArchiveConfig{SegmentSize: 1 << 16, ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	pad := strings.Repeat("x", 300)
	for i := 0; i < 4000; i++ {
		if err := hist.AppendOld(fmt.Sprintf("ClusterId = %d\nOwner = \"alice\"\nPad = \"%s\"", i, pad)); err != nil {
			t.Fatal(err)
		}
	}
	total := hist.Count()
	if total != 4000 {
		t.Fatalf("appended %d, want 4000", total)
	}

	m := &scheddSyncManager{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	// Apply a tiny cap and confirm it lands on the archive's retention.
	m.applyArchiveMaxBytes(hist, "history", 64<<10) // 64 KiB
	if got := hist.Retention().MaxBytes; got != 64<<10 {
		t.Fatalf("after apply MaxBytes = %d, want %d", got, 64<<10)
	}

	// The ordinary rotation pass now enforces it: oldest whole segments drop until under the cap.
	dropped, err := hist.Rotate(float64(time.Now().Unix()))
	if err != nil {
		t.Fatal(err)
	}
	if dropped == 0 {
		t.Fatalf("cap not enforced: Rotate dropped 0 segments with a 64 KiB cap over %d records", total)
	}
	if after := hist.Count(); after >= total {
		t.Errorf("record count %d did not drop from %d after rotation under the cap", after, total)
	}

	// Removing the knob (0) lifts the ceiling rather than leaving a stale one.
	m.applyArchiveMaxBytes(hist, "history", 0)
	if got := hist.Retention().MaxBytes; got != 0 {
		t.Errorf("MaxBytes = %d after clearing, want 0 (no limit)", got)
	}
}
