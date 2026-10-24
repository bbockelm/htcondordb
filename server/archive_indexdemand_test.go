package server

import (
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// TestArchiveIndexAutoTuneOnByDefault is the regression for a feature that shipped dark.
// classad implements archive index auto-tune and calls it from the maintenance pass, but
// only when MaintainOptions.ArchiveIndexMinDemand > 0 -- and the library leaves it at 0,
// which switches it off. Nothing in htcondordb ever set it, so every pass collected and
// checkpointed the archive's demand counters and then acted on none of them.
func TestArchiveIndexAutoTuneOnByDefault(t *testing.T) {
	if n := defaultMaintainOptions().ArchiveIndexMinDemand; n <= 0 {
		t.Fatalf("defaultMaintainOptions().ArchiveIndexMinDemand = %d, want > 0 so a history "+
			"table's accumulated query demand is actually acted on", n)
	}
}

// TestArchiveIndexMinDemandIsStricterThanMutable: the archive threshold is deliberately not
// inherited from MinIndexDemand. A mutable index is built from live data; an archive index
// is backfilled by decompressing history, and auto-drop is force-disabled for archives, so
// a wrong add is permanent. If a later edit set the two equal, this is what notices.
func TestArchiveIndexMinDemandIsStricterThanMutable(t *testing.T) {
	opts := defaultMaintainOptions()
	if opts.ArchiveIndexMinDemand <= opts.MinIndexDemand {
		t.Errorf("archive threshold %d <= mutable %d; an archive index costs a history backfill "+
			"and cannot be auto-dropped, so it must ask for more evidence",
			opts.ArchiveIndexMinDemand, opts.MinIndexDemand)
	}
}

// TestArchiveIndexMinDemandConfigured: the knob reaches the maintenance options the service
// actually runs with, in both directions -- a different threshold, and off.
func TestArchiveIndexMinDemandConfigured(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
		want int64
	}{
		{"unset takes the default", Config{}, defaultArchiveIndexMinDemand},
		{"override", Config{ArchiveIndexMinDemand: 42}, 42},
		{"negative is off", Config{ArchiveIndexMinDemand: -1}, 0},
		{
			"applied over a supplied Maintenance",
			Config{Maintenance: &db.MaintainOptions{HotTopN: 4}, ArchiveIndexMinDemand: 7},
			7,
		},
		{
			"a supplied Maintenance alone is respected",
			Config{Maintenance: &db.MaintainOptions{HotTopN: 4, ArchiveIndexMinDemand: 3}},
			3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveMaintainOptions(tc.cfg).ArchiveIndexMinDemand; got != tc.want {
				t.Errorf("ArchiveIndexMinDemand = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestArchiveIndexMinDemandReachesTheRunningService closes the gap the table above cannot:
// that New actually applies the resolved options rather than computing them and handing
// StartMaintenance something else.
func TestArchiveIndexMinDemandReachesTheRunningService(t *testing.T) {
	svc, err := New(Config{Dir: t.TempDir(), Authorize: allowAll, ArchiveIndexMinDemand: 55})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = svc.Close() }()
	if got := svc.maintainOpts.ArchiveIndexMinDemand; got != 55 {
		t.Errorf("running service's ArchiveIndexMinDemand = %d, want the configured 55", got)
	}
}
