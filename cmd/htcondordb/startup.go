package main

import (
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bbockelm/golang-htcondor/logging"
)

// Startup phase timing.
//
// Startup logs a line when the daemon begins and another when the command socket is up, with
// everything in between silent. A slow start therefore shows only as a gap between two
// timestamps -- enough to notice ("suspiciously close to exactly 10s") and not enough to act on,
// because the gap covers reading security config, building the authorization policy, opening the
// database, and adopting the listener, any of which could own it.
//
// This records each phase and logs the breakdown, so the next slow start names the culprit
// instead of prompting an investigation.

// slowPhase is the per-phase duration above which a phase is called out on its own line rather
// than only appearing in the summary. Opening a database over a large spool legitimately takes a
// while; a second spent reading config files does not.
const slowPhase = time.Second

// slowTableOpen is the threshold for calling out one table's open. It is far below slowPhase because
// the failure mode is arithmetic rather than a single stall: sixty tables at 250ms is a fifteen-second
// startup in which no individual open looks remarkable, and that is exactly the shape that went
// unexplained. A quarter second to open one table is already worth a line.
const slowTableOpen = 250 * time.Millisecond

// slowestKept bounds how many table opens the summary names. A catalog can hold hundreds; naming them
// all would push the line past what any log reader will read, and the tail is not what anyone acts on.
const slowestKept = 5

// startupTimer accumulates startup phase durations.
//
// Two levels. phases are the top-level spans, measured as deltas between mark calls. detail holds
// sub-phases reported with an explicit duration by code further down (server.New), which cannot use
// the delta scheme because it does not own the clock. Detail is logged on its own line and is NOT
// folded into the phase totals, so the two cannot double-count.
type startupTimer struct {
	log    *logging.Logger
	start  time.Time
	last   time.Time
	phases []phaseTime
	detail []phaseTime

	// Per-table catalog opens, reported concurrently from the goroutines that open them -- hence the
	// mutex, which the delta-based fields above do not need. They are aggregated rather than appended
	// because there is one per table and a summary listing hundreds is a summary of nothing.
	tblMu    sync.Mutex
	tblCount int
	tblSum   time.Duration
	tblSlow  []phaseTime // the slowestKept slowest, descending

	// One-time seal migrations reported during those same opens; guarded by tblMu for the same reason.
	migTables   int
	migSegments int
}

type phaseTime struct {
	name string
	d    time.Duration
}

// newStartupTimer starts the clock. The first mark measures from here.
func newStartupTimer(log *logging.Logger) *startupTimer {
	now := time.Now()
	return &startupTimer{log: log, start: now, last: now}
}

// mark records the time since the previous mark (or since construction) as phase name. A phase
// slower than slowPhase is logged immediately, so a daemon that hangs partway through startup
// still reports which phase it is in rather than dying silent.
func (t *startupTimer) mark(name string) {
	if t == nil {
		return
	}
	now := time.Now()
	d := now.Sub(t.last)
	t.last = now
	t.phases = append(t.phases, phaseTime{name, d})
	if d >= slowPhase && t.log != nil {
		t.log.Info(logging.DestinationGeneral, "startup phase slow",
			"phase", name, "took", d.Round(time.Millisecond).String())
	}
}

// record adds a sub-phase with an explicit duration, for a component that measures its own internal
// steps. Safe on a nil timer so a caller need not check.
//
// It exists because the top-level "open-database" phase turned out to cover server.New in its
// entirety -- opening the catalog, ensuring the default table, creating memory tables, building the
// RPC server, starting maintenance and the transaction reaper -- so a slow start named a phase that
// was really six. A number that attributes 4.6s to "open-database" when the catalog open is 80ms of
// it is worse than no number, because it sends the reader to the wrong code.
func (t *startupTimer) record(name string, d time.Duration) {
	if t == nil {
		return
	}
	t.detail = append(t.detail, phaseTime{name, d})
	if d >= slowPhase && t.log != nil {
		t.log.Info(logging.DestinationGeneral, "startup step slow",
			"step", name, "took", d.Round(time.Millisecond).String())
	}
}

// recordTableOpen notes one table or archive open. Safe for concurrent use and safe on a nil timer,
// because the catalog calls it from whichever goroutine did the open.
//
// An open past slowTableOpen is logged immediately with its name, which is the whole point: the summary
// only appears once the command socket is up, so a daemon that is still grinding through a catalog
// reports the table it is grinding on rather than nothing at all.
func (t *startupTimer) recordTableOpen(kind, name string, d time.Duration) {
	if t == nil {
		return
	}
	t.tblMu.Lock()
	t.tblCount++
	t.tblSum += d
	label := kind + " " + name
	// Insertion sort into a fixed-size descending top-N: cheaper and simpler than sorting hundreds of
	// entries later, and it means the slice never grows with the catalog.
	if len(t.tblSlow) < slowestKept || d > t.tblSlow[len(t.tblSlow)-1].d {
		i := sort.Search(len(t.tblSlow), func(i int) bool { return t.tblSlow[i].d < d })
		t.tblSlow = append(t.tblSlow, phaseTime{})
		copy(t.tblSlow[i+1:], t.tblSlow[i:])
		t.tblSlow[i] = phaseTime{label, d}
		if len(t.tblSlow) > slowestKept {
			t.tblSlow = t.tblSlow[:slowestKept]
		}
	}
	log := t.log
	t.tblMu.Unlock()

	if d >= slowTableOpen && log != nil {
		log.Info(logging.DestinationGeneral, "slow table open",
			"kind", kind, "name", name, "took", d.Round(time.Millisecond).String())
	}
}

// recordSealMigration notes that one table's open rewrote segments to encrypt private attributes that
// predate always-on encryption. Safe for concurrent use and on a nil timer, like recordTableOpen.
//
// Every migration is logged as it happens, with no threshold: unlike a slow open, this is a one-time
// event on the first start after the upgrade, and the number of segments is the explanation for a start
// that will not repeat. A threshold would hide the small cases and leave only the alarming ones, which is
// backwards -- the point is that the cost is expected.
func (t *startupTimer) recordSealMigration(table string, segments int) {
	if t == nil || segments <= 0 {
		return
	}
	t.tblMu.Lock()
	t.migTables++
	t.migSegments += segments
	log := t.log
	t.tblMu.Unlock()

	if log != nil {
		log.Info(logging.DestinationGeneral, "encrypting private attributes written before encryption "+
			"was always on (one-time, this start only)", "table", table, "segments", segments)
	}
}

// done logs the whole breakdown, slowest first, with the total. Called once the command socket is
// listening -- the point the old log line marked with no explanation of what preceded it.
func (t *startupTimer) done() {
	if t == nil || t.log == nil {
		return
	}
	total := time.Since(t.start)
	byTime := make([]phaseTime, len(t.phases))
	copy(byTime, t.phases)
	sort.SliceStable(byTime, func(i, j int) bool { return byTime[i].d > byTime[j].d })
	var b strings.Builder
	for i, p := range byTime {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=%s", p.name, p.d.Round(time.Millisecond))
	}
	t.log.Info(logging.DestinationGeneral, "startup timing",
		"total", total.Round(time.Millisecond).String(), "phases", b.String())
	t.logTableOpens()
	if len(t.detail) == 0 {
		return
	}
	byTime = make([]phaseTime, len(t.detail))
	copy(byTime, t.detail)
	sort.SliceStable(byTime, func(i, j int) bool { return byTime[i].d > byTime[j].d })
	var d strings.Builder
	for i, p := range byTime {
		if i > 0 {
			d.WriteString(", ")
		}
		fmt.Fprintf(&d, "%s=%s", p.name, p.d.Round(time.Millisecond))
	}
	t.log.Info(logging.DestinationGeneral, "startup detail", "steps", d.String())
}

// logTableOpens reports how many tables the catalog opened, the summed open time and the slowest few by
// name. The sum is reported as "cpu" rather than a wall-clock share because opens run in parallel: it
// can exceed the catalog-open phase it sits inside, and reading it as elapsed time would be wrong.
func (t *startupTimer) logTableOpens() {
	t.tblMu.Lock()
	count, sum := t.tblCount, t.tblSum
	migTables, migSegments := t.migTables, t.migSegments
	slow := make([]phaseTime, len(t.tblSlow))
	copy(slow, t.tblSlow)
	t.tblMu.Unlock()
	if migTables > 0 {
		// Repeated in the summary as well as per table: this is the line that explains the whole start,
		// and it must be findable next to the timing it accounts for rather than scrolled back to.
		t.log.Info(logging.DestinationGeneral, "one-time encryption of pre-existing private attributes "+
			"completed; later starts skip it", "tables", migTables, "segments", migSegments)
	}
	if count == 0 {
		return
	}
	var b strings.Builder
	for i, p := range slow {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=%s", p.name, p.d.Round(time.Millisecond))
	}
	t.log.Info(logging.DestinationGeneral, "startup catalog opens",
		"opened", count, "cpu", sum.Round(time.Millisecond).String(), "slowest", b.String())
}

// buildIdentity reports this binary's own version and the classad version it was compiled
// against, read from the embedded Go build info.
//
// It exists because "is the running daemon the build with the fix in it?" was not answerable from
// the log. A merged, tagged, released fix and a stale binary produce byte-identical output -- the
// old error message -- so the same report can mean "not fixed" or "not deployed", and there was no
// way to tell them apart without shell access to the host.
func buildIdentity() (self string, classad string) {
	self = version
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return self, "unknown"
	}
	// A binary built with `go build` from a checkout has a main version of "(devel)"; prefer the
	// linker-set version in that case, and fall back to the VCS revision so a dev build is still
	// identifiable.
	if self == "dev" || self == "" {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			self = info.Main.Version
		} else {
			for _, s := range info.Settings {
				if s.Key == "vcs.revision" && len(s.Value) >= 12 {
					self = "devel+" + s.Value[:12]
					break
				}
			}
		}
	}
	for _, dep := range info.Deps {
		if dep == nil {
			continue
		}
		// The dbrpc submodule is the one that carries the admin actions and opcodes, so it is the
		// version worth reporting when a command is rejected as unknown.
		if dep.Path == "github.com/PelicanPlatform/classad/dbrpc" {
			return self, dep.Version
		}
	}
	return self, "unknown"
}
