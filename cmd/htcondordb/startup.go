package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/collections/vm"
	"github.com/bbockelm/golang-htcondor/logging"
)

// Startup phase timing.
//
// Startup logs a line when the daemon begins and another when it is ready to serve, with
// everything in between silent. A slow start therefore shows only as a gap between two
// timestamps -- enough to notice ("suspiciously close to exactly 10s") and not enough to act on,
// because the gap covers reading security config, building the authorization policy, opening the
// database, adopting the listener, and starting the managers that mirror, export and replicate,
// any of which could own it.
//
// The span runs to the point the daemon advertises itself, not to the bind: the managers start
// after the listener and are where a startup stalls on something outside this process -- a
// schedd's job_queue.log, a broker, a raft quorum. A timer that stopped at the bind would report
// a fast startup for a daemon that then took a minute to become reachable.
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

// How often the catalog-open heartbeat reports. Short enough that a human watching a start does not
// conclude it has hung, long enough that it is a handful of lines rather than a log of its own.
var progressEvery = 15 * time.Second

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

	// Progress heartbeat state, guarded by tblMu because it is written from the same concurrent
	// open callbacks and read by the ticker goroutine.
	lastDone   string
	lastDoneAt time.Time
	progStop   chan struct{}
	progDone   chan struct{}
	// dir is the catalog root. It is what turns a progress callback's shard directory back into
	// the table or archive that shard belongs to; the storage layer reports the directory it is
	// working in and knows nothing of the catalog's naming.
	dir string
	// entries is every table and archive the open has to get through, with its segment count;
	// doneNames is the ones that have finished. The difference is what a beat reports as still
	// open -- the question "what is it doing" that a count of finished tables cannot answer.
	entries   []catalogEntry
	doneNames map[string]bool
	// prog is how far into its own open each entry has got, keyed by label, as reported from
	// inside the open. It is what makes "how many segments remain" answerable for an entry that
	// has not finished -- the count a beat could not previously reach.
	prog map[string]*entryProgress
	// rates is the exponential moving average of observed throughput, in segments per second,
	// kept PER PHASE.
	//
	// One average across all phases would be an average of unlike things: mapping a segment is
	// an mmap, replaying one is a walk of every record it holds, and loading its sidecar is
	// adopting an index or rebuilding it. A combined figure would be whichever phase happened
	// to be running weighted by how long it ran, and would lurch every time an entry crossed a
	// phase boundary. Per phase, each rate divides into the work denominated in its own pass.
	rates map[string]*phaseRate
	// stopped is set when the heartbeat stops. The storage layer's progress hook is a process
	// global set once at start, so it stays installed and a maintenance pass that reopens a
	// collection hours later still calls it; those reports are dropped rather than folded into a
	// finished start's numbers.
	stopped bool
}

// entryProgress is how far into its open one table or archive has got.
//
// shards is the last report from each shard directory, because an entry's reports arrive per
// shard and the beat wants the entry. done and total cover only the shards currently in phase: a
// shard whose load finishes inside one reporting interval never reports at all, so on a
// many-shard table these are a floor rather than a census. On an archive -- the shape that
// actually takes minutes, and the one this exists for -- there is exactly one shard and they are
// exact.
type entryProgress struct {
	phase  string
	done   int
	total  int
	shards map[string]shardProgress
	// seen is every phase this entry has reported, so the passes it still has ahead of it can
	// be counted without assuming a phase that may not run (see openPhaseOrder).
	seen map[string]bool
}

// phaseRate is one phase's throughput EMA. obs == 0 means nothing on this machine has measured
// this phase yet and rate still holds the seeded prior, a distinction the beat prints rather
// than papers over.
type phaseRate struct {
	rate float64
	obs  int
}

// openPhaseOrder is the order a persistent open runs its phases in, which is what lets a beat
// say how much work an entry has LEFT rather than only how far it is into the pass in flight.
// Each phase is one pass over the entry's segments, so an entry partway through the first of
// three still has the best part of three passes to go.
//
// rebuild_dir is conditional: a clean previous close leaves a directory snapshot and the replay
// is skipped entirely. It is therefore counted only once an entry has actually reported it,
// which makes a beat's remaining count a lower bound on the unclean path -- the number rises by
// one pass when the replay starts. Assuming it instead would overstate every healthy open,
// which is the common case and the one an operator reads the beat during.
var openPhaseOrder = []string{
	string(collections.OpenMapSegments),
	string(collections.OpenRebuildDir),
	string(collections.OpenLoadSidecars),
}

// shardProgress is one shard's last progress report, kept so the next report from the same shard
// can be differentiated against it: a rate is a delta over a delta, and each shard's stream is its
// own clean series.
//
// elapsed is the phase's own clock as the storage layer measured it, not this process's wall
// clock. Both the count and the clock are then read at the same instant inside the open, so a
// callback that waits on tblMu behind another goroutine cannot show up as a slower rate.
type shardProgress struct {
	phase   string
	done    int
	total   int
	elapsed time.Duration
}

// outstandingEntry is one entry still opening: what the directory scan found, plus whatever the
// open itself has since said about it.
type outstandingEntry struct {
	catalogEntry
	// remaining is segment-PASSES still to get through: the remainder of the pass in flight,
	// plus one whole pass for each phase still ahead of it (see openPhaseOrder). Before the
	// open reports anything it is one pass, the entry's own segment count.
	//
	// Counting only the pass in flight would report an entry as nearly finished at the moment
	// its next pass is about to start; counting only one chosen pass, as this once did, leaves
	// the number frozen for the whole of every other one. A pass is the unit because every
	// phase walks the same segments, so they are commensurable -- in count. They are not
	// commensurable in cost, which is why the estimate divides each pass by its own rate
	// rather than dividing this total by one.
	remaining int
	// est is how long those passes should take, each divided by its own phase's rate. It is
	// what the entries are ordered by, because the heartbeat reports the one that will finish
	// last and segment count does not decide that: a short pass in a slow phase outlasts a long
	// one in a fast phase, which is the whole reason the rates are kept per phase.
	est time.Duration
	// detail is the in-flight phase and its counts, empty until the open reports.
	detail string
}

// outstanding is the entries not yet opened, longest estimate first, with the total segment-passes
// remaining across all of them. Caller holds tblMu.
func (t *startupTimer) outstanding() (open []outstandingEntry, remaining int) {
	for _, e := range t.entries {
		if t.doneNames[e.label()] {
			continue
		}
		o := outstandingEntry{catalogEntry: e, remaining: e.segs}
		if p := t.prog[e.label()]; p != nil && p.total > 0 {
			o.detail = fmt.Sprintf("%s %d/%d", p.phase, p.done, p.total)
			o.remaining = p.total - p.done
			for _, later := range phasesAfter(p.phase) {
				if later == string(collections.OpenRebuildDir) && !p.seen[later] {
					continue // conditional, and not observed: see openPhaseOrder
				}
				o.remaining += p.total
			}
		}
		o.est = t.estimateEntry(o)
		open = append(open, o)
		remaining += o.remaining
	}
	sort.Slice(open, func(i, j int) bool {
		if open[i].est != open[j].est {
			return open[i].est > open[j].est
		}
		return open[i].remaining > open[j].remaining
	})
	return open, remaining
}

// entryLabel maps a shard directory reported by the storage layer back to the catalog entry it
// belongs to, matching catalogEntry.label. The catalog lays entries out as <dir>/tables/<name>
// and <dir>/archives/<name>, and each shard is a numbered subdirectory of one of those. A path
// that does not fit that shape belongs to something this heartbeat does not track (an in-memory
// table, a snapshot being restored), and is ignored. Caller holds tblMu, which is what makes
// reading t.dir safe against the goroutine that set it.
func (t *startupTimer) entryLabel(shardDir string) string {
	if t.dir == "" {
		return ""
	}
	rel, err := filepath.Rel(t.dir, shardDir)
	if err != nil {
		return ""
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) < 2 {
		return ""
	}
	switch parts[0] {
	case "tables":
		return "table " + parts[1]
	case "archives":
		return "archive " + parts[1]
	}
	return ""
}

// onOpenProgress receives the storage layer's in-flight report for one shard of one entry.
//
// This is the only thing in the process that can see inside a single table's open. Everything
// else -- the completion callback, the per-table timing lines -- fires when a table FINISHES, so
// the entry that is taking eleven minutes is precisely the one that says nothing for eleven
// minutes. The report carries the phase and a done/total over that phase's segments, which is the
// "how many segments remain" an operator reads the beat for, and it is also the only measurement
// of this machine's throughput available while that throughput still matters.
//
// Called concurrently from every goroutine opening a table, so everything it touches is under
// tblMu, and it must not block: it runs on the open path.
func (t *startupTimer) onOpenProgress(p collections.OpenProgress) {
	if t == nil {
		return
	}
	t.tblMu.Lock()
	defer t.tblMu.Unlock()
	if t.stopped {
		return
	}
	label := t.entryLabel(p.Dir)
	if label == "" {
		return
	}
	if t.prog == nil {
		t.prog = map[string]*entryProgress{}
	}
	e := t.prog[label]
	if e == nil {
		e = &entryProgress{shards: map[string]shardProgress{}, seen: map[string]bool{}}
		t.prog[label] = e
	}
	phase := string(p.Phase)
	e.seen[phase] = true
	prev, seen := e.shards[p.Dir]
	e.shards[p.Dir] = shardProgress{phase: phase, done: p.Done, total: p.Total, elapsed: p.Elapsed}
	e.phase = phase
	e.done, e.total = 0, 0
	for _, s := range e.shards {
		if s.phase == phase {
			e.done += s.done
			e.total += s.total
		}
	}

	// Every phase feeds a rate, into its OWN average. Which phase dominates depends on the
	// store: a segment whose sidecar cannot be adopted has its index rebuilt by decoding every
	// record, and then the sidecar pass is nearly all of the open; when sidecars adopt -- the
	// healthy case -- it is cheap and the mmap pass and the directory replay are the cost. A
	// rate collected from one chosen phase goes stale for the whole of every other one, which
	// leaves the estimate it divides into frozen exactly when an operator is watching it.
	if seen && prev.phase == phase {
		t.observeRate(phase, float64(p.Done-prev.done), p.Elapsed-prev.elapsed)
		return
	}
	// First report of this shard's pass. Done and Elapsed are both measured from the phase's
	// own start, so it is already a complete sample and there is no need to wait for a second
	// report to have a measured rate.
	t.observeRate(phase, float64(p.Done), p.Elapsed)
}

// observeRate folds one throughput observation into the EMA. Caller holds tblMu.
//
// A sample carrying no segments or no time is dropped rather than averaged in. Two reports with
// the same count do not say the open is running at zero segments a second; they say nothing
// landed in that window, and treating that as a measurement would drive the estimate to infinity
// on the one shape -- a genuine stall -- where the operator most needs the other fields to still
// make sense. A stall shows as since_last_beat and an unmoving done/total instead.
//
// The first measured sample REPLACES the seeded prior instead of being blended with it: the prior
// came off a different machine with different storage, and there is no reason to let it drag an
// actual measurement of this one for the next several beats.
func (t *startupTimer) observeRate(phase string, segs float64, d time.Duration) {
	if segs <= 0 || d <= 0 {
		return
	}
	if t.rates == nil {
		t.rates = map[string]*phaseRate{}
	}
	r := t.rates[phase]
	if r == nil {
		r = &phaseRate{}
		t.rates[phase] = r
	}
	sample := segs / d.Seconds()
	if r.obs == 0 {
		r.rate = sample
	} else {
		r.rate = rateEMAAlpha*sample + (1-rateEMAAlpha)*r.rate
	}
	r.obs++
}

// wholeEntryPhase buckets the coarse per-entry samples recordTableOpen takes (see there). They
// span every phase of an entry's open at once, so they belong to none of them and would skew any
// real phase's average. They are the fallback instead: better than a prior measured on somebody
// else's hardware, worse than a measurement of the phase actually being estimated.
const wholeEntryPhase = "entry"

// rateFor is the rate the estimate should use for one phase and whether anything measured it.
// Caller holds tblMu. A phase nothing has measured yet falls back to the whole-entry samples,
// then to the seeded prior.
func (t *startupTimer) rateFor(phase string) (float64, bool) {
	if r := t.rates[phase]; r != nil && r.obs > 0 {
		return r.rate, true
	}
	if r := t.rates[wholeEntryPhase]; r != nil && r.obs > 0 {
		return r.rate, true
	}
	return seedSegmentsPerSecond, false
}

// rateNow is the rate to PRINT: the phase of the entry the estimate is about, which is the one a
// reader is watching. It returns that phase too, because the rate is one phase's and the printed
// segments_remaining spans several -- dividing the one by the other is off by whatever the phases
// differ by, and naming the phase is what stops a reader doing it. Caller holds tblMu.
func (t *startupTimer) rateNow(open []outstandingEntry) (rate float64, phase string, measured bool) {
	if len(open) > 0 {
		if p := t.prog[open[0].label()]; p != nil && p.phase != "" {
			rate, measured = t.rateFor(p.phase)
			return rate, p.phase, measured
		}
	}
	return seedSegmentsPerSecond, "", false
}

// estimateEntry is how long the passes an entry has left should take, each divided by its own
// phase's rate. Caller holds tblMu.
//
// Summing passes and dividing once by a single rate is what the per-phase averages exist to
// avoid: the passes cost different amounts per segment, so one divisor would be wrong for all
// but one of them.
func (t *startupTimer) estimateEntry(o outstandingEntry) time.Duration {
	p := t.prog[o.label()]
	if p == nil || p.total <= 0 {
		rate, _ := t.rateFor(string(collections.OpenMapSegments))
		return estimateOpen(o.remaining, rate)
	}
	rate, _ := t.rateFor(p.phase)
	total := estimateOpen(p.total-p.done, rate)
	for _, later := range phasesAfter(p.phase) {
		if later == string(collections.OpenRebuildDir) && !p.seen[later] {
			continue // conditional, and not observed: see openPhaseOrder
		}
		lrate, _ := t.rateFor(later)
		total += estimateOpen(p.total, lrate)
	}
	return total
}

// phasesAfter is the phases that run after the named one, in order. An unrecognised phase has
// nothing known to follow it, so it contributes only its own remainder -- a new phase added to
// the storage layer makes the beat conservative, not wrong.
func phasesAfter(phase string) []string {
	for i, p := range openPhaseOrder {
		if p == phase {
			return openPhaseOrder[i+1:]
		}
	}
	return nil
}

// segsFor is the scanned segment count of one entry, or 0 if the scan did not see it. Caller
// holds tblMu.
func (t *startupTimer) segsFor(label string) int {
	for _, e := range t.entries {
		if e.label() == label {
			return e.segs
		}
	}
	return 0
}

// rateSource labels the rate for the log, because an estimate built on a constant and an estimate
// built on this machine's own throughput are worth different amounts and the reader cannot tell
// them apart from the number.
func rateSource(measured bool) string {
	if measured {
		return "measured"
	}
	return "seeded"
}

// catalogEntry is one table or archive the open has to get through, and how much of it there is.
// segs is its sealed segment files: the unit the open's cost scales with, and the only measure of
// remaining work available before anything is opened.
type catalogEntry struct {
	kind string // "table" or "archive", matching recordTableOpen
	name string
	segs int
}

func (e catalogEntry) label() string { return e.kind + " " + e.name }

// seedSegmentsPerSecond is the rate the estimate uses before this start has measured one of its
// own: a SEEDED PRIOR, not a measurement of the machine it is running on. It is the CHTC hub's
// shape -- history 5,278 segments in 7m49s and epoch_history 7,007 in 11m40s, the two opening
// concurrently, so ~10 segments a second per entry -- and every machine, storage stack and page
// cache differs. It exists only to answer the first beat, before any observation has arrived, and
// the beat says "seeded" while it is in use. The moment the open reports progress the measured
// rate replaces it (see observeRate).
const seedSegmentsPerSecond = 10.0

// rateEMAAlpha weights the newest observation in the throughput EMA.
//
// Observations arrive a few seconds apart per shard, so 0.3 settles on a changed rate within
// roughly half a minute while still absorbing the sample-to-sample noise of a page cache warming
// up or another entry starting alongside. Higher and the estimate jumps around with every
// interval; lower and a machine slower than the prior spends the first minutes of its open being
// told the wrong thing confidently.
const rateEMAAlpha = 0.3

// minRateSampleSegments is the smallest completed entry worth taking a rate from. An open has
// fixed costs -- dictionaries, directory restore, the snapshot check -- that a handful of
// segments cannot amortize, so a two-segment table's implied rate measures those costs and not
// throughput. Entries below this contribute nothing to the EMA.
const minRateSampleSegments = 16

// scanCatalogEntries lists what the catalog directory holds and how many sealed segments each one
// carries, so the heartbeat can name what is still outstanding and size it rather than print a
// bare count. Best effort on purpose: a catalog that does not exist yet is a new database with
// nothing to open, and a directory that cannot be read is not worth failing a startup over -- the
// heartbeat just omits what it could not learn.
func scanCatalogEntries(dir string) []catalogEntry {
	var out []catalogEntry
	for _, sub := range []struct{ dir, kind string }{{"tables", "table"}, {"archives", "archive"}} {
		ents, err := os.ReadDir(filepath.Join(dir, sub.dir))
		if err != nil {
			continue
		}
		for _, e := range ents {
			if !e.IsDir() {
				continue
			}
			out = append(out, catalogEntry{kind: sub.kind, name: e.Name(),
				segs: countSegmentFiles(filepath.Join(dir, sub.dir, e.Name()))})
		}
	}
	return out
}

// countSegmentFiles counts the sealed segment files under a table's directory, shard
// subdirectories included. Best effort: an unreadable directory contributes nothing.
func countSegmentFiles(dir string) int {
	n := 0
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable subtree: skip it, do not fail the walk
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".dat") {
			n++
		}
		return nil
	})
	return n
}

// startProgress logs what the catalog open is doing every progressEvery until stopProgress is
// called.
//
// Opening a catalog is the longest thing this daemon does and the quietest: every
// timing line -- "slow table open", "table open phase timing" -- is emitted when a table *finishes*,
// so the slowest opens are exactly the ones that report nothing while they are slow. A 53GB catalog
// recomputing zone maps went silent for over two minutes, which is indistinguishable from a wedged
// process; an orchestrator with a liveness probe draws the obvious conclusion and kills it, and the
// restart begins the same silent open again.
//
// Each beat reports what finished since the last one AND what is still open, by name and by how
// many segments it still has to get through. "Nothing finished since the last report" on its own
// reads as a stall even when it is the normal shape of a catalog whose tail is one large archive:
// the archives open concurrently, so once the small tables are done the only thing left to finish
// is the slowest of them, and a counter of completions necessarily sits still until it does.
// Naming the outstanding entry and counting down the segments inside it is what tells those
// apart -- a count that only the storage layer's in-flight reports can supply, since nothing else
// in the process hears from a table between its first segment and its last (see onOpenProgress).
func (t *startupTimer) startProgress(dir string) {
	if t == nil || t.log == nil {
		return
	}
	entries := scanCatalogEntries(dir)
	t.tblMu.Lock()
	t.dir = dir
	t.entries = entries
	if t.doneNames == nil {
		t.doneNames = map[string]bool{}
	}
	// Nothing is open yet, so the banner's estimate is the first pass at the first pass's rate.
	rate, measured := t.rateFor(string(collections.OpenMapSegments))
	t.tblMu.Unlock()
	if len(entries) > 0 {
		segs, biggest := 0, entries[0]
		for _, e := range entries {
			segs += e.segs
		}
		for _, e := range entries {
			if e.segs > biggest.segs {
				biggest = e
			}
		}
		// The archives open concurrently, so the wall clock is the slowest one, not the sum.
		t.log.Info(logging.DestinationGeneral, "opening the catalog",
			"entries", len(entries), "segments", segs, "largest", biggest.label(),
			"largest_segments", biggest.segs, "estimate", estimateOpen(biggest.segs, rate).String(),
			"rate", formatRate(rate), "rate_source", rateSource(measured),
			"note", "estimate is the largest entry at the current rate, which is refined from "+
				"this machine's own progress as the open runs; entries open concurrently")
	}
	t.progStop, t.progDone = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(t.progDone)
		tick := time.NewTicker(progressEvery)
		defer tick.Stop()
		lastCount := 0
		for {
			select {
			case <-t.progStop:
				return
			case <-tick.C:
				t.tblMu.Lock()
				done, last, lastAt := t.tblCount, t.lastDone, t.lastDoneAt
				open, remaining := t.outstanding()
				rate, ratePhase, measured := t.rateNow(open)
				var est time.Duration
				var estOf string
				if len(open) > 0 {
					est, estOf = open[0].est, estimateScope(open[0])
				}
				total := len(t.entries)
				t.tblMu.Unlock()

				fields := []any{"elapsed", time.Since(t.start).Round(time.Second).String(),
					"opened", done, "since_last_beat", done - lastCount}
				if total > 0 {
					fields = append(fields, "of", total)
				}
				if len(open) > 0 {
					// segments_remaining counts work left, not work owned: the outstanding
					// entries' total was the number that made "still opening" unactionable,
					// because it never moved until an entry finished outright.
					fields = append(fields, "open", openLabels(open),
						"segments_remaining", remaining,
						"estimate_remaining", est.String(),
						"estimate_of", estOf,
						"rate", formatRatePhase(rate, ratePhase), "rate_source", rateSource(measured))
				}
				if last != "" {
					// Time since a table last FINISHED -- not time spent doing nothing. Named for
					// what it measures: on a catalog whose tail is one large archive it rises for
					// the whole of that archive's open, which is the normal case, not a stall.
					fields = append(fields, "last", last,
						"since_last_table", time.Since(lastAt).Round(time.Second).String())
				}
				if done == lastCount {
					// Not an error: a single large archive legitimately takes minutes. The open
					// list above says which one and how much of it is left.
					t.log.Info(logging.DestinationGeneral,
						"still opening the catalog; nothing finished since the last report", fields...)
				} else {
					t.log.Info(logging.DestinationGeneral, "opening the catalog", fields...)
				}
				lastCount = done
			}
		}
	}()
}

// estimateOpen is how long rate says that many segments take, rounded to the second. An
// order-of-magnitude aid either way, but one built on a rate this process has watched rather than
// on a constant copied from somebody else's hardware -- see seedSegmentsPerSecond for the figure
// it starts from and observeRate for what replaces it.
func estimateOpen(segs int, rate float64) time.Duration {
	if segs <= 0 || rate <= 0 {
		return 0
	}
	return time.Duration(float64(segs) / rate * float64(time.Second)).Round(time.Second)
}

// formatRate renders the throughput for a log field. One decimal: a rate is a smoothed estimate
// and printing it to nine significant figures would suggest otherwise.
func formatRate(rate float64) string {
	return fmt.Sprintf("%.1f segments/s", rate)
}

// formatRatePhase is formatRate with the phase the rate belongs to. The heartbeat prints one
// phase's rate beside a segments_remaining that spans every phase still to come, so the obvious
// division -- segments over rate -- does not give the printed estimate and is not meant to. Naming
// the phase is what makes the rate legible as one phase's and not the catalog's.
func formatRatePhase(rate float64, phase string) string {
	if phase == "" {
		return formatRate(rate)
	}
	return fmt.Sprintf("%s (%s)", formatRate(rate), phase)
}

// estimateScope names what estimate_remaining is about: the entry expected to finish last, and the
// segment-passes it has left. The estimate is one entry's because entries open concurrently, so
// the wall clock is the slowest of them rather than the sum -- without this field the estimate sits
// beside a total it is not derived from and reads like an arithmetic error.
func estimateScope(o outstandingEntry) string {
	return fmt.Sprintf("%s, %d segments", o.label(), o.remaining)
}

// openLabels renders the outstanding entries for a log field, longest estimate first, capped
// so one line stays readable on a catalog with hundreds of tables. An entry the open has reported
// on is rendered with its phase and its counts, which is what turns "this one is big" into "this
// one is three quarters done"; one it has not yet reached shows the size the directory scan found.
func openLabels(open []outstandingEntry) string {
	const maxShown = 4
	parts := make([]string, 0, maxShown+1)
	for i, e := range open {
		if i == maxShown {
			parts = append(parts, fmt.Sprintf("+%d more", len(open)-maxShown))
			break
		}
		if e.detail != "" {
			parts = append(parts, fmt.Sprintf("%s(%s)", e.label(), e.detail))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s(%d segs)", e.label(), e.segs))
	}
	return strings.Join(parts, ", ")
}

// statusLine is the heartbeat's state as one line, for the observability listener's /startupz --
// the same facts the log beat carries, available to anything that can make an HTTP request while
// the open is still running.
func (t *startupTimer) statusLine() string {
	if t == nil {
		return "opening the catalog"
	}
	t.tblMu.Lock()
	done, last, lastAt := t.tblCount, t.lastDone, t.lastDoneAt
	open, remaining := t.outstanding()
	rate, ratePhase, measured := t.rateNow(open)
	var est time.Duration
	var estOf string
	if len(open) > 0 {
		est, estOf = open[0].est, estimateScope(open[0])
	}
	total := len(t.entries)
	t.tblMu.Unlock()

	b := fmt.Sprintf("opening the catalog: elapsed=%s opened=%d", time.Since(t.start).Round(time.Second), done)
	if total > 0 {
		b += fmt.Sprintf(" of=%d", total)
	}
	if len(open) > 0 {
		b += fmt.Sprintf(" open=%q segments_remaining=%d estimate_remaining=%s estimate_of=%q rate=%q rate_source=%s",
			openLabels(open), remaining, est, estOf, formatRatePhase(rate, ratePhase), rateSource(measured))
	}
	if last != "" {
		b += fmt.Sprintf(" last=%q since_last_table=%s", last, time.Since(lastAt).Round(time.Second))
	}
	return b
}

// stopProgress ends the heartbeat and waits for it, so no beat can be emitted after the daemon has
// reported itself up. Safe to call on a timer whose progress was never started.
func (t *startupTimer) stopProgress() {
	if t == nil {
		return
	}
	// Mark it stopped before anything else: the storage layer's progress hook is a process global
	// that stays installed for the life of the daemon, and a report from a maintenance reopen must
	// not land in a start that is already over.
	t.tblMu.Lock()
	t.stopped = true
	t.tblMu.Unlock()
	if t.progStop == nil {
		return
	}
	close(t.progStop)
	<-t.progDone
	t.progStop, t.progDone = nil, nil
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
	t.lastDone = label
	t.lastDoneAt = time.Now()
	if t.doneNames == nil {
		t.doneNames = map[string]bool{}
	}
	t.doneNames[label] = true
	// A finished entry is a coarse rate sample: its segments over the wall clock its open took.
	// Coarse twice over -- that clock covers the whole open, dictionaries and directory restore
	// included, not only the segment work the estimate is denominated in, and it arrives when the
	// entry is already done, which is when knowing its rate has stopped helping it. It is folded
	// in anyway for the catalog shape the in-flight reports cannot see: many moderate tables,
	// none of them open for the several seconds it takes the storage layer to report once, where
	// the EMA would otherwise never leave the seeded prior no matter how long the start ran.
	if segs := t.segsFor(label); segs >= minRateSampleSegments {
		t.observeRate(wholeEntryPhase, float64(segs), d)
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

// simdEngineStatus reports whether the columnar SIMD engine is live in THIS process, for the
// -version output and the startup line.
//
// Both halves of "live" matter and neither is visible from outside. The engine compiles only
// under GOEXPERIMENT=simd; built without it, the compiler deletes every raw-column branch of the
// vector executor. On amd64 it then also has to pass a runtime CPU check, so a simd build on a
// host without the vector extension runs the scalar path anyway. Either way every answer stays
// correct and only the speed changes -- which is exactly why losing the engine is invisible
// unless the daemon says so.
//
// vm.RawColumnUseful is the executor's own gate, so asking it answers both halves at once. The
// 4-byte unsigned column is just a width every engine supports; it is a probe, not a preference.
func simdEngineStatus() string {
	if vm.RawColumnUseful(4, true) {
		return "active"
	}
	return "not active"
}
