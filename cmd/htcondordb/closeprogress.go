package main

import (
	"sync"
	"time"

	"github.com/bbockelm/golang-htcondor/logging"
)

// closeProgressEvery is how often a close that is still running reports itself. The same cadence
// as the startup heartbeat: slow enough that a normal close (well under it) stays a single pair of
// lines, frequent enough that a 300-second termination grace does not pass in silence.
var closeProgressEvery = 15 * time.Second

// closeTimer reports what a shutdown is doing while it does it.
//
// Closing the store is the mirror of opening it and had the same defect, with worse consequences.
// It logged "closing store", then nothing until "store closed" -- and the long step inside it,
// draining the RPC server, waits for every in-flight maintenance pass to reach a safe boundary
// because the catalog close that follows unmaps the segments those passes are reading. On an
// archive at history scale that wait was measured at 189 seconds. For all of it the process sat
// after its own log said the daemon had stopped, which is indistinguishable from a hang, and a
// shutdown killed during the gap left a log showing a clean exit that never happened.
//
// It is also what makes a termination grace period knowable. Sized without these numbers it has to
// be inferred from the OPEN timings, which measure different work on the other side of the mmap.
type closeTimer struct {
	log   *logging.Logger
	start time.Time

	mu      sync.Mutex
	phase   string // the step running now, "" before the first begins
	phaseAt time.Time
	done    []closedPhase

	stop, stopped chan struct{}
}

type closedPhase struct {
	name string
	d    time.Duration
}

// newCloseTimer starts the heartbeat. Call finish when the close returns.
func newCloseTimer(log *logging.Logger) *closeTimer {
	t := &closeTimer{
		log: log, start: time.Now(),
		stop: make(chan struct{}), stopped: make(chan struct{}),
	}
	go t.beat()
	return t
}

// begin records the step now running, for the heartbeat to name.
func (t *closeTimer) begin(name string) {
	t.mu.Lock()
	t.phase, t.phaseAt = name, time.Now()
	t.mu.Unlock()
}

// record notes a completed step.
func (t *closeTimer) record(name string, d time.Duration) {
	t.mu.Lock()
	t.done = append(t.done, closedPhase{name, d})
	t.phase = "" // between steps until the next begin
	t.mu.Unlock()
}

func (t *closeTimer) beat() {
	defer close(t.stopped)
	tick := time.NewTicker(closeProgressEvery)
	defer tick.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-tick.C:
			t.mu.Lock()
			phase, phaseAt, done := t.phase, t.phaseAt, append([]closedPhase(nil), t.done...)
			t.mu.Unlock()
			fields := []any{"elapsed", time.Since(t.start).Round(time.Second).String()}
			if phase != "" {
				fields = append(fields, "phase", phase,
					"phase_elapsed", time.Since(phaseAt).Round(time.Second).String())
			}
			if len(done) > 0 {
				fields = append(fields, "finished", phaseList(done))
			}
			// Deliberately not a warning. A long close is the daemon doing the right thing --
			// refusing to unmap segments a maintenance pass is still reading -- and warning on it
			// would train an operator to ignore the one line that says a shutdown is stuck.
			t.log.Info(logging.DestinationGeneral, "still closing the store", fields...)
		}
	}
}

// finish stops the heartbeat and returns the per-phase summary for the final line.
func (t *closeTimer) finish() []any {
	close(t.stop)
	<-t.stopped
	t.mu.Lock()
	defer t.mu.Unlock()
	out := []any{"seconds", time.Since(t.start).Round(time.Millisecond).Seconds()}
	for _, p := range t.done {
		out = append(out, p.name, p.d.Round(time.Millisecond).String())
	}
	return out
}

// phaseList renders completed steps as "name=1.2s, name=3.4s".
func phaseList(done []closedPhase) string {
	s := ""
	for i, p := range done {
		if i > 0 {
			s += ", "
		}
		s += p.name + "=" + p.d.Round(time.Millisecond).String()
	}
	return s
}
