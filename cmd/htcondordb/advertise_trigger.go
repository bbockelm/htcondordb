package main

import (
	"context"
	"time"

	"github.com/bbockelm/htcondordb/dbad"
	"github.com/bbockelm/htcondordb/scheddsync"
)

const (
	// caughtUpSampleInterval is how often the trigger goroutine samples the sync sources for a
	// CaughtUp transition. Short so a change reaches the collector within seconds; cheap (it reads
	// in-memory status snapshots).
	caughtUpSampleInterval = 2 * time.Second
	// caughtUpMinGap is the minimum wall-clock between trigger-driven advertises, so a source that
	// oscillates CaughtUp cannot flap the collector. A transition seen within the gap is remembered
	// and fires once the gap elapses.
	caughtUpMinGap = 15 * time.Second
)

// caughtUpTrigger decides when a sync source's CaughtUp change warrants an early collector
// advertise, debounced to at most once per minGap.
type caughtUpTrigger struct {
	last     map[string]bool // source key -> last observed CaughtUp
	pending  bool            // a transition has been seen but not yet fired (waiting out minGap)
	lastFire time.Time
	minGap   time.Duration
}

func newCaughtUpTrigger(minGap time.Duration) *caughtUpTrigger {
	return &caughtUpTrigger{last: map[string]bool{}, minGap: minGap}
}

// observe records the current source states and reports whether an early advertise should fire now:
// true when some source's CaughtUp flipped since the last observation AND at least minGap has passed
// since the last fire. A flip during the gap is remembered (pending) and fires once the gap elapses,
// so no transition is missed and none flaps. A source's FIRST observation is not a transition.
func (c *caughtUpTrigger) observe(statuses []scheddsync.SyncStatus, now time.Time) bool {
	for _, s := range statuses {
		if s.Kind == "" {
			continue // not yet reporting
		}
		k := s.Kind + "\x00" + s.Source
		if prev, seen := c.last[k]; seen && prev != s.CaughtUp {
			c.pending = true
		}
		c.last[k] = s.CaughtUp
	}
	if c.pending && (c.lastFire.IsZero() || now.Sub(c.lastFire) >= c.minGap) {
		c.pending = false
		c.lastFire = now
		return true
	}
	return false
}

// runCaughtUpTrigger samples the sync sources and sends on trigger when an early advertise is
// warranted (see caughtUpTrigger). Non-blocking sends into the buffered-1 channel coalesce, so a
// second signal while one is already pending is dropped rather than queued.
func runCaughtUpTrigger(ctx context.Context, sourcesFunc func() []dbad.StatusSource, trigger chan<- struct{}) {
	c := newCaughtUpTrigger(caughtUpMinGap)
	t := time.NewTicker(caughtUpSampleInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			if c.observe(dbad.LiveStatuses(sourcesFunc), now) {
				select {
				case trigger <- struct{}{}:
				default:
				}
			}
		}
	}
}
