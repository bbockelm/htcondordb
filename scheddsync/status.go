package scheddsync

import (
	"sync/atomic"
	"time"
)

// SyncStatus is a point-in-time snapshot of one syncer's progress. It is published atomically
// from within the sync goroutine after each read pass and read lock-free from another goroutine
// (the collector-advertisement loop), so it never races the syncer's own state. It is the raw
// material for a per-source "sync health" record: how far behind the source the mirror is, and
// whether a durability gap has occurred.
type SyncStatus struct {
	Kind       string    // "job_queue.log" | "history"
	Source     string    // the source file path being mirrored
	Offset     int64     // bytes of the current source file consumed
	FileSize   int64     // current size of the current source file
	LagBytes   int64     // unconsumed tail of the current file (FileSize-Offset, clamped >= 0)
	CaughtUp   bool      // the last read pass reached end of file
	LastSync   time.Time // wall-clock of the last read pass that verified the mirror against the source
	Resyncs    int64     // cumulative durability-gap (resync) events seen (history only)
	LastResync time.Time // time of the most recent resync event, zero if none

	// Diagnostics for the "partial-ad / orphan" investigation (jobs present but missing JobStatus).
	// All are cumulative since process start, observe-only (they do not change behavior). A
	// constantly-climbing SetAttrAbsentKey on a busy schedd -- where reconcile is rare -- is the
	// signal that updates are landing on keys the store cannot resolve (which fabricate an
	// identity-less orphan via db.Txn.SetAttribute's create-on-absent). Reconciles confirms how
	// often the full-reload path actually fires (expected: ~once per compaction, i.e. rare).
	SetAttrAbsentKey int64 // Set/DeleteAttribute ops whose target key was absent when applied
	Reconciles       int64 // reconcileReload runs (full-replay-and-sweep)
	// ReconcileLookupMiss counts rows a reconcile refused to rewrite because the table held the key
	// but the lookup missed -- each one a full ad saved from being replaced by one log run's
	// attributes. Nonzero is a storage-side key-resolution fault, not a sync one.
	ReconcileLookupMiss int64
	// Unapplied counts writes the store refused to compose at all (db.UnappliedError): the update
	// did not land and is not retried, because retrying cannot succeed.
	Unapplied int64

	// Cumulative wall-clock time the tailer has spent, to localize WHERE a behind tailer's time
	// goes (surfaced as *_seconds_total counters). CommitSeconds is incremental commits only;
	// ReconcileSeconds is full reloads; PollSeconds is every poll (read+apply+commit+probe), so
	// read/apply time is PollSeconds - CommitSeconds - ReconcileSeconds. Job source only (history
	// appends self-persist and are not timed here); zero on the history source.
	CommitSeconds    float64
	PollSeconds      float64
	ReconcileSeconds float64
}

// Status exposes the latest published snapshot (zero value before the first read pass). Both
// are safe to call concurrently with the running syncer.
func (s *JobSync) Status() SyncStatus     { return loadStatus(&s.status) }
func (s *HistorySync) Status() SyncStatus { return loadStatus(&s.status) }

func loadStatus(p *atomic.Pointer[SyncStatus]) SyncStatus {
	if st := p.Load(); st != nil {
		return *st
	}
	return SyncStatus{}
}

// lagAndFile computes the current file size and unconsumed-tail lag for a source at the given
// consumed offset. A stat error leaves both zero (source not yet present).
func lagAndFile(path string, offset int64) (size, lag int64) {
	if id, err := statIdentity(path); err == nil {
		size = id.Size
		if size > offset {
			lag = size - offset
		}
	}
	return size, lag
}

// publishStatus atomically stores a fresh snapshot. progressed marks a read pass that verified the
// mirror against its source -- one that applied records, or one that confirmed there were none to
// apply -- refreshing LastSync. It preserves the accumulated LastSync/Resyncs/LastResync across
// snapshots. Called only from the sync goroutine.
func (s *JobSync) publishStatus(progressed bool) {
	now := nowFn()
	off := s.parser.GetNextOffset()
	src := s.parser.GetFilename()
	size, lag := lagAndFile(src, off)
	st := SyncStatus{Kind: "job_queue.log", Source: src, Offset: off, FileSize: size, LagBytes: lag, CaughtUp: lag == 0}
	st.SetAttrAbsentKey = s.mAbsentKey.Load()
	st.ReconcileLookupMiss = s.mReconcileLookupMiss.Load()
	st.Unapplied = s.mUnapplied.Load()
	st.Reconciles = s.mReconciles.Load()
	st.CommitSeconds = float64(s.mCommitNanos.Load()) / 1e9
	st.PollSeconds = float64(s.mPollNanos.Load()) / 1e9
	st.ReconcileSeconds = float64(s.mReconcileNanos.Load()) / 1e9
	if prev := s.status.Load(); prev != nil {
		st.LastSync = prev.LastSync
	}
	if progressed {
		st.LastSync = now
	}
	s.trackBehind(now, lag)
	s.status.Store(&st)
}

// trackBehind edge-logs a "falling behind" episode: it WARNs once when the source has been behind
// (lag over behindLagThreshold) for at least behindLogThreshold, and INFOs on recovery with the
// episode duration and peak lag. Called only from the sync goroutine (publishStatus), so its
// episode fields need no lock. This is the human-readable companion to sync_behind_seconds_total.
func (s *JobSync) trackBehind(now time.Time, lag int64) {
	if lag > behindLagThreshold {
		if s.behindSince.IsZero() {
			s.behindSince, s.behindPeak = now, lag
		}
		if lag > s.behindPeak {
			s.behindPeak = lag
		}
		if !s.behindLogged && now.Sub(s.behindSince) >= behindLogThreshold {
			s.log.Warn("scheddsync: job mirror falling behind",
				"behind_seconds", now.Sub(s.behindSince).Seconds(), "lag_bytes", lag)
			s.behindLogged = true
		}
		return
	}
	if s.behindLogged { // recovered from a logged episode
		s.log.Info("scheddsync: job mirror caught up",
			"behind_seconds", now.Sub(s.behindSince).Seconds(), "peak_lag_bytes", s.behindPeak)
	}
	s.behindSince, s.behindPeak, s.behindLogged = time.Time{}, 0, false
}

func (s *HistorySync) publishStatus(progressed bool) {
	size, lag := lagAndFile(s.filename, s.offset)
	st := SyncStatus{Kind: s.kind, Source: s.filename, Offset: s.offset, FileSize: size, LagBytes: lag, CaughtUp: lag == 0}
	st.Resyncs, st.LastResync = s.resyncs, s.lastResync
	if prev := s.status.Load(); prev != nil {
		st.LastSync = prev.LastSync
	}
	if progressed {
		st.LastSync = nowFn()
	}
	s.status.Store(&st)
}

// nowFn is time.Now, indirected so a test can pin the clock.
var nowFn = time.Now
