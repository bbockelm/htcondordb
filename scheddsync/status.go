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
	LastSync   time.Time // wall-clock of the last read pass that made progress
	Resyncs    int64     // cumulative durability-gap (resync) events seen (history only)
	LastResync time.Time // time of the most recent resync event, zero if none
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

// publishStatus atomically stores a fresh snapshot. progressed marks a read pass that consumed
// data, refreshing LastSync. It preserves the accumulated LastSync/Resyncs/LastResync across
// snapshots. Called only from the sync goroutine.
func (s *JobSync) publishStatus(progressed bool) {
	off := s.parser.GetNextOffset()
	src := s.parser.GetFilename()
	size, lag := lagAndFile(src, off)
	st := SyncStatus{Kind: "job_queue.log", Source: src, Offset: off, FileSize: size, LagBytes: lag, CaughtUp: lag == 0}
	if prev := s.status.Load(); prev != nil {
		st.LastSync = prev.LastSync
	}
	if progressed {
		st.LastSync = nowFn()
	}
	s.status.Store(&st)
}

func (s *HistorySync) publishStatus(progressed bool) {
	size, lag := lagAndFile(s.filename, s.offset)
	st := SyncStatus{Kind: "history", Source: s.filename, Offset: s.offset, FileSize: size, LagBytes: lag, CaughtUp: lag == 0}
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
