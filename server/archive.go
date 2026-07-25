package server

import (
	"context"
	"time"
)

// RunPeriodicArchiveMaintenance maintains every archive table every interval until ctx is
// cancelled (a no-op if interval <= 0, or while no archive tables exist). Each pass rotates
// (drops whole sealed segments outside retention, so a history table does not grow without
// bound) and then reindexes -- building the per-segment sidecar index for segments sealed
// since the last pass, so a continuously-appended history keeps its queries accelerated
// without waiting for a daemon restart. Reindex is idempotent: an already-sidecar'd segment
// is skipped, and it builds pageable mmap sidecars off the write path, so it neither stalls
// appends nor holds indexes resident in RAM. Intended to run in its own goroutine.
func (s *Service) RunPeriodicArchiveMaintenance(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.maintainArchivesOnce(float64(time.Now().Unix()))
		}
	}
}

// maintainArchivesOnce runs one maintenance pass over every archive table: rotate to
// retention, then reindex so segments sealed since the last pass get their sidecar index.
func (s *Service) maintainArchivesOnce(now float64) {
	for _, name := range s.cat.ArchiveTables() {
		a, ok := s.cat.ArchiveTable(name)
		if !ok {
			continue
		}
		dropped, err := a.Rotate(now)
		if err != nil {
			s.log.Warn("archive rotation failed", "archive", name, "err", err.Error())
		} else if dropped > 0 {
			s.log.Info("rotated archive", "archive", name, "segments_dropped", dropped)
		}
		a.Reindex() // build sidecar indexes for newly-sealed segments (idempotent, off-lock)
	}
}
