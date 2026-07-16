package server

import (
	"context"
	"time"
)

// RunPeriodicArchiveRotation enforces every archive table's retention policy every
// interval until ctx is cancelled (a no-op if interval <= 0, or while no archive tables
// exist). Rotation drops whole sealed segments that fall outside retention, so a history
// table does not grow without bound. Intended to run in its own goroutine.
func (s *Service) RunPeriodicArchiveRotation(ctx context.Context, interval time.Duration) {
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
			now := float64(time.Now().Unix())
			for _, name := range s.cat.ArchiveTables() {
				a, ok := s.cat.ArchiveTable(name)
				if !ok {
					continue
				}
				dropped, err := a.Rotate(now)
				if err != nil {
					s.log.Warn("archive rotation failed", "archive", name, "err", err.Error())
					continue
				}
				if dropped > 0 {
					s.log.Info("rotated archive", "archive", name, "segments_dropped", dropped)
				}
			}
		}
	}
}
