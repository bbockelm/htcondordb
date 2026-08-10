package server

import (
	"context"
	"time"
)

// DefaultArchiveRetrainInterval is how often an archive retrains its compression dictionary during
// maintenance. Daily rather than the mutable side's 15 minutes, because an archive's dictionary is
// adopted lazily (see maybeRetrainArchive) and each retrain registers a dictionary that segments
// written under it then reference, so retraining often buys little and accumulates dictionaries.
const DefaultArchiveRetrainInterval = 24 * time.Hour

// RunPeriodicArchiveMaintenance maintains every archive table every interval until ctx is
// cancelled (a no-op if interval <= 0, or while no archive tables exist). Each pass rotates
// (drops whole sealed segments outside retention, so a history table does not grow without
// bound) and then reindexes -- building the per-segment sidecar index for segments sealed
// since the last pass, so a continuously-appended history keeps its queries accelerated
// without waiting for a daemon restart. Reindex is idempotent: an already-sidecar'd segment
// is skipped, and it builds pageable mmap sidecars off the write path, so it neither stalls
// appends nor holds indexes resident in RAM. Intended to run in its own goroutine.
func (s *Service) RunPeriodicArchiveMaintenance(ctx context.Context, interval time.Duration) {
	s.RunPeriodicArchiveMaintenanceEvery(ctx, interval, DefaultArchiveRetrainInterval)
}

// RunPeriodicArchiveMaintenanceEvery is RunPeriodicArchiveMaintenance with an explicit dictionary
// retrain interval. retrainInterval <= 0 disables retraining, leaving rotation and reindexing.
func (s *Service) RunPeriodicArchiveMaintenanceEvery(ctx context.Context, interval, retrainInterval time.Duration) {
	if interval <= 0 {
		return
	}
	s.archiveRetrainEvery = retrainInterval
	// Seed each archive's clock at startup rather than at zero, so a restart does not retrain
	// immediately: a daemon restarting frequently would otherwise retrain on every start, and the
	// last-retrain time is in-process state that a reopen does not recover.
	s.archiveRetrainSeed = time.Now()
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
		s.maybeRetrainArchive(name)
	}
}

// maybeRetrainArchive retrains the archive's compression dictionary if its interval has elapsed.
//
// LAZY ADOPTION is the point. RetrainDict on an append-only collection trains a dictionary, installs
// it for new writes, and stops: every segment records the dictionary it was written under and
// recovery reconstructs all of them, so existing segments stay readable on their old dictionary
// indefinitely. Re-encoding them is an optimization, not a correctness requirement -- so a retrain
// here costs one sample pass and one dictionary registration, not an archive-wide reseal. Old data
// picks the new dictionary up when something else rewrites it (a merge, an UpgradeCodecPass, an
// explicit Rewrite), which is why this can run on an ordinary maintenance boundary at all.
//
// Retraining matters most for an archive precisely because it is append-only: the data drifts, and
// unlike a mutable table nothing else ever revisits the dictionary. Until now nothing retrained one
// automatically, so a history table kept whatever dictionary it happened to get early in life.
func (s *Service) maybeRetrainArchive(name string) {
	if s.archiveRetrainEvery <= 0 {
		return
	}
	s.retrainMu.Lock()
	last, ok := s.lastArchiveRetrain[name]
	if !ok {
		last = s.archiveRetrainSeed
	}
	if time.Since(last) < s.archiveRetrainEvery {
		s.retrainMu.Unlock()
		return
	}
	// Claim the slot before the (slow) retrain so a second pass cannot start one concurrently.
	if s.lastArchiveRetrain == nil {
		s.lastArchiveRetrain = map[string]time.Time{}
	}
	s.lastArchiveRetrain[name] = time.Now()
	s.retrainMu.Unlock()

	a, ok := s.cat.ArchiveTable(name)
	if !ok {
		return
	}
	start := time.Now()
	// 0 = no record-count bound, so the sampler's own byte budget decides how much to draw. That is
	// what the dictionary trainer actually consumes, and it is what makes the cost independent of how
	// fat this table's ads happen to be: a count of 2000 is a few hundred KB of small job ads or ~15
	// MB of slot ads, while the byte budget is the same work either way. Requires classad >= v0.26.0,
	// where 0 means "unbounded count"; in older versions CollectSamples(0) returned NO samples, so a
	// retrain would have silently declined with "no samples" instead of training.
	n, err := a.RetrainDict(0)
	if err != nil {
		// Training legitimately declines on small or homogeneous data (BuildDict rejects some
		// sample distributions). Keep the existing dictionary and try again next interval.
		s.log.Info("archive dictionary retrain declined", "archive", name, "err", err.Error())
		return
	}
	s.log.Info("retrained archive dictionary", "archive", name, "dict_bytes", n,
		"took", time.Since(start).Round(time.Millisecond).String())
}
