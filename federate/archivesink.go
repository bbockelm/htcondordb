package federate

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
)

// archiveSink replicates one spoke's archive (history, epoch_history) into the hub's archive of the
// same name. Unlike replicate.NewArchiveSink it does not re-append what the hub already holds:
//
//   - every record gets ScheddName overwritten from the source's identity;
//   - from session start until Synced -- which covers both a Reset replay of the spoke's whole
//     retained archive and the at-least-once overlap a cursor resume re-delivers -- each record is
//     checked against the hub archive by its identity (see recordIdentity) and dropped if present.
//     The first probeBudget records of a catch-up are checked with an exact query each; a
//     catch-up that runs past that is a full replay, and the sink then loads this schedd's
//     identities from the hub archive once (one projected scan of the schedd's rows) and checks
//     the rest in memory. A query per record costs about a millisecond -- the archive's active
//     segment is scanned, not indexed -- which is fine for a resume's overlap and hours for a
//     replay of a million records;
//   - live-tail records are new by construction and are appended without a probe;
//   - a record with no identity is appended and counted (it cannot be deduplicated).
//
// Appends are not transactional, so a flush only commits the cursor -- after the appends it
// covers, which is what makes a crash between the two a re-delivery (deduplicated) rather than a
// loss. Archives are append-only at the spoke too, so deletes are ignored and a Reset needs no
// sweep.
type archiveSink struct {
	arch    *db.ArchiveTable
	table   string
	schedd  string
	store   replicate.CursorStore
	metrics *Metrics
	log     *slog.Logger
	onReset func()

	catchup bool
	nextCur []byte
	// failed is the error that stopped this session. Until the next BeginSession the sink commits
	// no cursor (see tableSink.failed).
	failed error
	// appended is set by an append since the last flush.
	appended bool
	// syncData makes every append so far durable; Flush calls it once, before committing the
	// cursor. See archiveSync. A seam for the ordering test.
	syncData func() error
	probes   int                   // exact probes made this catch-up
	idset    map[[16]byte]struct{} // this schedd's identities, once a catch-up outgrows probing

	mu  sync.Mutex
	cur []byte
}

func newArchiveSink(arch *db.ArchiveTable, table, schedd string, store replicate.CursorStore, m *Metrics, log *slog.Logger, onReset func()) (*archiveSink, error) {
	cur, err := store.Load()
	if err != nil {
		return nil, err
	}
	return &archiveSink{arch: arch, table: table, schedd: schedd, store: store, metrics: m, log: log, onReset: onReset,
		syncData: archiveSync(arch), cur: cur, catchup: true}, nil
}

// archiveSync returns the durability step Flush runs before it commits a cursor.
//
// In classad v0.30.11 there is nothing to call: db.ArchiveTable.Append is already durable when it
// returns -- collections.Archive.Append -> Collection.Put -> shard.applyOne/applyBatch, which run
// shard.syncFor (an msync of the pages written) before returning -- and db.ArchiveTable exposes
// neither a Sync/Flush nor a non-durable append (collections.Archive.Flush is a no-op). So the
// ordering "data durable, then cursor" holds, at the price of one msync per appended record.
//
// TODO(classad): add db.ArchiveTable.AppendNondurable and db.ArchiveTable.Sync() (msync every
// shard's dirty pages); then append nondurably and return arch.Sync here, so a catch-up pays one
// msync per flush instead of one per record.
func archiveSync(_ *db.ArchiveTable) func() error {
	return func() error { return nil }
}

// probeBudget is how many records of a catch-up are checked by exact query before the sink loads
// the identity set instead.
const probeBudget = 256

// BeginSession starts from the committed cursor: a cursor a failed session left uncommitted is
// discarded, since the session re-delivers what it covers. (appended is kept: those appends still
// need their sync before any cursor that follows them.)
func (s *archiveSink) BeginSession() {
	s.nextCur, s.failed = nil, nil
	s.startCatchup()
}

func (s *archiveSink) EndSession() { s.idset = nil }

// fail records err as the session's failure and drops the uncommitted cursor.
func (s *archiveSink) fail(err error) error {
	s.nextCur = nil
	if s.failed == nil {
		s.failed = err
	}
	return err
}

func (s *archiveSink) startCatchup() {
	s.catchup, s.probes, s.idset = true, 0, nil
}

func (s *archiveSink) Apply(c replicate.Change) error {
	if s.failed != nil {
		return fmt.Errorf("federate: %s from %s: an earlier change failed this session: %w", s.table, s.schedd, s.failed)
	}
	if err := s.apply(c); err != nil {
		return s.fail(err)
	}
	return nil
}

func (s *archiveSink) apply(c replicate.Change) error {
	switch c.Kind {
	case replicate.KindUpsert:
		if c.Ad == nil {
			break
		}
		c.Ad.InsertAttrString(ScheddNameAttr, s.schedd)
		var id recordIdentity
		var hasID bool
		if s.catchup {
			var present bool
			var err error
			id, hasID, present, err = s.present(c)
			if err != nil {
				return err
			}
			if present {
				s.metrics.DedupHits.WithLabelValues(s.table).Inc()
				break
			}
		}
		if err := s.arch.Append(c.Ad); err != nil {
			return err
		}
		s.appended = true
		if hasID && s.idset != nil {
			s.idset[id.digest()] = struct{}{}
		}
		s.metrics.EventsApplied.WithLabelValues(s.table, "upsert").Inc()
	case replicate.KindReset:
		s.startCatchup() // a replay of everything retained: all of it may already be here
		s.metrics.Resets.WithLabelValues(s.table).Inc()
		if s.onReset != nil {
			s.onReset()
		}
	case replicate.KindSynced:
		s.catchup, s.idset = false, nil
	case replicate.KindDelete, replicate.KindGap:
		// append-only: nothing to remove
	}
	if len(c.Cursor) > 0 {
		s.nextCur = c.Cursor
	}
	if c.Kind == replicate.KindSynced {
		return s.flush()
	}
	return nil
}

// present reports whether the hub archive already holds c's record.
func (s *archiveSink) present(c replicate.Change) (id recordIdentity, ok, present bool, err error) {
	id, ok = archiveIdentity(s.table, c.Ad)
	if !ok {
		s.metrics.MissingIdentity.WithLabelValues(s.table).Inc()
		s.log.Debug("federate: archive record has no identity; appended without dedup",
			"table", s.table, "schedd", s.schedd, "key", c.Key)
		return id, false, false, nil
	}
	if s.idset == nil && s.probes >= probeBudget {
		if err := s.loadIdentities(); err != nil {
			return id, true, false, err
		}
	}
	if s.idset != nil {
		_, present = s.idset[id.digest()]
		return id, true, present, nil
	}
	s.probes++
	seq, err := s.arch.QueryLimit(id.constraint(s.table, s.schedd), 1)
	if err != nil {
		return id, true, false, err
	}
	for range seq {
		return id, true, true, nil
	}
	return id, true, false, nil
}

// loadIdentities reads every identity this schedd has in the hub archive.
func (s *archiveSink) loadIdentities() error {
	seq, err := s.arch.QueryProject(scheddConstraint(s.schedd), identityAttrs)
	if err != nil {
		return err
	}
	set := map[[16]byte]struct{}{}
	for vals := range seq {
		if id, ok := identityFromValues(s.table, vals); ok {
			set[id.digest()] = struct{}{}
		}
	}
	s.idset = set
	s.log.Info("federate: catch-up is a full replay; loaded the hub's identities for it",
		"table", s.table, "schedd", s.schedd, "identities", len(set))
	return nil
}

// Flush makes the appends since the last flush durable (once, not per record) and only then
// commits the cursor of the last applied change, so a committed cursor never covers an append a
// crash could lose. A failed sync drops the cursor and commits none for the rest of the session.
func (s *archiveSink) Flush() error {
	if s.failed != nil {
		return s.failed
	}
	if err := s.flush(); err != nil {
		return s.fail(err)
	}
	return nil
}

func (s *archiveSink) flush() error {
	if s.appended {
		if err := s.syncData(); err != nil {
			return fmt.Errorf("federate: syncing %s appends from %s: %w", s.table, s.schedd, err)
		}
		s.appended = false
	}
	if len(s.nextCur) == 0 {
		return nil
	}
	cur := s.nextCur
	s.nextCur = nil
	return s.Commit(cur)
}

func (s *archiveSink) Commit(cursor []byte) error {
	if len(cursor) == 0 {
		return nil
	}
	if err := s.store.Save(cursor); err != nil {
		return err
	}
	s.mu.Lock()
	s.cur = append([]byte(nil), cursor...)
	s.mu.Unlock()
	return nil
}

func (s *archiveSink) Cursor() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.cur...)
}
