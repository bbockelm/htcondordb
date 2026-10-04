package federate

import (
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
	probes  int                   // exact probes made this catch-up
	idset   map[[16]byte]struct{} // this schedd's identities, once a catch-up outgrows probing

	mu  sync.Mutex
	cur []byte
}

func newArchiveSink(arch *db.ArchiveTable, table, schedd string, store replicate.CursorStore, m *Metrics, log *slog.Logger, onReset func()) (*archiveSink, error) {
	cur, err := store.Load()
	if err != nil {
		return nil, err
	}
	return &archiveSink{arch: arch, table: table, schedd: schedd, store: store, metrics: m, log: log, onReset: onReset, cur: cur, catchup: true}, nil
}

// probeBudget is how many records of a catch-up are checked by exact query before the sink loads
// the identity set instead.
const probeBudget = 256

func (s *archiveSink) BeginSession() { s.startCatchup() }
func (s *archiveSink) EndSession()   { s.idset = nil }

func (s *archiveSink) startCatchup() {
	s.catchup, s.probes, s.idset = true, 0, nil
}

func (s *archiveSink) Apply(c replicate.Change) error {
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
		return s.Flush()
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

// Flush commits the cursor of the last applied change. The appends it covers are already in the
// archive.
func (s *archiveSink) Flush() error {
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
