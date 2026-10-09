package federate

import (
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"

	"github.com/bbockelm/htcondordb/cedarsync"
)

// archiveSink replicates one spoke's archive (history, epoch_history) into the hub's archive of the
// same name. Unlike replicate.NewArchiveSink it does not re-append what the hub already holds:
//
//   - every record gets ScheddName overwritten from the source's identity;
//   - from session start until Synced -- which covers both a Reset replay of the spoke's whole
//     retained archive and the at-least-once overlap a cursor resume re-delivers -- each record is
//     checked against what the hub archive held BEFORE the catch-up, by identity (see
//     recordIdentity) and by count: an identity is not unique (one run instance writes a
//     CHECKPOINT epoch record per checkpoint, a COMMON one per common-files group), so if the hub
//     held k records of an identity, the first k replayed records of it are dropped and the rest
//     appended. Records this catch-up appends never count as already held. The count of an
//     identity is read when the catch-up first meets it -- before it can have appended any -- by
//     an exact query. A Reset replay that runs past probeBudget queries then loads every identity
//     count this schedd has in the hub archive once (one projected scan of the schedd's rows) and
//     checks the rest in memory. A query per record costs about a millisecond -- the archive's
//     active segment is scanned, not indexed -- which is fine for a resume's overlap and hours for
//     a replay of a million records. A resume's catch-up is always probed: it re-delivers only the
//     overlap since the committed cursor (after a WatchResync, up to the spoke's watch buffer), and
//     loading the AP's whole history for that would cost more than the probes;
//   - during a Reset replay into a capped archive that has reached its cap, a record older than
//     the oldest the hub still holds for this schedd (by EnteredHistoryTime, or EpochWriteDate for
//     epochs) is skipped: the cap already dropped it, and re-appending it would make it the newest
//     data in the archive, evicting other APs' recent records at the next rotation -- on every
//     spoke restart;
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
	full     bool // this catch-up is a Reset replay of everything the spoke retains
	probes   int  // exact probes made this catch-up
	// remaining maps an identity digest to how many records of it the hub held before this
	// catch-up that the catch-up has not yet matched. An identity absent from it has not been
	// looked up yet -- unless setLoaded, when it was loaded and the hub held none.
	remaining map[[16]byte]int
	setLoaded bool // remaining holds every identity of this schedd (a long catch-up)
	setLoads  int  // identity-set loads since the sink was made (for tests)
	// floor is the oldest floorAttr value the hub holds for this schedd, read when a Reset begins;
	// floorOK only when the archive is capped and at its cap (see retentionDropped).
	floor   float64
	floorOK bool

	mu  sync.Mutex
	cur []byte
}

var _ cedarsync.UndecodableSink = (*archiveSink)(nil)

func newArchiveSink(arch *db.ArchiveTable, table, schedd string, store replicate.CursorStore, m *Metrics, log *slog.Logger, onReset func()) (*archiveSink, error) {
	cur, err := store.Load()
	if err != nil {
		return nil, err
	}
	s := &archiveSink{arch: arch, table: table, schedd: schedd, store: store, metrics: m, log: log, onReset: onReset,
		syncData: archiveSync(arch), cur: cur}
	s.startCatchup() // a sink starts in catch-up, as after BeginSession
	return s, nil
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

// probeBudget is how many records of a Reset replay are checked by exact query before the sink
// loads the identity set instead.
const probeBudget = 256

// BeginSession starts from the committed cursor: a cursor a failed session left uncommitted is
// discarded, since the session re-delivers what it covers. (appended is kept: those appends still
// need their sync before any cursor that follows them.)
func (s *archiveSink) BeginSession() {
	s.nextCur, s.failed = nil, nil
	s.startCatchup()
}

func (s *archiveSink) EndSession() { s.endCatchup() }

// fail records err as the session's failure and drops the uncommitted cursor.
func (s *archiveSink) fail(err error) error {
	s.nextCur = nil
	if s.failed == nil {
		s.failed = err
	}
	return err
}

func (s *archiveSink) startCatchup() {
	s.catchup, s.full, s.probes, s.remaining, s.setLoaded = true, false, 0, map[[16]byte]int{}, false
	s.floorOK = false
}

func (s *archiveSink) endCatchup() {
	s.catchup, s.full, s.probes, s.remaining, s.setLoaded = false, false, 0, nil, false
	s.floorOK = false
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
		if c.Ad == nil { // undecodable: nothing to append
			s.metrics.Undecodable.WithLabelValues(s.table).Inc()
			break
		}
		c.Ad.InsertAttrString(ScheddNameAttr, s.schedd)
		if s.full && s.floorOK {
			if v, ok := c.Ad.EvaluateAttrNumber(floorAttr(s.table)); ok && v < s.floor {
				s.metrics.BelowRetention.WithLabelValues(s.table).Inc()
				break
			}
		}
		if s.catchup {
			held, err := s.held(c)
			if err != nil {
				return err
			}
			if held {
				s.metrics.DedupHits.WithLabelValues(s.table).Inc()
				break
			}
		}
		if err := s.arch.Append(c.Ad); err != nil {
			return err
		}
		s.appended = true
		s.metrics.EventsApplied.WithLabelValues(s.table, "upsert").Inc()
	case replicate.KindReset:
		if err := s.flush(); err != nil {
			return err
		}
		if err := clearCursor(s.store, &s.mu, &s.cur); err != nil {
			return err
		}
		s.startCatchup() // a replay of everything retained: all of it may already be here
		s.full = true
		if err := s.loadFloor(); err != nil {
			return err
		}
		s.metrics.Resets.WithLabelValues(s.table).Inc()
		if s.onReset != nil {
			s.onReset()
		}
	case replicate.KindSynced:
		s.endCatchup()
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

// ApplyUndecodable takes an upsert whose ad could not be decoded (cedarsync.UndecodableSink): it is
// counted and skipped.
func (s *archiveSink) ApplyUndecodable(c replicate.Change, _ error) error {
	c.Kind, c.Ad = replicate.KindUpsert, nil
	return s.Apply(c)
}

// held reports whether c's record matches one the hub held before this catch-up that no earlier
// record of the catch-up matched, and consumes that match.
func (s *archiveSink) held(c replicate.Change) (bool, error) {
	id, ok := archiveIdentity(s.table, c.Ad)
	if !ok {
		s.metrics.MissingIdentity.WithLabelValues(s.table).Inc()
		s.log.Debug("federate: archive record has no identity; appended without dedup",
			"table", s.table, "schedd", s.schedd, "key", c.Key)
		return false, nil
	}
	d := id.digest()
	n, known := s.remaining[d]
	if !known && !s.setLoaded {
		if s.full && s.probes >= probeBudget {
			if err := s.loadIdentities(); err != nil {
				return false, err
			}
			n = s.remaining[d]
		} else {
			s.probes++
			var err error
			if n, err = s.countHeld(id); err != nil {
				return false, err
			}
		}
	}
	if n == 0 {
		s.remaining[d] = 0
		return false, nil
	}
	s.remaining[d] = n - 1
	return true, nil
}

// countHeld counts the hub's records of id for this schedd.
func (s *archiveSink) countHeld(id recordIdentity) (int, error) {
	seq, err := s.arch.Query(id.constraint(s.table, s.schedd))
	if err != nil {
		return 0, err
	}
	n := 0
	for range seq {
		n++
	}
	return n, nil
}

// loadIdentities counts every identity this schedd has in the hub archive, for the identities
// this catch-up has not met yet. One it has met keeps its running count: the scan would also see
// the records the catch-up appended, and those are not "held before".
func (s *archiveSink) loadIdentities() error {
	seq, err := s.arch.QueryProject(scheddConstraint(s.schedd), identityAttrs)
	if err != nil {
		return err
	}
	loaded := map[[16]byte]int{}
	for vals := range seq {
		if id, ok := identityFromValues(s.table, vals); ok {
			loaded[id.digest()]++
		}
	}
	for d, n := range loaded {
		if _, met := s.remaining[d]; !met {
			s.remaining[d] = n
		}
	}
	s.setLoaded = true
	s.setLoads++
	s.log.Info("federate: catch-up is a full replay; loaded the hub's identities for it",
		"table", s.table, "schedd", s.schedd, "identities", len(loaded))
	return nil
}

// floorAttr is the time attribute a replayed record is compared to the hub's retained floor by.
func floorAttr(table string) string {
	if table == TableEpochHistory {
		return "EpochWriteDate"
	}
	return "EnteredHistoryTime"
}

// loadFloor reads, when the archive's cap has dropped data, the oldest floorAttr value the hub
// holds for this schedd.
func (s *archiveSink) loadFloor() error {
	s.floorOK = false
	if !retentionDropped(s.arch) {
		return nil
	}
	attr := floorAttr(s.table)
	rows, err := s.arch.Aggregate(scheddConstraint(s.schedd), nil, []db.AggSpec{{Func: db.AggMin, Arg: attr}})
	if err != nil {
		return fmt.Errorf("federate: reading the oldest retained %s of %s: %w", attr, s.schedd, err)
	}
	if len(rows) == 0 || len(rows[0].Values) == 0 {
		return nil
	}
	v, err := strconv.ParseFloat(rows[0].Values[0], 64)
	if err != nil {
		return nil // undefined: the hub holds nothing of this schedd's to compare with
	}
	s.floor, s.floorOK = v, true
	s.log.Info("federate: replaying into a capped archive; records older than the hub retains are skipped",
		"table", s.table, "schedd", s.schedd, "attr", attr, "floor", v)
	return nil
}

// retentionDropped reports whether an archive's retention has (or may have) dropped records: it is
// capped by segment count or bytes and is within one segment of that cap -- an append-only archive
// that has reached its cap stays there, so being under it means rotation has dropped nothing
// (unless the cap was raised since) -- or it is capped by age, which drops whatever ages out.
func retentionDropped(a *db.ArchiveTable) bool {
	r := a.Retention()
	if r.MaxAge > 0 {
		return true
	}
	st := a.Stats()
	if st.Segments == 0 {
		return false
	}
	if r.MaxSegments > 0 && st.Segments >= r.MaxSegments {
		return true
	}
	return r.MaxBytes > 0 && st.UsedBytes+st.UsedBytes/int64(st.Segments) > r.MaxBytes
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
