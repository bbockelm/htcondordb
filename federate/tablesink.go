package federate

import (
	"fmt"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"

	"github.com/bbockelm/htcondordb/cedarsync"
	"github.com/bbockelm/htcondordb/syncstatus"
)

// maxBatch bounds the writes one hub transaction carries. A flush window normally commits far
// fewer; this caps a Reset replay's transaction size.
const maxBatch = 4096

// tableSink replicates one spoke's mutable table (jobs, syncstatus) into the hub's table of the
// same name. It differs from replicate.NewTableSink in every way that matters for an aggregate:
//
//   - keys are namespaced by the source schedd (HubKey), so two APs' "123.0" are two rows and a
//     delete from one AP never touches another's;
//   - ScheddName is overwritten from the source's validated identity on every row;
//   - a Reset is reconciled, not ignored: the keys the replay touches are recorded, an incoming
//     row identical to the stored one is not written, and at Synced every row of this schedd the
//     replay did not touch is deleted -- so a row deleted at the spoke while the hub was away does
//     not live on as a phantom, and the AP's rows never vanish mid-replay;
//   - writes are batched into one transaction per flush and the resume cursor is committed only
//     after that transaction (at-least-once; upserts and deletes are idempotent).
//
// Not safe for concurrent use: the Runner calls Apply, Flush and the session hooks from one
// goroutine. Cursor is the exception (guarded), since status reporting may read it.
type tableSink struct {
	tbl     *db.DB
	table   string
	schedd  string
	store   replicate.CursorStore
	metrics *Metrics
	now     func() time.Time
	onReset func()
	// commit makes a batch durable: (*db.Txn).Commit, which returns only after the store's
	// durability sync (msync) -- never CommitNondurable, since the cursor that follows must not
	// cover writes a crash could lose. A seam for the ordering test.
	commit func(*db.Txn) error

	tx      *db.Txn
	pending int
	nextCur []byte // cursor to commit once the pending transaction commits
	// failed is the error that dropped a batch this session. Until the next BeginSession the sink
	// commits no cursor: the dropped batch's changes are covered by every later cursor of the
	// session, and only a new session (resuming from the last committed cursor) re-delivers them.
	failed error

	catchup   bool                // before this session's Synced
	resetting bool                // a Reset replay is in progress
	touched   map[string]struct{} // hub keys the Reset replay delivered

	mu  sync.Mutex
	cur []byte
}

var _ cedarsync.UndecodableSink = (*tableSink)(nil)

func newTableSink(tbl *db.DB, table, schedd string, store replicate.CursorStore, m *Metrics, now func() time.Time, onReset func()) (*tableSink, error) {
	cur, err := store.Load()
	if err != nil {
		return nil, err
	}
	return &tableSink{tbl: tbl, table: table, schedd: schedd, store: store, metrics: m, now: now, onReset: onReset,
		commit: (*db.Txn).Commit, cur: cur, catchup: true}, nil
}

// BeginSession starts from the committed cursor with nothing pending: whatever a failed session
// left behind (an uncommitted batch, its cursor) is discarded, since the session re-delivers it.
func (s *tableSink) BeginSession() {
	s.discard()
	s.failed = nil
	s.catchup = true
}

// discard drops the pending batch and the cursor that would have covered it.
func (s *tableSink) discard() {
	if s.tx != nil {
		s.tx.Abort()
		s.tx = nil
	}
	s.pending, s.nextCur = 0, nil
}

// fail records err as the session's failure and drops what it covers.
func (s *tableSink) fail(err error) error {
	s.discard()
	if s.failed == nil {
		s.failed = err
	}
	return err
}

// EndSession abandons an unfinished Reset replay WITHOUT sweeping: the replay did not deliver the
// whole source, so the untouched set is not "rows the source no longer has". The Reset cleared the
// committed cursor, so the next session is a full replay again and that replay sweeps.
func (s *tableSink) EndSession() {
	s.resetting, s.touched = false, nil
}

func (s *tableSink) Apply(c replicate.Change) error {
	if s.failed != nil {
		return fmt.Errorf("federate: %s from %s: a batch failed this session: %w", s.table, s.schedd, s.failed)
	}
	if err := s.apply(c); err != nil {
		return s.fail(err)
	}
	return nil
}

func (s *tableSink) apply(c replicate.Change) error {
	switch c.Kind {
	case replicate.KindUpsert:
		if c.Ad == nil {
			s.undecodable(c.Key)
			break
		}
		if err := s.upsert(c.Key, c.Ad); err != nil {
			return err
		}
	case replicate.KindDelete:
		hk := HubKey(s.schedd, c.Key)
		tx := s.txn()
		if tx.Has(hk) {
			tx.DestroyClassAd(hk)
			s.pending++
			s.metrics.EventsApplied.WithLabelValues(s.table, "delete").Inc()
		}
	case replicate.KindReset:
		if err := s.flush(); err != nil {
			return err
		}
		if err := clearCursor(s.store, &s.mu, &s.cur); err != nil {
			return err
		}
		s.resetting, s.touched, s.catchup = true, map[string]struct{}{}, true
		s.metrics.Resets.WithLabelValues(s.table).Inc()
		if s.onReset != nil {
			s.onReset()
		}
	case replicate.KindSynced:
		if err := s.commitTxn(); err != nil {
			return err
		}
		if s.resetting {
			if err := s.sweep(); err != nil {
				return err
			}
			s.resetting, s.touched = false, nil
		}
		s.catchup = false
		if len(c.Cursor) > 0 {
			s.nextCur = c.Cursor
		}
		return s.saveCursor()
	}
	if len(c.Cursor) > 0 {
		s.nextCur = c.Cursor
	}
	if s.pending >= maxBatch {
		return s.flush()
	}
	return nil
}

// ApplyUndecodable takes an upsert whose ad could not be decoded (cedarsync.UndecodableSink). The
// source has the key, so a Reset replay counts it as delivered and the sweep keeps the hub's row;
// the row stays as the hub holds it (or absent) until the source changes it again.
func (s *tableSink) ApplyUndecodable(c replicate.Change, _ error) error {
	c.Kind, c.Ad = replicate.KindUpsert, nil
	return s.Apply(c)
}

func (s *tableSink) undecodable(key string) {
	if s.resetting {
		s.touched[HubKey(s.schedd, key)] = struct{}{}
	}
	s.metrics.Undecodable.WithLabelValues(s.table).Inc()
}

func (s *tableSink) upsert(key string, ad *classad.ClassAd) error {
	hk := HubKey(s.schedd, key)
	ad.InsertAttrString(ScheddNameAttr, s.schedd) // overwrite: the source's identity, never the row's claim
	if s.resetting {
		s.touched[hk] = struct{}{}
	}
	tx := s.txn()
	if s.table == TableSyncStatus {
		s.stampHeartbeat(tx, hk, ad)
	}
	if s.catchup {
		// During catch-up most rows are unchanged (a spoke restart replays its whole table), so
		// compare before writing: an identical row costs a lookup, not a write, a watch event
		// downstream and a dead version in the store. (A redelivered heartbeat keeps its receipt
		// stamp, so it compares identical too.)
		if stored, ok := tx.LookupClassAd(hk); ok && stored.Equal(ad) {
			s.metrics.IdenticalSkips.WithLabelValues(s.table).Inc()
			return nil
		}
	}
	tx.NewClassAd(hk, ad)
	s.pending++
	s.metrics.EventsApplied.WithLabelValues(s.table, "upsert").Inc()
	return nil
}

// stampHeartbeat sets HubReceivedTime on a syncstatus row. A redelivery of the heartbeat already
// stored (same HeartbeatSeq and HeartbeatTime) keeps its original receipt time; otherwise a hub
// restart or a spoke Reset would replay a heartbeat that might be minutes old and stamp it as just
// received, and a dead syncer would read as fresh. A heartbeat the hub has not seen is stamped now
// only when it arrives live: one first seen during catch-up (a Reset replay, or a resume's
// overlap) may be arbitrarily old, so it gets no stamp -- staleness unknown, hence stale -- until
// the next live heartbeat. Any HubReceivedTime the row arrives with is the spoke's claim, never
// kept.
func (s *tableSink) stampHeartbeat(tx *db.Txn, hk string, ad *classad.ClassAd) {
	if stored, ok := tx.LookupClassAd(hk); ok {
		seq, ok1 := ad.EvaluateAttrInt(syncstatus.AttrHeartbeatSeq)
		ht, ok2 := ad.EvaluateAttrInt(syncstatus.AttrHeartbeatTime)
		oseq, ok3 := stored.EvaluateAttrInt(syncstatus.AttrHeartbeatSeq)
		oht, ok4 := stored.EvaluateAttrInt(syncstatus.AttrHeartbeatTime)
		if ok1 && ok2 && ok3 && ok4 && seq == oseq && ht == oht {
			if prev, ok := stored.EvaluateAttrInt(HubReceivedTimeAttr); ok {
				ad.InsertAttr(HubReceivedTimeAttr, prev)
				return
			}
		}
	}
	if s.catchup {
		ad.Delete(HubReceivedTimeAttr)
		return
	}
	ad.InsertAttr(HubReceivedTimeAttr, s.now().Unix())
}

// sweep deletes this schedd's rows that the just-finished Reset replay did not deliver: the source
// no longer has them.
func (s *tableSink) sweep() error {
	keys, err := s.tbl.KeysWhere(scheddConstraint(s.schedd))
	if err != nil {
		return err
	}
	var stale []string
	for k := range keys {
		if _, ok := s.touched[k]; !ok {
			stale = append(stale, k)
		}
	}
	for i := 0; i < len(stale); i += maxBatch {
		tx := s.tbl.Begin()
		for _, k := range stale[i:min(i+maxBatch, len(stale))] {
			tx.DestroyClassAd(k)
		}
		if err := s.commit(tx); err != nil {
			return fmt.Errorf("federate: sweeping %s rows of %s: %w", s.table, s.schedd, err)
		}
	}
	s.metrics.PhantomDeletes.WithLabelValues(s.table).Add(float64(len(stale)))
	return nil
}

func (s *tableSink) txn() *db.Txn {
	if s.tx == nil {
		s.tx = s.tbl.Begin()
	}
	return s.tx
}

func (s *tableSink) commitTxn() error {
	if s.tx == nil {
		return nil
	}
	tx := s.tx
	s.tx, s.pending = nil, 0
	if err := s.commit(tx); err != nil {
		return fmt.Errorf("federate: committing %s batch from %s: %w", s.table, s.schedd, err)
	}
	return nil
}

// Flush commits the pending batch durably and only then the cursor of its last change, so a
// committed cursor never covers a write a crash could lose. A failed commit drops the batch and
// its cursor and commits no further cursor this session: the next session resumes from the last
// committed cursor and re-delivers the batch.
func (s *tableSink) Flush() error {
	if s.failed != nil {
		return s.failed
	}
	if err := s.flush(); err != nil {
		return s.fail(err)
	}
	return nil
}

func (s *tableSink) flush() error {
	if err := s.commitTxn(); err != nil {
		return err
	}
	return s.saveCursor()
}

func (s *tableSink) saveCursor() error {
	if len(s.nextCur) == 0 {
		return nil
	}
	cur := s.nextCur
	s.nextCur = nil
	return s.Commit(cur)
}

func (s *tableSink) Commit(cursor []byte) error {
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

func (s *tableSink) Cursor() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.cur...)
}
