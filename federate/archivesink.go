package federate

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"

	"github.com/bbockelm/htcondordb/cedarsync"
)

// archiveSink replicates one spoke's archive (history, epoch_history, job_metrics) into the hub's
// archive of the same name. Unlike replicate.NewArchiveSink it does not re-append what the hub already holds:
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
//     an exact query. A query per record costs about a millisecond -- the archive's active segment
//     is scanned, not indexed;
//   - a Reset replay runs that check per record until probeBudget queries are spent and then loads
//     every identity count this schedd has in the hub archive once (one projected scan of the
//     schedd's rows, bounded by maxLoadedIdentities) and checks the rest in memory. Nothing about
//     a full replay is a prefix of what the hub holds -- the hub may hold a middle of it, having
//     been added to the federation late or having evicted the oldest -- so there is no boundary to
//     find and every record has to be checked;
//   - a resume's catch-up skips by batch instead (see drainBatch): it re-delivers the records
//     since the committed cursor in append order, so the ones the hub already holds are a PREFIX of
//     it, and only the boundary has to be found. Records are buffered up to catchupBatchBytes and
//     the LAST of each batch is probed: if the hub holds it the whole batch is prefix and is
//     dropped unprobed, and once a batch's last record is missing, that batch is checked record by
//     record and everything after the first miss is new;
//   - during a Reset replay into a capped archive that has reached its cap, a record older than
//     the oldest the hub still holds for this schedd (by floorAttr: EnteredHistoryTime,
//     EpochWriteDate for epochs, SampleTime for job_metrics) is skipped: the cap already dropped it, and re-appending it would make it the newest
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
	setGaveUp bool // the identity set did not fit its budget; this catch-up probes instead
	setLoads  int  // identity-set loads since the sink was made (for tests)
	// skipAhead is on while a resume's catch-up is buffering batches to skip the prefix the hub
	// already holds; batch holds that buffer and batchBytes approximates its memory.
	skipAhead   bool
	batch       []replicate.Change
	batchBytes  int
	batchProbes int // batch-tail probes made this catch-up (for tests)
	// batchLimit is catchupBatchBytes. A seam: the test that shows the bound holds drives it at a
	// size a test can fill.
	batchLimit int
	// budget is the hub-wide ceiling on identity sets (see identityBudget); identityHeld is what
	// this sink has reserved from it and identityChunk how much it takes at a time. Seams: a test
	// gives the sink a budget and a chunk it can fill.
	budget        *identityBudget
	identityHeld  int
	identityChunk int
	// pastBoundary is set once a resume's catch-up has met a record the hub does not hold. Every
	// record after it is new, and is appended without a probe -- see drainBatch.
	pastBoundary bool
	// floor is the oldest floorAttr value the hub holds for this schedd, read when a Reset begins;
	// floorOK only when the archive is capped and at its cap (see retentionDropped).
	floor   float64
	floorOK bool

	mu  sync.Mutex
	cur []byte
}

var _ cedarsync.UndecodableSink = (*archiveSink)(nil)

func newArchiveSink(arch *db.ArchiveTable, table, schedd string, store replicate.CursorStore, m *Metrics, log *slog.Logger, onReset func(), budget *identityBudget) (*archiveSink, error) {
	cur, err := store.Load()
	if err != nil {
		return nil, err
	}
	if budget == nil {
		budget = newIdentityBudget(defaultIdentityBudget)
	}
	s := &archiveSink{arch: arch, table: table, schedd: schedd, store: store, metrics: m, log: log, onReset: onReset,
		syncData: archiveSync(arch), cur: cur, batchLimit: catchupBatchBytes, budget: budget,
		identityChunk: identityReserveChunk}
	s.startCatchup() // a sink starts in catch-up, as after BeginSession
	return s, nil
}

// archiveSync returns the durability step Flush runs before it commits a cursor. Appends are
// deferred (db.ArchiveTable.AppendDeferred), so this is what makes them durable: the archive's
// Flush returns once every append before it is on disk. Committing the cursor only after it
// keeps the cursor from getting ahead of the archive across a crash, and a catch-up pays one
// sync per flush instead of one per record.
func archiveSync(arch *db.ArchiveTable) func() error { return arch.Flush }

// probeBudget is how many records of a Reset replay are checked by exact query before the sink
// loads the identity set instead.
const probeBudget = 256

// catchupBatchBytes is how much memory a resume's catch-up may hold in its skip-ahead buffer, per
// (schedd, table) sink.
//
// The bound is bytes, not records: 256 buffered history ads and 256 buffered job_metrics samples
// are very different amounts of memory, and memory is what has to stay bounded. A history ad of
// ~110 attributes costs about 15 KiB decoded and a job_metrics sample under 2 KiB, so 4 MiB is a
// batch of roughly 270 of the former or 2000 of the latter: an overlap of 150k history records
// costs about 550 probes, well under a second, where a probe per record costs 150k queries at
// about a millisecond each -- two and a half minutes, repeated on every hub restart for an AP
// whose cursor is stuck. Raising it buys less and less, and the memory is held by every sink
// catching up at once (three archive tables per AP).
//
// The buffered ads are not copies. The stream allocated them to deliver them and the archive would
// have taken them a moment later; buffering holds them past the point where they would otherwise
// be collected. approxAdBytes measures them (approximately, and deliberately on the high side).
const catchupBatchBytes = 4 << 20

// bytesPerIdentity is what one entry of the identity set costs, measured by
// BenchmarkJobMetricsIdentitySet: 73 MiB at a million entries and 276 MiB at five million, i.e.
// 58-77 bytes per entry for the map alone.
const bytesPerIdentity = 80

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

// startCatchup arms the catch-up checks. It starts in skip-ahead: a session begins by resuming
// from the committed cursor, and only a Reset that arrives afterwards turns it into a full replay.
func (s *archiveSink) startCatchup() {
	s.releaseIdentities()
	s.catchup, s.full, s.probes, s.remaining, s.setLoaded = true, false, 0, map[[16]byte]int{}, false
	s.setGaveUp, s.floorOK = false, false
	s.skipAhead, s.pastBoundary = true, false
	s.batch, s.batchBytes, s.batchProbes = nil, 0, 0
}

// endCatchup drops the catch-up state. Any records still buffered are dropped with it, so every
// caller drains the batch first (the drain is what appends them, and no cursor covering them is
// committed until it has: see flush).
func (s *archiveSink) endCatchup() {
	s.releaseIdentities()
	s.catchup, s.full, s.probes, s.remaining, s.setLoaded = false, false, 0, nil, false
	s.setGaveUp, s.floorOK = false, false
	s.skipAhead, s.pastBoundary = false, false
	s.batch, s.batchBytes = nil, 0
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
			s.metrics.Undecodable.WithLabelValues(s.table, s.schedd).Inc()
			break
		}
		c.Ad.InsertAttrString(ScheddNameAttr, s.schedd)
		if s.full && s.floorOK {
			if v, ok := c.Ad.EvaluateAttrNumber(floorAttr(s.table)); ok && v < s.floor {
				s.metrics.BelowRetention.WithLabelValues(s.table, s.schedd).Inc()
				break
			}
		}
		if s.skipAhead {
			s.buffer(c)
			if s.batchBytes >= s.batchLimit {
				if err := s.drainBatch(); err != nil {
					return err
				}
			}
			break
		}
		if s.catchup && !s.pastBoundary {
			held, _, err := s.held(c)
			if err != nil {
				return err
			}
			if held {
				s.metrics.DedupHits.WithLabelValues(s.table, s.schedd).Inc()
				break
			}
		}
		if err := s.appendRecord(c.Ad); err != nil {
			return err
		}
	case replicate.KindReset:
		if err := s.flush(); err != nil {
			return err
		}
		if err := clearCursor(s.store, &s.mu, &s.cur); err != nil {
			return err
		}
		s.startCatchup() // a replay of everything retained: all of it may already be here
		s.full = true
		// A full replay is not a prefix of what the hub holds, so there is no boundary to skip to
		// and every record is checked (by probe, then by the identity set).
		s.skipAhead = false
		if err := s.loadFloor(); err != nil {
			return err
		}
		s.metrics.Resets.WithLabelValues(s.table, s.schedd).Inc()
		if s.onReset != nil {
			s.onReset()
		}
	case replicate.KindSynced:
		if err := s.drainBatch(); err != nil { // before endCatchup drops what is buffered
			return err
		}
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
// record of the catch-up matched, and consumes that match. decided is false for a record with no
// identity: nothing can be said about it, so it is appended, and it says nothing about where a
// resume's boundary is either.
func (s *archiveSink) held(c replicate.Change) (held, decided bool, err error) {
	id, ok := archiveIdentity(s.table, c.Ad)
	if !ok {
		s.metrics.MissingIdentity.WithLabelValues(s.table, s.schedd).Inc()
		s.log.Debug("federate: archive record has no identity; appended without dedup",
			"table", s.table, "schedd", s.schedd, "key", c.Key)
		return false, false, nil
	}
	d := id.digest()
	n, known := s.remaining[d]
	if !known && !s.setLoaded {
		if s.full && s.probes >= probeBudget && !s.setGaveUp {
			if err := s.loadIdentities(); err != nil {
				return false, true, err
			}
		}
		if s.setLoaded {
			n = s.remaining[d]
		} else {
			s.probes++
			if n, err = s.countHeld(id); err != nil {
				return false, true, err
			}
		}
	}
	if n == 0 {
		s.remaining[d] = 0
		return false, true, nil
	}
	s.remaining[d] = n - 1
	return true, true, nil
}

// appendRecord appends one record and counts it.
func (s *archiveSink) appendRecord(ad *classad.ClassAd) error {
	if err := s.arch.AppendDeferred(ad); err != nil {
		// A record nested deeper than the archive can hold is refused the same way on every
		// replay, so failing the session on it would wedge this source at that record. A spoke
		// on the same storage could not have stored it either; this guards a spoke that can.
		if errors.Is(err, db.ErrTooDeep) {
			s.metrics.TooDeep.WithLabelValues(s.table, s.schedd).Inc()
			return nil
		}
		return err
	}
	s.appended = true
	s.metrics.EventsApplied.WithLabelValues(s.table, s.schedd, "upsert").Inc()
	return nil
}

// buffer holds c for the skip-ahead batch.
func (s *archiveSink) buffer(c replicate.Change) {
	s.batch = append(s.batch, c)
	s.batchBytes += changeOverheadBytes + len(c.Key) + len(c.Cursor) + approxAdBytes(c.Ad)
}

// changeOverheadBytes is what one buffered replicate.Change costs beyond its ad: the struct in the
// slice and the headers of the key and cursor it carries.
const changeOverheadBytes = 160

// drainBatch decides what to do with the records a resume's catch-up has buffered, and is where
// the skip-ahead happens.
//
// What makes one probe stand for a whole batch: an archive is append-only, which forces it into a
// SINGLE shard (collections/retention.go, collections/watch.go and collections/parallel_ordered.go
// in the classad module all take that as given), so a catch-up streams its records in append order
// with no interleaving between shards, and the spoke's planCatchup resets the session with
// WatchResetRetention when the cursor has fallen below the spoke's append floor rather than
// silently skipping the gap. The records the hub already holds are therefore a PREFIX of what the
// resume delivers, and finding the boundary is the whole job: if the hub holds the LAST record of
// a batch it holds everything before it too, and the batch can be dropped without a probe for any
// other record of it. (The spoke's own history tailer recovers on the same argument: "once a
// record is NOT present every later record is new" -- see scheddsync.HistorySync.)
//
// The holes in that prefix are holes that have to stay holes. A record the hub refused as too
// deeply nested, or could not decode, is skipped permanently and would be refused again. A record
// the hub's retention evicted must not be re-appended -- re-appending it would make the oldest
// data the newest and evict other APs at the next rotation, which is exactly what the Reset path's
// BelowRetention floor prevents. A transient append error cannot leave a hole: it returns, fails
// the session, and the uncommitted cursor has the whole range re-delivered.
//
// Repeated identities: an identity is not unique, and a dropped batch's records are never counted
// against the hub's copies of them. That is right for the dropped records themselves -- the whole
// batch is prefix, so all of them are held, and counting them could only have dropped them -- but
// it means a LATER record of an identity whose earlier occurrences were dropped would be matched
// against copies the prefix already accounts for. That is why nothing after the boundary is probed
// at all: by the prefix property every record after it is new, so probing there would be not
// merely wasteful but wrong. Up to the boundary the exact per-record path runs as always, and the
// one case it can still get wrong is a record at the boundary itself whose identity also occurs
// among the dropped records (a CHECKPOINT or COMMON epoch record of a run instance that straddles
// it): its count is spent on occurrences the prefix already covered, and it is dropped. That costs
// that record, not the batch -- the first record whose identity the hub holds nothing of ends the
// probing for good.
func (s *archiveSink) drainBatch() error {
	buf := s.batch
	s.batch, s.batchBytes = nil, 0
	if len(buf) == 0 {
		return nil
	}
	// The probe point is the last buffered record that can be probed exactly (holdsTail). It is
	// normally the last record of the batch; a trailing record that cannot be probed leaves the
	// few records after the probe point to the exact path rather than giving up the skip.
	probe := len(buf) - 1
	for probe >= 0 && !probeable(s.table, buf[probe].Ad) {
		probe--
	}
	exactFrom := 0
	if probe >= 0 {
		s.batchProbes++
		held, err := s.holdsTail(buf[probe])
		if err != nil {
			return err
		}
		if held {
			s.metrics.DedupHits.WithLabelValues(s.table, s.schedd).Add(float64(probe + 1))
			exactFrom = probe + 1
		} else {
			// The hub does not hold the probe point, so the boundary is at or before it and
			// nothing later in this catch-up is held either.
			s.skipAhead = false
		}
	}
	for _, c := range buf[exactFrom:] {
		if !s.pastBoundary {
			held, decided, err := s.held(c)
			if err != nil {
				return err
			}
			if held {
				s.metrics.DedupHits.WithLabelValues(s.table, s.schedd).Inc()
				continue
			}
			// A record with no identity says nothing about where the boundary is: append it, but
			// keep checking the ones that follow.
			if decided {
				s.pastBoundary, s.skipAhead = true, false
			}
		}
		if err := s.appendRecord(c.Ad); err != nil {
			return err
		}
	}
	return nil
}

// holdsTail reports whether the hub already holds the record at the probe point of a skip-ahead
// batch -- the one probe that condemns or spares every record before it.
//
// It may only be wrong in one direction. Saying "not held" of a record the hub holds costs the
// batch its skip: its records go through the exact per-record path, which is what they would have
// got anyway. Saying "held" of a record the hub does NOT hold drops every record before it,
// including new ones past the boundary, and nothing re-delivers them. So the probe asks for more
// than the identity: narrowed by the record's retention timestamp, a false "held" needs a held
// record matching on identity AND timestamp -- a record indistinguishable from this one, not
// merely a sibling of it. Two CHECKPOINT records of one run instance share an identity but not the
// second they were written in.
func (s *archiveSink) holdsTail(c replicate.Change) (bool, error) {
	id, ok := archiveIdentity(s.table, c.Ad)
	if !ok {
		return false, nil
	}
	term, ok := retentionTerm(s.table, c.Ad)
	if !ok {
		return false, nil
	}
	n, err := s.countMatching(id.constraint(s.schedd) + term)
	return n > 0, err
}

// probeable reports whether a record can be probed exactly enough to skip a batch on: it needs an
// identity and the retention timestamp that narrows the probe. A spoke old enough to write epoch
// records without EpochWriteDate has none, and its records are checked one at a time as before.
func probeable(table string, ad *classad.ClassAd) bool {
	if ad == nil {
		return false
	}
	if _, ok := archiveIdentity(table, ad); !ok {
		return false
	}
	_, ok := retentionTerm(table, ad)
	return ok
}

// retentionTerm is the constraint term pinning a record's floorAttr value (EnteredHistoryTime,
// EpochWriteDate for epochs, SampleTime for samples), for the narrowed batch-tail probe. A record
// without that attribute has no term, and does not get skipped by batch.
func retentionTerm(table string, ad *classad.ClassAd) (string, bool) {
	attr := floorAttr(table)
	v, ok := ad.EvaluateAttrNumber(attr)
	if !ok {
		return "", false
	}
	return " && " + attr + " == " + numberLit(v), true
}

// numberLit writes v as a ClassAd literal, as an integer when it is one: the attribute is a
// timestamp, the hub stores it as the spoke wrote it, and an integer literal compares equal to it
// either way (ClassAd == promotes). A value that is not exactly an integer keeps full precision.
func numberLit(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1<<53 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// countHeld counts the hub's records of id for this schedd.
func (s *archiveSink) countHeld(id recordIdentity) (int, error) {
	return s.countMatching(id.constraint(s.schedd))
}

func (s *archiveSink) countMatching(constraint string) (int, error) {
	seq, err := s.arch.Query(constraint)
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
//
// The scan counts straight into s.remaining and the identities already met are set aside first.
// There are at most probeBudget of those (the replay probes one record at a time until the budget
// is spent), so setting them aside is cheap, and the sink never holds a second copy of the whole
// set to copy out of.
//
// The load is bounded by the hub-wide identityBudget, reserved a chunk at a time as the set grows.
// When the budget refuses, the set is abandoned -- what this sink reserved is returned,
// s.remaining goes back to what it was, and the replay returns to one query per record for this
// catch-up -- rather than grown until the daemon is killed.
func (s *archiveSink) loadIdentities() error {
	seq, err := s.arch.QueryProject(scheddConstraint(s.schedd), identityAttrs(s.table))
	if err != nil {
		return err
	}
	met := s.remaining
	loaded := make(map[[16]byte]int, len(met))
	s.remaining = loaded
	for vals := range seq {
		id, ok := identityFromValues(s.table, vals)
		if !ok {
			continue
		}
		loaded[id.digest()]++
		if len(loaded) > s.identityHeld && !s.reserveIdentities() {
			s.releaseIdentities()
			s.remaining, s.setGaveUp = met, true
			s.log.Warn("federate: the hub's identity budget cannot hold this AP's set; probing the replay instead",
				"table", s.table, "schedd", s.schedd, "loaded", len(loaded),
				"budget_identities", s.budget.limit, "approx_bytes_per_identity", bytesPerIdentity)
			return nil
		}
	}
	for d, n := range met {
		loaded[d] = n
	}
	s.setLoaded = true
	s.setLoads++
	s.log.Info("federate: catch-up is a full replay; loaded the hub's identities for it",
		"table", s.table, "schedd", s.schedd, "identities", len(loaded))
	return nil
}

// reserveIdentities takes another chunk of the hub's budget for this sink's set.
func (s *archiveSink) reserveIdentities() bool {
	if !s.budget.reserve(s.identityChunk) {
		return false
	}
	s.identityHeld += s.identityChunk
	return true
}

// releaseIdentities returns this sink's whole reservation. Idempotent: the sink's set is dropped
// at both ends of a catch-up and on give-up, and only the first call gives anything back.
func (s *archiveSink) releaseIdentities() {
	s.budget.release(s.identityHeld)
	s.identityHeld = 0
}

// floorAttr is the time attribute a replayed record is compared to the hub's retained floor by.
func floorAttr(table string) string {
	switch table {
	case TableEpochHistory:
		return "EpochWriteDate"
	case TableJobMetrics:
		return "SampleTime"
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
// capped by segment count or bytes and occupies the band a rotation leaves it in, or it is capped
// by age, which drops whatever ages out.
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
	if r.MaxBytes <= 0 {
		return false
	}
	// A size pass is triggered by the ceiling but frees down to the low mark, so an archive that
	// has evicted sits anywhere in [TargetBytes, MaxBytes] rather than parked against the cap.
	// Occupancy alone therefore no longer says which side of its first eviction an archive is on,
	// and the test has to take the whole band. That over-reports only while an archive grows
	// through the band for the first time, and over-reporting costs at most a replayed record
	// older than anything the hub holds for that AP being skipped rather than appended.
	low := r.TargetBytes
	if low <= 0 || low > r.MaxBytes {
		low = r.MaxBytes
	}
	return st.UsedBytes+st.UsedBytes/int64(st.Segments) > low
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
	// Drain first: a cursor must never be committed over records that are still only buffered.
	// It costs one probe, not one per buffered record, so a flush interval that fires in the
	// middle of a long overlap does not put the catch-up back on the per-record path.
	if err := s.drainBatch(); err != nil {
		return err
	}
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
