package federate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"

	"github.com/bbockelm/htcondordb/cedarsync"
	"github.com/bbockelm/htcondordb/dbad"
	"github.com/bbockelm/htcondordb/syncstatus"
)

// Defaults for Config's durations.
const (
	DefaultDiscoverInterval = 60 * time.Second
	DefaultStateInterval    = 5 * time.Second
	DefaultFreshThreshold   = 60 * time.Second
	DefaultRetireAfter      = 7 * 24 * time.Hour
)

// Source states, as federation_sources reports them.
const (
	StateFresh     = "fresh"
	StateStale     = "stale"
	StateAbsent    = "absent"
	StateUntrusted = "untrusted"
	StateRetiring  = "retiring"
)

// Config configures a Hub.
type Config struct {
	// Catalog is the hub's local catalog. The hub writes its federated tables in process.
	Catalog *db.Catalog
	// Tables are the spoke tables to federate (DefaultTables when empty). syncstatus is needed for
	// freshness: without it every source reads stale.
	Tables []string
	// Archive tunes the hub's archives.
	Archive ArchiveOptions
	// Discovery finds the AP set's spokes.
	Discovery Discoverer
	// Dial returns a cedarsync.Dial for a spoke address (a fresh authenticated DBSession per call).
	Dial func(address string) cedarsync.Dial
	// CursorDir holds the per-(source, table) resume cursors. Empty keeps them in memory only, so
	// a hub restart replays every source (correct -- the sinks reconcile and deduplicate -- but
	// expensive).
	CursorDir string

	DiscoverInterval time.Duration // DefaultDiscoverInterval when <= 0
	StateInterval    time.Duration // how often states and federation_sources are refreshed
	FlushInterval    time.Duration // cedarsync.DefaultFlushInterval when <= 0
	FreshThreshold   time.Duration // staleness at or under this is fresh
	RetireAfter      time.Duration // unseen (or unmatched) this long => rows deleted

	Metrics *Metrics     // nil makes a private set
	Logger  *slog.Logger // nil discards
	Now     func() time.Time
}

// Hub fans the spokes of an AP set into one catalog. See the package documentation.
type Hub struct {
	cfg     Config
	log     *slog.Logger
	metrics *Metrics
	now     func() time.Time
	tables  []string

	ht *hubTables

	// Everything below is owned by the Run goroutine, except where noted.
	sources       map[string]*source
	discoveryDone bool      // a discovery pass with a known match set has completed in this run
	runStart      time.Time // when this Run started: unseen time accrues only while the hub runs
	runCtx        context.Context
	retireReq     chan retireReq
	discoverNow   chan struct{}

	summary atomic.Pointer[dbad.Federation]

	resetMu    sync.Mutex
	resetTimes map[string]time.Time // schedd -> last Reset received (written by runner goroutines)
}

// source is the hub's state for one schedd.
type source struct {
	schedd            string
	spokeAddress      string
	spokeName         string
	static            bool // configured in HTCONDORDB_FEDERATE_SPOKES now
	droppedStatic     bool // was static, is no longer, and has not been matched since
	inSet             bool // matched by the last discovery with a known match set
	untrusted         string
	declined          string
	unverified        string   // why a restored spoke address is not dialed until discovery pairs it
	spellings         []string // every spelling of the name seen, for cursors kept by spelling
	lastCollectorSeen time.Time
	lastContact       time.Time
	lastReset         time.Time
	retiringSince     time.Time
	rows              map[string]int64
	runners           map[string]*runnerHandle
	lastRow           *classad.ClassAd
}

type runnerHandle struct {
	runner  *cedarsync.Runner
	address string
	cancel  context.CancelFunc
	done    chan struct{}

	// The runner status last observed by refreshState: a change in either means the spoke was
	// reached since, even if the session has dropped again by the time of the tick.
	seenSessions int
	seenEvent    time.Time
}

type retireReq struct {
	schedd string
	resp   chan error
}

// New validates cfg and builds a Hub. Run creates the tables and starts replicating.
func New(cfg Config) (*Hub, error) {
	if cfg.Catalog == nil || cfg.Discovery == nil || cfg.Dial == nil {
		return nil, errors.New("federate: Catalog, Discovery and Dial are required")
	}
	tables := cfg.Tables
	if len(tables) == 0 {
		tables = DefaultTables
	}
	for _, t := range tables {
		switch t {
		case TableJobs, TableHistory, TableEpochHistory, TableSyncStatus:
		default:
			return nil, fmt.Errorf("federate: cannot federate table %q (want jobs, history, epoch_history, syncstatus)", t)
		}
	}
	h := &Hub{
		cfg: cfg, log: cfg.Logger, metrics: cfg.Metrics, now: cfg.Now, tables: tables,
		sources: map[string]*source{}, retireReq: make(chan retireReq), discoverNow: make(chan struct{}, 1),
		resetTimes: map[string]time.Time{},
	}
	if h.log == nil {
		h.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if h.metrics == nil {
		h.metrics = NewMetrics()
	}
	if h.now == nil {
		h.now = time.Now
	}
	if !containsTable(tables, TableSyncStatus) {
		h.log.Warn("federate: syncstatus is not federated; no source can be measured fresh", "tables", tables)
	}
	return h, nil
}

func containsTable(tables []string, t string) bool {
	for _, x := range tables {
		if x == t {
			return true
		}
	}
	return false
}

func orDur(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}

// Summary returns the latest source summary for the hub's collector ad (nil before the first
// state pass).
func (h *Hub) Summary() *dbad.Federation { return h.summary.Load() }

// Metrics returns the hub's metric set.
func (h *Hub) Metrics() *Metrics { return h.metrics }

// Retire deletes schedd's rows from the hub's mutable tables (jobs, syncstatus,
// federation_sources) and forgets its cursors. Archives are append-only and are left to age out.
// If the schedd is still in the AP set and its spoke still advertises, the next discovery adds it
// back and replays it from scratch.
func (h *Hub) Retire(ctx context.Context, schedd string) error {
	req := retireReq{schedd: schedd, resp: make(chan error, 1)}
	select {
	case h.retireReq <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Rediscover asks Run to run a discovery pass now instead of at the next interval.
func (h *Hub) Rediscover() {
	select {
	case h.discoverNow <- struct{}{}:
	default:
	}
}

// Run replicates until ctx is cancelled. It returns only after every runner and background index
// backfill has stopped, so the caller may close the catalog afterwards.
func (h *Hub) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	ht, err := ensureTables(h.cfg.Catalog, h.tables, h.cfg.Archive, h.log, &wg)
	if err != nil {
		return err
	}
	h.ht = ht
	h.runCtx = ctx
	h.runStart = h.now()
	defer h.stopAll()

	h.loadSources()
	h.revalidateRestored(ctx)
	for _, s := range h.sources {
		if s.retiringSince.IsZero() && s.spokeAddress != "" {
			h.startRunners(s)
		}
	}
	h.refreshState()

	discover := time.NewTicker(orDur(h.cfg.DiscoverInterval, DefaultDiscoverInterval))
	defer discover.Stop()
	state := time.NewTicker(orDur(h.cfg.StateInterval, DefaultStateInterval))
	defer state.Stop()

	h.discover(ctx)
	h.refreshState()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-discover.C:
			h.discover(ctx)
			h.refreshState()
		case <-h.discoverNow:
			h.discover(ctx)
			h.refreshState()
		case <-state.C:
			h.refreshState()
		case req := <-h.retireReq:
			req.resp <- h.retire(req.schedd, "admin request")
			h.refreshState()
		}
	}
}

// discover runs one discovery pass and applies it to the source set.
func (h *Hub) discover(ctx context.Context) {
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	snap, err := h.cfg.Discovery.Discover(dctx)
	if err != nil {
		h.log.Warn("federate: discovery failed; keeping the current source set", "err", err.Error())
		return
	}
	now := h.now()
	snap, names := foldSnapshot(snap)
	for _, r := range snap.Rejected {
		h.metrics.RejectedSpokes.WithLabelValues(r.Reason).Inc()
		h.log.Warn("federate: spoke rejected", "schedd", r.Schedd, "spoke", r.SpokeName,
			"address", r.SpokeAddress, "reason", r.Reason, "detail", r.Detail)
	}

	// The static set is configuration, known on every pass: each source's Static flag is the
	// current configuration's (a schedd moved from the static list to the constraint is no longer
	// static, so a later collector outage cannot read as "removed from the static list").
	static := map[string]bool{}
	for schedd, sp := range snap.Spokes {
		if sp.Static {
			static[schedd] = true
		}
	}
	for schedd, s := range h.sources {
		if s.static && !static[schedd] {
			s.droppedStatic = true // left the static list; leaving the set unless the constraint matches it
		}
		s.static = static[schedd]
	}

	// Schedds in the AP set: the static spokes always, the constraint's matches when known.
	for key := range static {
		h.admit(key, names[key], snap, now)
	}
	if snap.MatchKnown {
		for key := range snap.Matched {
			if !static[key] {
				h.admit(key, names[key], snap, now)
			}
		}
		h.discoveryDone = true
	}

	// Members not in the AP set this pass.
	for schedd, s := range h.sources {
		if !snap.MatchKnown || snap.Matched[schedd] {
			continue
		}
		s.inSet = false
		leaving := s.droppedStatic || (snap.PresentKnown && snap.Present[schedd])
		if leaving && s.retiringSince.IsZero() {
			why := "the schedd no longer matches the constraint"
			if s.droppedStatic {
				why = "the static spoke was removed from HTCONDORDB_FEDERATE_SPOKES"
			}
			h.log.Warn("federate: source leaving the AP set; retiring", "schedd", schedd, "reason", why,
				"deleted_after", orDur(h.cfg.RetireAfter, DefaultRetireAfter).String())
			s.retiringSince = now
			h.stopRunners(s)
		}
	}
	h.countRows()
}

// admit applies one pass's view of a schedd in the AP set. key is its folded name (snap is folded)
// and schedd its spelling.
func (h *Hub) admit(key, schedd string, snap Snapshot, now time.Time) {
	s := h.source(schedd)
	s.noteSpelling(schedd)
	s.inSet = true
	s.droppedStatic = false
	s.lastCollectorSeen = now
	if !s.retiringSince.IsZero() {
		h.log.Info("federate: source matches again; no longer retiring", "schedd", schedd)
		s.retiringSince = time.Time{}
	}
	s.untrusted, s.declined = snap.Untrusted[key], snap.Declined[key]
	if sp, ok := snap.Spokes[key]; ok {
		s.static, s.spokeName = sp.Static, sp.SpokeName
		s.unverified = "" // paired by this pass
		if sp.Address != s.spokeAddress {
			if s.spokeAddress != "" {
				h.log.Info("federate: spoke address changed; restarting runners", "schedd", schedd,
					"from", s.spokeAddress, "to", sp.Address)
			}
			h.stopRunners(s)
			s.spokeAddress = sp.Address
		}
	}
	// A schedd whose spoke ad is missing, untrusted or declined keeps streaming from the last
	// validated address, if it has one: membership is sticky.
	if s.spokeAddress != "" {
		h.startRunners(s)
	}
}

// source returns (creating if needed) schedd's state. Schedd names are case-insensitive, as
// HTCondor's are: every spelling of one name is one source, which keeps the spelling it was first
// known by.
func (h *Hub) source(schedd string) *source {
	key := foldName(schedd)
	s, ok := h.sources[key]
	if !ok {
		s = &source{schedd: schedd, runners: map[string]*runnerHandle{}, rows: map[string]int64{}}
		h.sources[key] = s
		h.log.Info("federate: new source", "schedd", schedd)
	}
	return s
}

// noteSpelling records a spelling of s's name.
func (s *source) noteSpelling(name string) {
	if !slices.Contains(s.spellings, name) {
		s.spellings = append(s.spellings, name)
	}
}

// legacyCursorDirs are the cursor directories a hub before case folding kept for s's spellings.
func (h *Hub) legacyCursorDirs(s *source) []string {
	var out []string
	for _, n := range append([]string{s.schedd}, s.spellings...) {
		if d := h.legacyCursorDir(n); d != "" && !slices.Contains(out, d) {
			out = append(out, d)
		}
	}
	return out
}

// foldName is the case-folded schedd name the hub keys sources, federation_sources rows and cursor
// directories by.
func foldName(schedd string) string { return strings.ToLower(schedd) }

// foldSnapshot rekeys snap by folded schedd name, merging case variants of one name (the first
// spelling in sorted order wins, so the choice is stable), and returns the spelling of each key.
func foldSnapshot(snap Snapshot) (Snapshot, map[string]string) {
	names := map[string]string{}
	note := func(n string) string {
		k := foldName(n)
		if cur, ok := names[k]; !ok || n < cur {
			names[k] = n
		}
		return k
	}
	foldBool := func(m map[string]bool) map[string]bool {
		out := make(map[string]bool, len(m))
		for n, v := range m {
			if v {
				out[note(n)] = true
			}
		}
		return out
	}
	foldStr := func(m map[string]string) map[string]string {
		out := make(map[string]string, len(m))
		for n, v := range m {
			out[note(n)] = v
		}
		return out
	}
	out := snap
	out.Matched, out.Present = foldBool(snap.Matched), foldBool(snap.Present)
	out.Untrusted, out.Declined = foldStr(snap.Untrusted), foldStr(snap.Declined)
	out.Spokes = make(map[string]Spoke, len(snap.Spokes))
	spokeNames := make([]string, 0, len(snap.Spokes))
	for n := range snap.Spokes {
		spokeNames = append(spokeNames, n)
	}
	sort.Strings(spokeNames)
	for _, n := range spokeNames {
		k := note(n)
		// A static spoke wins over a discovered one for the same name; else the first spelling.
		if cur, ok := out.Spokes[k]; !ok || (snap.Spokes[n].Static && !cur.Static) {
			out.Spokes[k] = snap.Spokes[n]
		}
	}
	return out, names
}

// revalidateRestored checks each restored spoke address before anything dials it: the persisted
// row is only a record of an earlier pairing (made under an earlier configuration, or written by
// something other than the hub), and the hub streams its data under a real AP's name. An address
// that fails is kept but not dialed until a discovery pass pairs the schedd with a spoke again.
// Without a RestoreValidator nothing restored is dialed before discovery.
func (h *Hub) revalidateRestored(ctx context.Context) {
	v, _ := h.cfg.Discovery.(RestoreValidator)
	for _, s := range h.sources {
		if s.spokeAddress == "" || !s.retiringSince.IsZero() {
			continue
		}
		reason, ok := "no validator for persisted spoke addresses", false
		if v != nil {
			vctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			reason, ok = v.ValidateRestored(vctx, s.schedd, s.spokeAddress)
			cancel()
		}
		if !ok {
			s.unverified = "persisted spoke address " + s.spokeAddress + " not revalidated (" + reason + "); waiting for discovery"
			h.log.Warn("federate: not resuming a persisted spoke address until discovery pairs it again",
				"schedd", s.schedd, "spoke", s.spokeAddress, "reason", reason)
		}
	}
}

// startRunners starts any missing runner for s, one per federated table. A source whose address
// is unverified streams nothing.
func (h *Hub) startRunners(s *source) {
	if s.unverified != "" {
		return
	}
	for _, table := range h.tables {
		if rh, ok := s.runners[table]; ok && rh.address == s.spokeAddress {
			continue
		}
		if err := h.startRunner(s, table); err != nil {
			h.log.Error("federate: cannot start runner", "schedd", s.schedd, "table", table, "err", err.Error())
		}
	}
}

func (h *Hub) startRunner(s *source, table string) error {
	store, err := h.cursorStore(s, table)
	if err != nil {
		return err
	}
	schedd, key := s.schedd, foldName(s.schedd)
	onReset := func() {
		h.resetMu.Lock()
		h.resetTimes[key] = h.now()
		h.resetMu.Unlock()
	}
	var sink replicate.Sink
	if isArchiveTable(table) {
		sink, err = newArchiveSink(h.ht.archives[table], table, schedd, store, h.metrics, h.log, onReset)
	} else {
		sink, err = newTableSink(h.ht.mutable[table], table, schedd, store, h.metrics, h.now, onReset)
	}
	if err != nil {
		return err
	}
	r, err := cedarsync.NewRunner(h.cfg.Dial(s.spokeAddress), cedarsync.Config{
		Source: table, Src: schedd, FlushInterval: h.cfg.FlushInterval,
	}, sink, h.log)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(h.runCtx)
	rh := &runnerHandle{runner: r, address: s.spokeAddress, cancel: cancel, done: make(chan struct{})}
	s.runners[table] = rh
	go func() {
		defer close(rh.done)
		_ = r.Run(ctx)
	}()
	h.log.Info("federate: replicating", "schedd", schedd, "table", table, "spoke", s.spokeAddress)
	return nil
}

func (h *Hub) stopRunners(s *source) {
	for table, rh := range s.runners {
		rh.cancel()
		<-rh.done
		delete(s.runners, table)
	}
}

func (h *Hub) stopAll() {
	for _, s := range h.sources {
		h.stopRunners(s)
	}
}

// cursorDir is the per-schedd cursor directory: a readable prefix of the folded name plus a hash,
// so any schedd name maps to one safe directory, the same for every spelling of it.
func (h *Hub) cursorDir(schedd string) string {
	return h.cursorDirOf(foldName(schedd))
}

// legacyCursorDir is where a hub before case folding kept schedd's cursors: named by its spelling.
// "" when that is the folded directory.
func (h *Hub) legacyCursorDir(schedd string) string {
	if schedd == foldName(schedd) {
		return ""
	}
	return h.cursorDirOf(schedd)
}

func (h *Hub) cursorDirOf(schedd string) string {
	if h.cfg.CursorDir == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(schedd))
	var b strings.Builder
	for _, r := range schedd {
		if b.Len() >= 48 {
			break
		}
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return filepath.Join(h.cfg.CursorDir, b.String()+"-"+hex.EncodeToString(sum[:6]))
}

func (h *Hub) cursorStore(s *source, table string) (replicate.CursorStore, error) {
	schedd := s.schedd
	dir := h.cursorDir(schedd)
	if dir == "" {
		return &replicate.MemCursorStore{}, nil
	}
	// A source named with capitals has cursors under its spelling from before case folding. They
	// are dropped, not adopted: the rows they cover are stored under the earlier key encoding, and
	// only a full replay -- whose Reset sweep deletes what it does not re-deliver -- moves them.
	for _, legacy := range h.legacyCursorDirs(s) {
		if _, err := os.Stat(legacy); err == nil {
			h.log.Warn("federate: dropping cursors kept under a case-sensitive name; the source replays once",
				"schedd", schedd, "dir", legacy)
			if err := removeDirDurably(legacy); err != nil {
				return nil, err
			}
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return fileCursorStore{path: filepath.Join(dir, table+".cursor")}, nil
}

// retire deletes schedd's rows from the mutable tables and forgets it.
//
// The cursors go first, durably, and a failure to remove them fails the retirement with nothing
// deleted. In the other order a source whose cursors survived (a removal error, or a crash between
// the steps) would resume from them when rediscovered and never re-send the rows just deleted;
// in this order the worst case is rows kept with no cursor, which the next session's full replay
// reconciles.
func (h *Hub) retire(schedd, why string) error {
	key := foldName(schedd)
	s, known := h.sources[key]
	if known {
		h.stopRunners(s)
		schedd = s.schedd
	}
	// Every spelling's row (ScheddName == is case-insensitive), including rows a hub before case
	// folding keyed by spelling.
	hasRow := false
	if seq, err := h.ht.sources.Query(scheddConstraint(schedd)); err == nil {
		for range seq {
			hasRow = true
			break
		}
	}
	if !known && !hasRow {
		return fmt.Errorf("no federated source named %q", schedd)
	}
	dirs := []string{h.cursorDir(schedd), h.legacyCursorDir(schedd)}
	if known {
		dirs = append(dirs, h.legacyCursorDirs(s)...)
	}
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		if err := removeDirDurably(dir); err != nil {
			return fmt.Errorf("federate: retiring %s: removing its cursors (no rows deleted): %w", schedd, err)
		}
	}
	deleted := map[string]int{}
	for table, d := range h.ht.mutable {
		n, err := d.DeleteWhere(scheddConstraint(schedd))
		if err != nil {
			return fmt.Errorf("federate: retiring %s: deleting its %s rows: %w", schedd, table, err)
		}
		deleted[table] = n
	}
	if hasRow {
		if _, err := h.ht.sources.DeleteWhere(scheddConstraint(schedd)); err != nil {
			return fmt.Errorf("federate: retiring %s: %w", schedd, err)
		}
	}
	delete(h.sources, key)
	h.resetMu.Lock()
	delete(h.resetTimes, key)
	h.resetMu.Unlock()
	h.metrics.Retired.Inc()
	h.log.Warn("federate: source retired", "schedd", schedd, "reason", why, "deleted_rows", fmt.Sprint(deleted),
		"note", "archive rows are kept and age out with retention")
	return nil
}

// removeDirDurably removes dir and everything in it, then syncs its parent so the removal survives
// an OS crash. A dir that does not exist is already removed.
func removeDirDurably(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	parent, err := os.Open(filepath.Dir(dir))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = parent.Close() }()
	return parent.Sync()
}

// staleness computes the upper bound on how far the hub's copy of an AP is behind its schedd,
// from the hub's syncstatus row for it, using only the hub's clock:
//
//	(hub_now - HubReceivedTime) + SpokeLagSeconds + HeartbeatIntervalSeconds
//
// ok is false with no heartbeat yet, or a heartbeat whose lag the spoke could not measure: unknown
// is never fresh.
func staleness(row *classad.ClassAd, now time.Time) (int64, bool) {
	if row == nil {
		return 0, false
	}
	recv, ok1 := row.EvaluateAttrInt(HubReceivedTimeAttr)
	lag, ok2 := row.EvaluateAttrInt(syncstatus.AttrSpokeLagSeconds)
	if !ok1 || !ok2 {
		return 0, false
	}
	if lag < 0 {
		return 0, false // a spoke never reports one; reading it as 0 would be fresh
	}
	iv, ok := row.EvaluateAttrInt(syncstatus.AttrHeartbeatInterval)
	if !ok || iv <= 0 {
		iv = int64(syncstatus.DefaultInterval / time.Second)
	}
	since := now.Unix() - recv
	if since < 0 {
		// Stamped by this hub's clock, so only a backward step of it gets here. "Received just
		// now" would read a dead spoke fresh for as long as the step: unknown instead.
		return 0, false
	}
	return min(since, maxStaleness) + min(lag, maxStaleness) + min(iv, maxStaleness), true
}

// maxStaleness caps each term of staleness (about 100 years), so a nonsense spoke value cannot
// overflow the sum into a negative -- fresh -- staleness.
const maxStaleness = 100 * 365 * 24 * 3600

// refreshState recomputes every source's state, persists changed federation_sources rows, retires
// what is due, and publishes the summary and metrics.
func (h *Hub) refreshState() {
	now := h.now()
	retireAfter := orDur(h.cfg.RetireAfter, DefaultRetireAfter)
	fresh := int64(orDur(h.cfg.FreshThreshold, DefaultFreshThreshold) / time.Second)

	// Contact first: a spoke reached since the last tick is seen now, whatever retirement says.
	for _, s := range h.sources {
		if h.observeRunners(s) {
			s.lastContact = now
		}
	}

	// Retirement next, so a retired source is not written back. Nothing retires before this run's
	// first discovery pass: until then the hub knows only what it persisted.
	for schedd, s := range h.sources {
		switch {
		case !h.discoveryDone:
		case !s.retiringSince.IsZero() && now.Sub(s.retiringSince) >= retireAfter:
			_ = h.logRetire(schedd, fmt.Sprintf("retiring since %s", s.retiringSince.UTC().Format(time.RFC3339)))
		case !s.inSet && s.retiringSince.IsZero() && h.unseenFor(s, now) >= retireAfter && h.attempted(s):
			_ = h.logRetire(schedd, fmt.Sprintf("unseen since %s", lastSeen(s).UTC().Format(time.RFC3339)))
		}
	}

	sum := &dbad.Federation{Constraint: h.cfg.Discovery.Constraint()}
	byState := map[string]float64{StateFresh: 0, StateStale: 0, StateAbsent: 0, StateUntrusted: 0, StateRetiring: 0}
	known := map[string]float64{}
	var hb *classadLookup
	if d, ok := h.ht.mutable[TableSyncStatus]; ok {
		hb = &classadLookup{d.LookupClassAd}
	}
	h.resetMu.Lock()
	resets := make(map[string]time.Time, len(h.resetTimes))
	for k, v := range h.resetTimes {
		resets[k] = v
	}
	h.resetMu.Unlock()

	for schedd, s := range h.sources {
		if t, ok := resets[schedd]; ok && t.After(s.lastReset) {
			s.lastReset = t
		}
		var stale int64
		staleKnown := false
		if hb != nil {
			if row, ok := hb.lookup(HubKey(schedd, syncstatus.Key)); ok {
				stale, staleKnown = staleness(row, now)
			}
		}
		state, reason := h.stateOf(s, stale, staleKnown, fresh)
		byState[state]++
		sum.Total++
		switch state {
		case StateFresh:
			sum.Fresh++
		case StateStale:
			sum.Stale++
		case StateAbsent:
			sum.Absent++
		case StateUntrusted:
			sum.Untrusted++
		case StateRetiring:
			sum.Retiring++
		}
		if staleKnown && state != StateRetiring {
			known[schedd] = float64(stale)
			if !sum.MaxStalenessKnown || stale > sum.MaxStaleness {
				sum.MaxStaleness, sum.MaxStalenessKnown = stale, true
			}
		}
		h.writeSourceRow(s, state, reason, stale, staleKnown)
	}
	for st, n := range byState {
		h.metrics.SourcesByState.WithLabelValues(st).Set(n)
	}
	h.metrics.setStaleness(known)
	h.summary.Store(sum)
}

func (h *Hub) logRetire(schedd, why string) error {
	if err := h.retire(schedd, why); err != nil {
		h.log.Error("federate: retirement failed", "schedd", schedd, "err", err.Error())
		return err
	}
	return nil
}

type classadLookup struct {
	lookup func(string) (*classad.ClassAd, bool)
}

// observeRunners reports whether s's spoke was reached since the last call: a session open now,
// or one opened or an event applied since -- a session that connected and dropped between two
// ticks is contact too.
func (h *Hub) observeRunners(s *source) bool {
	reached := false
	for _, rh := range s.runners {
		st := rh.runner.Status()
		if st.Connected || st.Sessions != rh.seenSessions || !st.LastEvent.Equal(rh.seenEvent) {
			reached = true
		}
		rh.seenSessions, rh.seenEvent = st.Sessions, st.LastEvent
	}
	return reached
}

// unseenFor is how long s has gone unseen while this hub was running. Time the hub was down does
// not count: the hub could not see anything then, and a source seen last before a long hub outage
// is not thereby gone (its spoke may be up and reachable, and the collector restarting empty).
func (h *Hub) unseenFor(s *source, now time.Time) time.Duration {
	since := lastSeen(s)
	if since.Before(h.runStart) {
		since = h.runStart
	}
	return now.Sub(since)
}

// attempted reports whether every runner of s has tried its spoke at least once in this run, so a
// reachable spoke has had its chance to count as contact before s can be retired as unseen.
func (h *Hub) attempted(s *source) bool {
	for _, rh := range s.runners {
		st := rh.runner.Status()
		if st.Sessions == 0 && st.LastErrorTime.IsZero() {
			return false
		}
	}
	return true
}

func lastSeen(s *source) time.Time {
	if s.lastContact.After(s.lastCollectorSeen) {
		return s.lastContact
	}
	return s.lastCollectorSeen
}

// stateOf decides a source's state. Precedence: retiring, then absent (not in the AP set), then a
// restored address not yet revalidated (untrusted), then absent/untrusted for a schedd with no
// paired spoke, then fresh/stale by measured staleness.
func (h *Hub) stateOf(s *source, stale int64, staleKnown bool, fresh int64) (string, string) {
	switch {
	case !s.retiringSince.IsZero():
		return StateRetiring, "left the AP set; rows are deleted when the retirement delay passes"
	case h.discoveryDone && !s.inSet:
		return StateAbsent, "not in the AP set at the last discovery; rows kept"
	case s.unverified != "":
		return StateUntrusted, s.unverified
	case s.spokeAddress == "" && s.untrusted != "":
		return StateUntrusted, s.untrusted
	case s.spokeAddress == "" && s.declined != "":
		return StateAbsent, s.declined
	case s.spokeAddress == "":
		return StateAbsent, "no spoke advertises MirroredScheddName for this schedd"
	case !staleKnown:
		return StateStale, "no heartbeat with a measured lag received yet"
	case stale <= fresh:
		return StateFresh, ""
	default:
		return StateStale, fmt.Sprintf("staleness %ds exceeds %ds", stale, fresh)
	}
}

// Table attribute prefixes in federation_sources.
func tablePrefix(table string) string {
	switch table {
	case TableJobs:
		return "Jobs"
	case TableHistory:
		return "History"
	case TableEpochHistory:
		return "EpochHistory"
	case TableSyncStatus:
		return "SyncStatus"
	}
	return table
}

// writeSourceRow persists s's federation_sources row when it changed.
func (h *Hub) writeSourceRow(s *source, state, reason string, stale int64, staleKnown bool) {
	ad := classad.New()
	ad.InsertAttrString(ScheddNameAttr, s.schedd)
	ad.InsertAttrString("State", state)
	if reason != "" {
		ad.InsertAttrString("Reason", reason)
	}
	if staleKnown {
		ad.InsertAttr("StalenessSeconds", stale)
	}
	putTime := func(name string, t time.Time) {
		if !t.IsZero() {
			ad.InsertAttr(name, t.Unix())
		}
	}
	putTime("LastSeen", lastSeen(s))
	putTime("LastCollectorSeen", s.lastCollectorSeen)
	putTime("LastContact", s.lastContact)
	putTime("LastReset", s.lastReset)
	putTime("RetiringSince", s.retiringSince)
	if s.spokeAddress != "" {
		ad.InsertAttrString("SpokeAddress", s.spokeAddress)
	}
	if s.spokeName != "" {
		ad.InsertAttrString("SpokeName", s.spokeName)
	}
	ad.InsertAttrBool("Static", s.static)
	tables := append([]string(nil), h.tables...)
	sort.Strings(tables)
	for _, t := range tables {
		p := tablePrefix(t)
		rh, ok := s.runners[t]
		var st cedarsync.Status
		if ok {
			st = rh.runner.Status()
		}
		ad.InsertAttrBool(p+"Connected", st.Connected)
		if st.LastError != "" {
			ad.InsertAttrString(p+"LastError", st.LastError)
			putTime(p+"LastErrorTime", st.LastErrorTime)
		}
		if n, ok := s.rows[t]; ok {
			ad.InsertAttr(p+"Rows", n)
		}
	}
	if s.lastRow != nil && s.lastRow.Equal(ad) {
		return
	}
	tx := h.ht.sources.Begin()
	tx.NewClassAd(foldName(s.schedd), ad)
	if err := tx.Commit(); err != nil {
		h.log.Warn("federate: writing federation_sources row failed", "schedd", s.schedd, "err", err.Error())
		return
	}
	s.lastRow = ad
}

// loadSources restores the persisted source set, so a hub restarting while the collector is empty
// still knows every member, its last validated spoke address and when it was last seen.
//
// Rows are keyed by folded name. A hub before case folding keyed them by spelling, so one schedd
// may have several: they are merged into one source (its latest times win) and the rows under
// other keys are deleted when it is written back.
func (h *Hub) loadSources() {
	var spellings []string
	h.ht.sources.ForEach(func(ad *classad.ClassAd) bool {
		name, _ := ad.EvaluateAttrString(ScheddNameAttr)
		if name == "" {
			return true
		}
		if name != foldName(name) {
			spellings = append(spellings, name)
		}
		s := &source{schedd: name, runners: map[string]*runnerHandle{}, rows: map[string]int64{}}
		s.spokeAddress, _ = ad.EvaluateAttrString("SpokeAddress")
		s.spokeName, _ = ad.EvaluateAttrString("SpokeName")
		s.static, _ = ad.EvaluateAttrBool("Static")
		getTime := func(n string) time.Time {
			if v, ok := ad.EvaluateAttrInt(n); ok && v > 0 {
				return time.Unix(v, 0)
			}
			return time.Time{}
		}
		s.lastCollectorSeen = getTime("LastCollectorSeen")
		s.lastContact = getTime("LastContact")
		s.lastReset = getTime("LastReset")
		s.retiringSince = getTime("RetiringSince")
		if seen := getTime("LastSeen"); seen.After(lastSeen(s)) {
			s.lastCollectorSeen = seen
		}
		for _, t := range h.tables {
			if n, ok := ad.EvaluateAttrInt(tablePrefix(t) + "Rows"); ok {
				s.rows[t] = n
			}
		}
		key := foldName(name)
		s.noteSpelling(name)
		if prev, ok := h.sources[key]; ok {
			spellings := append(prev.spellings, name)
			s = mergeRestored(prev, s)
			s.spellings = spellings
		}
		h.sources[key] = s
		return true
	})
	if len(spellings) > 0 {
		tx := h.ht.sources.Begin()
		for _, n := range spellings {
			tx.DestroyClassAd(n) // rewritten under the folded key by the next state pass
		}
		if err := tx.Commit(); err != nil {
			h.log.Warn("federate: removing case-variant federation_sources rows", "err", err.Error())
		}
	}
	if len(h.sources) > 0 {
		h.log.Info("federate: restored persisted sources", "count", len(h.sources))
	}
}

// mergeRestored merges two persisted rows of one schedd (case variants from before case folding):
// the later of each time, and the pairing from the more recently contacted.
func mergeRestored(a, b *source) *source {
	keep, other := a, b
	if b.lastContact.After(a.lastContact) {
		keep, other = b, a
	}
	later := func(x, y time.Time) time.Time {
		if y.After(x) {
			return y
		}
		return x
	}
	keep.lastCollectorSeen = later(keep.lastCollectorSeen, other.lastCollectorSeen)
	keep.lastReset = later(keep.lastReset, other.lastReset)
	if keep.retiringSince.IsZero() || other.retiringSince.IsZero() {
		keep.retiringSince = time.Time{}
	}
	keep.static = keep.static || other.static
	return keep
}

// countRows refreshes per-source row counts where an index can answer them cheaply. A count the
// index cannot answer is left out rather than computed by a scan.
func (h *Hub) countRows() {
	for schedd, s := range h.sources {
		c := scheddConstraint(schedd)
		for table, d := range h.ht.mutable {
			if n, ok := d.CountConstraint(c); ok {
				s.rows[table] = int64(n)
			}
		}
		for table, a := range h.ht.archives {
			if n, ok := a.CountConstraint(c); ok {
				s.rows[table] = int64(n)
			}
		}
	}
}
