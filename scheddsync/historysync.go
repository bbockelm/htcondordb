package scheddsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// bannerPrefix terminates each record in an HTCondor history file: a line like
// "*** Offset = N ClusterId = C ProcId = P ...". The attributes of the completed job
// precede it as old-ClassAd "Attr = Value" lines.
var bannerPrefix = []byte("*** ")

// HistorySync tails a schedd history file and appends each completed job to an archive
// table. It follows the file across rotation (HTCondor renames the current history file
// aside and starts a fresh one), holding the open file which survives the rename, draining
// it, then switching to the new file (detected via os.SameFile).
//
// It resumes durably across restarts: after each batch it records how far into which file it
// read. On restart it re-reads from there and DEDUPS against the archive itself -- an
// appended job is one entry per (ClusterId, ProcId), so any record already present is
// skipped, and once a record is NOT present every later record is new (history is appended
// chronologically), so the dedup check turns off. This makes recovery robust even when the
// file rotated while we were down: it re-walks the rotation chain and simply skips what the
// archive already holds, never missing or duplicating a job.
type HistorySync struct {
	filename string
	archive  *db.ArchiveTable
	interval time.Duration
	log      *slog.Logger
	store    PositionStore

	file         *os.File    // current open handle (survives a rename of filename)
	fi           os.FileInfo // its FileInfo, for SameFile rotation detection
	offset       int64       // bytes consumed from file
	partial      []byte      // buffered bytes of an incomplete trailing record
	dedup        bool        // while true, skip records the archive already holds (recovery)
	started      bool        // whether restore() has run this process
	warnedNoFile bool        // whether we have logged that the history file is absent (log once)
	onResync     func(ResyncEvent)

	// resyncs / lastResync accumulate durability-gap events for the status snapshot; status
	// holds the latest published snapshot read lock-free by Status(). All written only from the
	// sync goroutine.
	resyncs    int64
	lastResync time.Time
	status     atomic.Pointer[SyncStatus]
}

// ResyncEvent reports that a durability gap was detected on restart: the history file the
// syncer was last reading has rotated out of retention entirely, so any completed jobs that
// finished after the last sync but before the oldest record still on disk are permanently
// lost from the source. It fires once, during recovery, so an operator can alert or trigger a
// fuller reconciliation from another source. The syncer still recovers everything that
// remains on disk (deduping against the archive) -- the event flags only the unrecoverable
// hole.
type ResyncEvent struct {
	// Reason is a short, machine-stable code for the gap.
	Reason string
	// OldestAvailableCompletion is the CompletionDate (Unix seconds) of the oldest record
	// still present on disk after the rotation, or 0 if it could not be read. Completed jobs
	// with a CompletionDate before this that were not yet synced are lost.
	OldestAvailableCompletion int64
}

// HistorySyncConfig configures a HistorySync.
type HistorySyncConfig struct {
	Filename     string        // path to the history file (required)
	PollInterval time.Duration // default 200ms
	Logger       *slog.Logger  // default slog.Default()
	// Store, if set, durably records the resume position so a restart resumes instead of
	// re-appending the whole file, recovering across rotation via archive dedup.
	Store PositionStore
	// OnResync, if set, is called once during recovery when the history file the syncer was
	// last reading has rotated out of retention entirely (a durability gap -- completed jobs
	// were lost from the source). Recovery still proceeds against whatever remains on disk.
	OnResync func(ResyncEvent)
}

// historyPos is HistorySync's persisted resume point.
type historyPos struct {
	File   fileIdentity `json:"file"`
	Offset int64        `json:"offset"`
}

// NewHistorySync creates a syncer that appends completed jobs from cfg.Filename to
// archive. The file need not exist yet.
func NewHistorySync(archive *db.ArchiveTable, cfg HistorySyncConfig) *HistorySync {
	interval := cfg.PollInterval
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &HistorySync{filename: cfg.Filename, archive: archive, interval: interval, log: logger, store: cfg.Store, onResync: cfg.OnResync}
}

// Run polls until ctx is cancelled, starting immediately.
func (s *HistorySync) Run(ctx context.Context) error {
	if err := s.Poll(ctx); err != nil {
		s.log.Warn("history initial poll failed", "err", err.Error())
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.close()
			return ctx.Err()
		case <-ticker.C:
			if err := s.Poll(ctx); err != nil {
				s.log.Warn("history poll failed", "err", err.Error())
			}
		}
	}
}

// Poll reads any newly-completed jobs and appends them. Exported for tests.
func (s *HistorySync) Poll(ctx context.Context) error {
	if !s.started {
		if err := s.restore(); err != nil {
			return err
		}
		s.started = true
	}
	// Detect rotation: the path now names a different file than our open handle.
	if s.file != nil {
		if pathFI, err := os.Stat(s.filename); err == nil && !os.SameFile(s.fi, pathFI) {
			s.drainToEOF() // finish the rotated-away file (still readable via the handle)
			s.close()
		}
	}
	if s.file == nil {
		if err := s.openCurrent(0); err != nil {
			if os.IsNotExist(err) {
				// Absent history file is common (no jobs have completed yet, or HISTORY points
				// at the wrong path). Say so once, at info, so a misconfiguration is diagnosable
				// rather than a silent no-op -- the primary cause of "history isn't syncing".
				if !s.warnedNoFile {
					s.warnedNoFile = true
					s.log.Info("scheddsync: history file not present yet; waiting", "file", s.filename)
				}
				return nil // not created yet
			}
			return err
		}
		s.warnedNoFile = false // re-arm so a later disappearance is reported again
	}
	return s.drainToEOF()
}

// restore positions the syncer on startup, deduping against the archive so nothing is missed
// or duplicated, and leaves the current file open at the right offset for live tailing. It has
// four cases:
//
//   - Trusted position (the saved file is still in the chain): drain from it forward -- the
//     fast, common path.
//   - Saved file rotated out of retention: a data-loss gap. Fire a resync event and recover
//     whatever remains on disk.
//   - No usable position but the archive holds prior history (the position store was lost --
//     state dir cleared, format change, ...): re-derive the frontier from the ARCHIVE by
//     walking the chain, skipping files the archive already holds in full. This makes the
//     position store a fast-path hint rather than a hard dependency.
//   - No position and an empty archive: a genuine first run -- tail the current file from its
//     head (the archive is empty, so dedup trips off on the first record); rotated history is
//     not back-filled.
func (s *HistorySync) restore() error {
	s.dedup = true // recovery runs with dedup on until the first record the archive lacks
	if s.store == nil {
		s.dedup = false
		return nil
	}
	blob, ok, err := s.store.Load()
	if err != nil {
		return err
	}
	var pos historyPos
	havePos := false
	if ok {
		if jerr := json.Unmarshal(blob, &pos); jerr == nil {
			havePos = true
		} else {
			s.log.Warn("scheddsync: unreadable saved history position; recovering from the archive", "err", jerr.Error())
		}
	}

	files, ferr := s.historyChain()
	if ferr != nil {
		return ferr
	}
	// Locate our resume file when we have a usable saved position.
	start, found := 0, false
	if havePos {
		for i, f := range files {
			if sameFileIdentity(f.id, pos.File) {
				start, found = i, true
				break
			}
		}
	}

	switch {
	case found:
		s.drainChain(files, start, pos.Offset)
	case havePos && len(files) > 0:
		s.reportRetentionGap(files)
		s.drainChain(files, 0, 0)
	case !havePos && s.archiveNonEmpty():
		// Lost position, non-empty archive: re-derive the frontier from the archive.
		s.drainChain(files, 0, 0)
	default:
		return nil // genuine first run (or nothing on disk): Poll opens the current file at 0
	}

	// Open the current file for tailing. If the saved position IS the current file, resume at
	// its offset; else start at its head (dedup skips anything already archived).
	startOff := int64(0)
	if found && files[start].path == s.filename {
		startOff = pos.Offset
	}
	if oerr := s.openCurrent(startOff); oerr != nil && !os.IsNotExist(oerr) {
		return oerr
	}
	return nil
}

// drainChain replays rotated files [start, len) (the current file is tailed live by the
// caller). firstOffset is the byte offset for the file at index start (a resumed saved file);
// later files start at 0. While deduping, a rotated file whose LAST record the archive already
// holds is skipped WITHOUT scanning it: history is appended chronologically and synced in
// order, so if the last record is present every earlier one is too. This turns re-locating the
// frontier across a deep rotation chain from O(all records) into O(files) for the already-
// synced prefix -- the CompletionDate/last-record fast-skip.
func (s *HistorySync) drainChain(files []historyEntry, start int, firstOffset int64) {
	for i := start; i < len(files); i++ {
		if files[i].path == s.filename {
			continue // the current file is tailed live by the caller
		}
		off := int64(0)
		if i == start {
			off = firstOffset
		}
		if s.dedup && off == 0 && s.fileFullyArchived(files[i].path) {
			continue // entire file already in the archive; skip without scanning
		}
		if derr := s.drainRotatedFile(files[i].path, off); derr != nil {
			s.log.Warn("scheddsync: reading rotated history file failed", "file", files[i].path, "err", derr.Error())
		}
	}
}

// reportRetentionGap emits the resync event for a saved file that rotated out of retention:
// completed jobs after our last sync but before the oldest surviving record are lost.
func (s *HistorySync) reportRetentionGap(files []historyEntry) {
	s.resyncs++
	s.lastResync = nowFn()
	ev := ResyncEvent{Reason: "history-file-rotated-out-of-retention"}
	// Oldest-first, use the first file with a readable record -- skipping any sibling that
	// matched the history.* glob but is not a history file (e.g. a co-located position file).
	for _, f := range files {
		if cd, cok := firstCompletionDate(f.path); cok {
			ev.OldestAvailableCompletion = cd
			break
		}
	}
	s.log.Warn("scheddsync: saved history file rotated out of retention while down; some completed jobs may be missing",
		"oldest_available_completion", ev.OldestAvailableCompletion)
	if s.onResync != nil {
		s.onResync(ev)
	}
}

// archiveNonEmpty reports whether the archive already holds any records (prior history).
func (s *HistorySync) archiveNonEmpty() bool { return s.archive.Count() > 0 }

// fileFullyArchived reports whether a rotated file's LAST record is already in the archive --
// which, because history is appended chronologically and synced in order, means the whole file
// is. A file whose last record cannot be read (unparseable, or beyond the tail window) is
// treated as NOT fully archived, so it is scanned rather than wrongly skipped.
func (s *HistorySync) fileFullyArchived(path string) bool {
	ad, ok := lastRecordAd(path)
	if !ok {
		return false
	}
	return s.alreadyArchived(ad)
}

// historyEntry is one file in the rotation chain.
type historyEntry struct {
	path string
	id   fileIdentity
	mod  time.Time
}

// historyChain returns the current history file plus its rotated siblings (path.*), oldest
// first by modification time, so recovery replays them in append order.
func (s *HistorySync) historyChain() ([]historyEntry, error) {
	matches, _ := filepath.Glob(s.filename + ".*") // rotated files, e.g. history.20260722T...
	paths := append([]string{s.filename}, matches...)
	var out []historyEntry
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			continue // gone between glob and stat, or the current file not created yet
		}
		out = append(out, historyEntry{path: p, id: identityFromInfo(fi), mod: fi.ModTime()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].mod.Before(out[j].mod) })
	return out, nil
}

// firstCompletionDate reads the first complete record of a history file and returns its
// CompletionDate (Unix seconds). Used to quantify a retention gap; ok is false if the file is
// empty, unreadable, or the record lacks the attribute.
func firstCompletionDate(path string) (int64, bool) {
	f, err := os.Open(path) //nolint:gosec // operator-controlled history path
	if err != nil {
		return 0, false
	}
	defer f.Close() //nolint:errcheck
	buf := make([]byte, 64*1024)
	n, _ := io.ReadFull(f, buf)
	data := buf[:n]
	end := bytes.Index(data, append([]byte("\n"), bannerPrefix...))
	if end < 0 {
		return 0, false // no complete record within the first read
	}
	ad, perr := classad.ParseOld(string(data[:end+1]))
	if perr != nil {
		return 0, false
	}
	cd, ok := ad.EvaluateAttrInt("CompletionDate")
	if !ok {
		return 0, false
	}
	return cd, true
}

// lastRecordAd parses the last complete record of a history file (reading only a tail window,
// so it is cheap on a large file). ok is false if the file is empty, the last record is
// unparseable, or a single record does not fit the window. Used to test whether a whole file
// is already archived via its final record.
func lastRecordAd(path string) (*classad.ClassAd, bool) {
	f, err := os.Open(path) //nolint:gosec // operator-controlled history path
	if err != nil {
		return nil, false
	}
	defer f.Close() //nolint:errcheck
	fi, err := f.Stat()
	if err != nil {
		return nil, false
	}
	const window = 256 << 10
	var start int64
	if fi.Size() > window {
		start = fi.Size() - window
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return nil, false
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, false
	}
	needle := append([]byte("\n"), bannerPrefix...) // "\n*** " -- newline before a banner line
	lastNL := bytes.LastIndex(data, needle)         // newline before the final banner
	if lastNL < 0 {
		return nil, false
	}
	// The final record's attributes are the lines between the penultimate banner and the last.
	recStart := 0
	if prevNL := bytes.LastIndex(data[:lastNL], needle); prevNL >= 0 {
		bannerLine := prevNL + 1
		nl := bytes.IndexByte(data[bannerLine:], '\n')
		if nl < 0 {
			return nil, false
		}
		recStart = bannerLine + nl + 1
	} else if start > 0 {
		return nil, false // windowed and only one banner seen: cannot trust the record start
	}
	ad, perr := classad.ParseOld(string(data[recStart : lastNL+1]))
	if perr != nil {
		return nil, false
	}
	return ad, true
}

// drainRotatedFile reads a rotated (no-longer-current) file from off to EOF, appending its
// records; used only during restart recovery.
func (s *HistorySync) drainRotatedFile(path string, off int64) error {
	f, err := os.Open(path) //nolint:gosec // operator-controlled history path
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck
	if off > 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			return err
		}
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	s.partial = nil
	s.processRecords(data)
	s.partial = nil // a rotated file ends at a record boundary; drop any trailing partial
	return nil
}

// openCurrent opens the current history file and seeks to off, resetting the tail state.
func (s *HistorySync) openCurrent(off int64) error {
	f, err := os.Open(s.filename) //nolint:gosec // operator-controlled history path
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	if off > 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			_ = f.Close()
			return err
		}
	}
	s.file, s.fi, s.offset, s.partial = f, fi, off, nil
	return nil
}

// drainToEOF reads all currently-available bytes from the open handle, appends any completed
// records, advances the offset, and checkpoints the position.
func (s *HistorySync) drainToEOF() error {
	data, err := io.ReadAll(s.file)
	if err != nil {
		return err
	}
	if len(data) > 0 {
		s.offset += int64(len(data))
		s.processRecords(data)
		s.checkpoint()
	}
	s.publishStatus(len(data) > 0)
	return nil
}

// processRecords appends new bytes to the partial buffer and appends every complete record
// (terminated by a banner line) to the archive, keeping any incomplete trailing record.
func (s *HistorySync) processRecords(data []byte) {
	buf := append(s.partial, data...)
	var rec []byte
	lineStart, lastComplete := 0, 0
	for i := 0; i < len(buf); i++ {
		if buf[i] != '\n' {
			continue
		}
		line := buf[lineStart:i]
		lineStart = i + 1
		if bytes.HasPrefix(line, bannerPrefix) {
			s.appendRecord(rec)
			rec = rec[:0]
			lastComplete = lineStart
		} else {
			rec = append(rec, line...)
			rec = append(rec, '\n')
		}
	}
	// Retain everything after the last complete record (an incomplete trailing record,
	// including a partial final line the schedd has not finished writing).
	s.partial = append([]byte(nil), buf[lastComplete:]...)
}

// appendRecord parses one record's old-ClassAd text and appends it to the archive. During
// recovery it first skips records the archive already holds; the first record NOT present
// turns dedup off (everything after is new).
func (s *HistorySync) appendRecord(rec []byte) {
	if len(bytes.TrimSpace(rec)) == 0 {
		return
	}
	ad, err := classad.ParseOld(string(rec))
	if err != nil {
		s.log.Warn("history: skipping unparseable record", "err", err.Error())
		return
	}
	if s.dedup {
		if s.alreadyArchived(ad) {
			return // already synced before the crash; skip
		}
		s.dedup = false // first record the archive lacks: caught up, everything after is new
	}
	if err := s.archive.Append(ad); err != nil {
		s.log.Warn("history: append failed", "err", err.Error())
	}
}

// alreadyArchived reports whether a completed job (keyed by ClusterId+ProcId, unique per
// schedd history) is already in the archive.
func (s *HistorySync) alreadyArchived(ad *classad.ClassAd) bool {
	cid, ok1 := ad.EvaluateAttrInt("ClusterId")
	pid, ok2 := ad.EvaluateAttrInt("ProcId")
	if !ok1 || !ok2 {
		return false // no key to dedup on: treat as new
	}
	seq, err := s.archive.QueryLimit(fmt.Sprintf("ClusterId == %d && ProcId == %d", cid, pid), 1)
	if err != nil {
		return false
	}
	for range seq {
		return true
	}
	return false
}

// checkpoint durably records how far into the current file we have consumed complete records.
func (s *HistorySync) checkpoint() {
	if s.store == nil || s.fi == nil {
		return
	}
	pos := historyPos{File: identityFromInfo(s.fi), Offset: s.offset - int64(len(s.partial))}
	blob, err := json.Marshal(pos)
	if err != nil {
		return
	}
	if serr := s.store.Save(blob); serr != nil {
		s.log.Warn("scheddsync: saving history position failed", "err", serr.Error())
	}
}

func (s *HistorySync) close() {
	if s.file != nil {
		_ = s.file.Close()
		s.file, s.fi, s.offset, s.partial = nil, nil, 0, nil
	}
}
