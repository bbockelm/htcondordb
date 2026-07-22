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

	file    *os.File    // current open handle (survives a rename of filename)
	fi      os.FileInfo // its FileInfo, for SameFile rotation detection
	offset  int64       // bytes consumed from file
	partial []byte      // buffered bytes of an incomplete trailing record
	dedup   bool        // while true, skip records the archive already holds (recovery)
	started bool        // whether restore() has run this process
}

// HistorySyncConfig configures a HistorySync.
type HistorySyncConfig struct {
	Filename     string        // path to the history file (required)
	PollInterval time.Duration // default 200ms
	Logger       *slog.Logger  // default slog.Default()
	// Store, if set, durably records the resume position so a restart resumes instead of
	// re-appending the whole file, recovering across rotation via archive dedup.
	Store PositionStore
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
	return &HistorySync{filename: cfg.Filename, archive: archive, interval: interval, log: logger, store: cfg.Store}
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
				return nil // not created yet
			}
			return err
		}
	}
	return s.drainToEOF()
}

// restore loads the persisted position and, if a rotation happened while we were down,
// re-walks the rotation chain (rotated files + the current one) deduping against the archive
// so nothing is missed or duplicated. It leaves the current file open at its end for live
// tailing. With no saved position (first run) it starts at the head of the current file; the
// archive is empty, so the dedup check trips off on the first record.
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
	if !ok {
		return nil // first run: Poll opens the current file at 0; dedup trips off immediately
	}
	var pos historyPos
	if jerr := json.Unmarshal(blob, &pos); jerr != nil {
		s.log.Warn("scheddsync: unreadable saved history position; recovering via dedup", "err", jerr.Error())
		return nil
	}

	files, ferr := s.historyChain()
	if ferr != nil {
		return ferr
	}
	// Find where we left off. If the saved file is still in the chain, replay from it (at the
	// saved offset) forward; otherwise it rotated out of retention -- read what remains and
	// let dedup place us, warning that older completed jobs may have been lost while down.
	start, found := 0, false
	for i, f := range files {
		if sameFileIdentity(f.id, pos.File) {
			start, found = i, true
			break
		}
	}
	if !found && len(files) > 0 {
		s.log.Warn("scheddsync: saved history file rotated out of retention while down; some completed jobs may be missing")
	}
	// Drain every rotated file from the start point up to (but not including) the current one;
	// the current file is opened for live tailing afterward.
	for i := start; i < len(files); i++ {
		if files[i].path == s.filename {
			continue // the current file is tailed live below
		}
		off := int64(0)
		if i == start && found {
			off = pos.Offset
		}
		if derr := s.drainRotatedFile(files[i].path, off); derr != nil {
			s.log.Warn("scheddsync: reading rotated history file failed", "file", files[i].path, "err", derr.Error())
		}
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
	if len(data) == 0 {
		return nil
	}
	s.offset += int64(len(data))
	s.processRecords(data)
	s.checkpoint()
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
