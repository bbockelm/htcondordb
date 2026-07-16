package scheddsync

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// bannerPrefix terminates each record in an HTCondor history file: a line like
// "*** Offset = N ClusterId = C ProcId = P ...". The attributes of the completed job
// precede it as old-ClassAd "Attr = Value" lines.
var bannerPrefix = []byte("*** ")

// HistorySync tails a schedd history file and appends each completed job to an archive
// table. It follows the file across rotation: HTCondor renames the current history file
// aside and starts a fresh one, so the syncer holds the open file (which survives the
// rename), drains it, then switches to the new file (detected via os.SameFile).
type HistorySync struct {
	filename string
	archive  *db.ArchiveTable
	interval time.Duration
	log      *slog.Logger

	file    *os.File    // current open handle (survives a rename of filename)
	fi      os.FileInfo // its FileInfo, for SameFile rotation detection
	partial []byte      // buffered bytes of an incomplete trailing record
}

// HistorySyncConfig configures a HistorySync.
type HistorySyncConfig struct {
	Filename     string        // path to the history file (required)
	PollInterval time.Duration // default 200ms
	Logger       *slog.Logger  // default slog.Default()
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
	return &HistorySync{filename: cfg.Filename, archive: archive, interval: interval, log: logger}
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
	// Detect rotation: the path now names a different file than our open handle.
	if s.file != nil {
		if pathFI, err := os.Stat(s.filename); err == nil && !os.SameFile(s.fi, pathFI) {
			s.drainToEOF() // finish the rotated-away file (still readable via the handle)
			s.close()
		}
	}
	if s.file == nil {
		f, err := os.Open(s.filename)
		if err != nil {
			if os.IsNotExist(err) {
				return nil // not created yet
			}
			return err
		}
		fi, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}
		s.file, s.fi, s.partial = f, fi, nil
	}
	return s.drainToEOF()
}

// drainToEOF reads all currently-available bytes from the open handle and appends any
// completed records.
func (s *HistorySync) drainToEOF() error {
	data, err := io.ReadAll(s.file)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	s.processRecords(data)
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

// appendRecord parses one record's old-ClassAd text and appends it to the archive.
func (s *HistorySync) appendRecord(rec []byte) {
	if len(bytes.TrimSpace(rec)) == 0 {
		return
	}
	ad, err := classad.ParseOld(string(rec))
	if err != nil {
		s.log.Warn("history: skipping unparseable record", "err", err.Error())
		return
	}
	if err := s.archive.Append(ad); err != nil {
		s.log.Warn("history: append failed", "err", err.Error())
	}
}

func (s *HistorySync) close() {
	if s.file != nil {
		_ = s.file.Close()
		s.file, s.fi, s.partial = nil, nil, nil
	}
}
