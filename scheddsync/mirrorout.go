package scheddsync

// mirrorout.go is the follower end of schedd mirroring: it periodically reconstructs a
// job_queue.log from the mirrored tables (via QueueLogWriter, the inverse of JobSync) and
// writes it out atomically. On a leader htcondordb, JobSync tails a schedd's job_queue.log
// INTO the tables; the tables replicate to a follower; MirrorOut on the follower regenerates
// a job_queue.log back OUT -- a live, restorable backup of the schedd's queue that a schedd
// could load (see the Level-2 restart-restore test for QueueLogWriter's fidelity).
//
// Each cycle is a full rewrite (the schedd's own truncation model), written to a temp file
// and renamed over the destination so a reader never sees a half-written log and a failed
// cycle leaves the previous good log intact.

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// DefaultMirrorInterval is how often the log is regenerated when unset.
const DefaultMirrorInterval = 30 * time.Second

// MirrorOutConfig configures a MirrorOut. The table set matches JobSync's: Jobs is required,
// the sibling namespaces are optional (a nil table contributes no records).
type MirrorOutConfig struct {
	OutPath  string        // destination job_queue.log (required)
	Interval time.Duration // regeneration cadence; default 30s
	Logger   *slog.Logger  // default slog.Default()

	Jobs           *db.DB
	Users          *db.DB
	Jobsets        *db.DB
	Clusters       *db.DB
	Header         *db.DB
	ClusterPrivate *db.DB
	LogMeta        *db.DB
}

// MirrorOut regenerates a job_queue.log from the mirrored tables on a fixed cadence.
type MirrorOut struct {
	writer   *QueueLogWriter
	outPath  string
	interval time.Duration
	log      *slog.Logger
	status   atomic.Pointer[SyncStatus]
}

// NewMirrorOut creates a mirror that writes cfg.OutPath from the given tables.
func NewMirrorOut(cfg MirrorOutConfig) *MirrorOut {
	interval := cfg.Interval
	if interval <= 0 {
		interval = DefaultMirrorInterval
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &MirrorOut{
		writer: &QueueLogWriter{
			Jobs: cfg.Jobs, Users: cfg.Users, Jobsets: cfg.Jobsets, Clusters: cfg.Clusters,
			Header: cfg.Header, ClusterPrivate: cfg.ClusterPrivate, LogMeta: cfg.LogMeta,
		},
		outPath:  cfg.OutPath,
		interval: interval,
		log:      logger,
	}
}

// Run regenerates the log immediately and then every interval until ctx is cancelled. A
// regeneration error is logged and retried on the next tick (the previous good log is kept).
func (m *MirrorOut) Run(ctx context.Context) error {
	if err := m.Regenerate(); err != nil {
		m.log.Warn("mirror-out: initial regeneration failed", "out", m.outPath, "err", err.Error())
	}
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.Regenerate(); err != nil {
				m.log.Warn("mirror-out: regeneration failed", "out", m.outPath, "err", err.Error())
			}
		}
	}
}

// Regenerate reconstructs the log and atomically replaces outPath. It writes a sibling temp
// file, fsyncs it, and renames it over the destination, so a concurrent reader never observes
// a partial log and a failure leaves the previous log untouched.
func (m *MirrorOut) Regenerate() error {
	if m.outPath == "" {
		return fmt.Errorf("scheddsync: MirrorOut requires an output path")
	}
	if err := os.MkdirAll(filepath.Dir(m.outPath), 0o755); err != nil {
		return fmt.Errorf("scheddsync: creating mirror-out directory: %w", err)
	}
	tmp := m.outPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	n, werr := m.writer.WriteTo(f)
	if werr != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return werr
	}
	if serr := f.Sync(); serr != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return serr
	}
	if cerr := f.Close(); cerr != nil {
		_ = os.Remove(tmp)
		return cerr
	}
	if rerr := os.Rename(tmp, m.outPath); rerr != nil {
		_ = os.Remove(tmp)
		return rerr
	}
	m.publishStatus(n)
	return nil
}

// Status exposes the latest regeneration snapshot (zero value before the first cycle), mapped
// onto SyncStatus so a mirror-out plugs into the same collector-ad path as the sync sources:
// the "source" is the file it writes and the mirror is always caught up once a cycle succeeds.
func (m *MirrorOut) Status() SyncStatus { return loadStatus(&m.status) }

func (m *MirrorOut) publishStatus(n int64) {
	st := SyncStatus{
		Kind:     "job_queue.log(out)",
		Source:   m.outPath,
		Offset:   n,
		FileSize: n,
		LagBytes: 0,
		CaughtUp: true,
		LastSync: time.Now(),
	}
	m.status.Store(&st)
}
