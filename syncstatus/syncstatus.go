// Package syncstatus writes a spoke's heartbeat row: one small ad in the mutable table
// "syncstatus", under key "status", rewritten on a fixed cadence by the daemon running
// schedd-sync.
//
// It exists for a federation hub. A hub's copy of an access point is behind the schedd by two
// amounts -- how far the spoke is behind the schedd, plus how far the hub is behind the spoke --
// and measuring either with timestamps from two hosts would turn clock skew into staleness. So
// the spoke states the first amount itself (the same self-measured lag its collector ad carries,
// never now minus a timestamp), and the row travels the same Watch stream as the data, in order.
// The hub stamps the time it received the row on its own clock and adds the two. An idle AP keeps
// heartbeating, so "quiet" and "stuck" are distinguishable at the hub.
//
// The row is written only by the in-process syncer. It is a few hundred bytes every few seconds.
package syncstatus

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/htcondordb/dbad"
	"github.com/bbockelm/htcondordb/scheddsync"
)

// Table is the heartbeat table's name and Key the one row's key.
const (
	Table = "syncstatus"
	Key   = "status"
)

// DefaultInterval is the heartbeat cadence when HTCONDORDB_SYNCSTATUS_INTERVAL is unset.
const DefaultInterval = 5 * time.Second

// Attribute names. The per-source attributes use the collector ad's prefixes (JobQueue, History,
// Epoch) and the same meanings, so a reader of one reads the other.
const (
	AttrMirroredScheddName    = "MirroredScheddName"
	AttrMirroredScheddAddress = "MirroredScheddAddress"
	// AttrHeartbeatSeq increases by one per row written. It continues across a spoke restart
	// (seeded from the stored row), so a hub can tell a new heartbeat from a redelivered one.
	AttrHeartbeatSeq = "HeartbeatSeq"
	// AttrHeartbeatTime is the spoke's clock when it wrote the row. Informational only: a hub
	// must never subtract it from its own clock.
	AttrHeartbeatTime = "HeartbeatTime"
	// AttrHeartbeatInterval is the cadence in seconds, so a hub can bound how old a received
	// heartbeat may be without knowing the spoke's configuration.
	AttrHeartbeatInterval = "HeartbeatIntervalSeconds"
	// AttrSpokeLagSeconds is the largest per-source lag below. Absent when any source whose file
	// exists has not completed a read pass yet: an unknown lag must not read as zero. A source whose
	// file does not exist (an epoch history on a schedd that has written none) has nothing to be
	// behind on and does not hold it back; it is reported with <Prefix>FilePresent = false.
	AttrSpokeLagSeconds = "SpokeLagSeconds"

	SuffixCaughtUp    = "CaughtUp"
	SuffixLagBytes    = "LagBytes"
	SuffixLagSeconds  = "LagSeconds"
	SuffixLastSync    = "LastSyncTime"
	SuffixGapDetected = "GapDetected"
	SuffixFilePresent = "FilePresent"
)

// Prefix maps a scheddsync source kind to its attribute prefix ("" for an unknown kind). It is
// the collector ad's mapping.
func Prefix(kind string) string {
	switch kind {
	case "job_queue.log":
		return "JobQueue"
	case "history":
		return "History"
	case "job_epoch":
		return "Epoch"
	default:
		return ""
	}
}

// LagSeconds is how far a source was behind its file at now, in whole seconds: now minus the last
// read pass that verified the mirror against the file. It is the quantity the collector ad's
// *SecondsSinceSync attributes carry, computed at the moment the row is written, which is when it
// is true. ok is false before the first such pass.
func LagSeconds(st scheddsync.SyncStatus, now time.Time) (int64, bool) {
	if st.LastSync.IsZero() {
		return 0, false
	}
	secs := int64(now.Sub(st.LastSync).Seconds())
	if secs < 0 {
		secs = 0
	}
	return secs, true
}

// Row is the content of one heartbeat, before it becomes an ad.
type Row struct {
	ScheddName    string
	ScheddAddress string
	Seq           int64
	Now           time.Time
	Interval      time.Duration
	Sources       []scheddsync.SyncStatus
	// Missing marks, by index into Sources, a source whose file does not exist.
	Missing map[int]bool
}

// BuildAd renders a heartbeat row. Pure, for testing.
func BuildAd(r Row) *classad.ClassAd {
	ad := classad.New()
	if r.ScheddName != "" {
		ad.InsertAttrString(AttrMirroredScheddName, r.ScheddName)
	}
	if r.ScheddAddress != "" {
		ad.InsertAttrString(AttrMirroredScheddAddress, r.ScheddAddress)
	}
	ad.InsertAttr(AttrHeartbeatSeq, r.Seq)
	ad.InsertAttr(AttrHeartbeatTime, r.Now.Unix())
	ad.InsertAttr(AttrHeartbeatInterval, int64((r.Interval+time.Second-1)/time.Second))

	var maxLag int64
	lagKnown := len(r.Sources) > 0
	for i, st := range r.Sources {
		p := Prefix(st.Kind)
		if p == "" {
			continue
		}
		if r.Missing[i] && st.LastSync.IsZero() {
			ad.InsertAttrBool(p+SuffixFilePresent, false)
			continue
		}
		ad.InsertAttrBool(p+SuffixCaughtUp, st.CaughtUp)
		ad.InsertAttr(p+SuffixLagBytes, st.LagBytes)
		if !st.LastSync.IsZero() {
			ad.InsertAttr(p+SuffixLastSync, st.LastSync.Unix())
		}
		if secs, ok := LagSeconds(st, r.Now); ok {
			ad.InsertAttr(p+SuffixLagSeconds, secs)
			maxLag = max(maxLag, secs)
		} else {
			lagKnown = false
		}
		if st.Kind == "history" || st.Kind == "job_epoch" {
			ad.InsertAttrBool(p+SuffixGapDetected, st.Resyncs > 0)
		}
	}
	if lagKnown {
		ad.InsertAttr(AttrSpokeLagSeconds, maxLag)
	}
	return ad
}

// Writer rewrites the heartbeat row on a fixed cadence.
type Writer struct {
	// Table is the local syncstatus table.
	Table *db.DB
	// Sources returns the running schedd-sync tailers. Their status is read through
	// dbad.LiveStatuses, the same path the collector ad and /metrics take, so all three agree.
	Sources func() []dbad.StatusSource
	// Mirrored returns the mirrored schedd's name and address (address read live, since the
	// schedd rewrites its address file on restart).
	Mirrored func() (name, address string)
	// Interval is the cadence; <= 0 means DefaultInterval.
	Interval time.Duration
	// Logger receives write failures. Nil discards.
	Logger *slog.Logger

	// Now and NewTicker are seams for tests. Nil means time.Now / time.NewTicker.
	Now       func() time.Time
	NewTicker func(time.Duration) (<-chan time.Time, func())

	seq     int64
	started bool
}

func (w *Writer) interval() time.Duration {
	if w.Interval <= 0 {
		return DefaultInterval
	}
	return w.Interval
}

func (w *Writer) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

// Run writes a row immediately and then once per interval until ctx is done.
func (w *Writer) Run(ctx context.Context) {
	tick, stop := w.ticker()
	defer stop()
	w.write()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			w.write()
		}
	}
}

func (w *Writer) ticker() (<-chan time.Time, func()) {
	if w.NewTicker != nil {
		return w.NewTicker(w.interval())
	}
	t := time.NewTicker(w.interval())
	return t.C, t.Stop
}

func (w *Writer) write() {
	if err := w.WriteOnce(); err != nil && w.Logger != nil {
		w.Logger.Warn("syncstatus: writing heartbeat row failed", "err", err.Error())
	}
}

// WriteOnce writes one heartbeat row. Not safe for concurrent use; Run calls it from one goroutine.
func (w *Writer) WriteOnce() error {
	if !w.started {
		// Continue the stored sequence, so a restart does not re-issue numbers a hub has
		// already seen.
		if prev, ok := w.Table.LookupClassAd(Key); ok {
			if n, ok := prev.EvaluateAttrInt(AttrHeartbeatSeq); ok {
				w.seq = n
			}
		}
		w.started = true
	}
	w.seq++
	var name, addr string
	if w.Mirrored != nil {
		name, addr = w.Mirrored()
	}
	sources := dbad.LiveStatuses(w.Sources)
	missing := map[int]bool{}
	for i, st := range sources {
		if st.Source != "" {
			if _, err := os.Stat(st.Source); errors.Is(err, fs.ErrNotExist) {
				missing[i] = true
			}
		}
	}
	ad := BuildAd(Row{
		ScheddName: name, ScheddAddress: addr, Seq: w.seq, Now: w.now(),
		Interval: w.interval(), Sources: sources, Missing: missing,
	})
	tx := w.Table.Begin()
	tx.NewClassAd(Key, ad)
	// Nondurable: a heartbeat lost to a crash is replaced within one interval, and fsyncing a
	// throwaway row every few seconds would be the most expensive thing this daemon does when idle.
	return tx.CommitNondurable()
}
