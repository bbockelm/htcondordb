package historyimport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

// ScheddNameAttr is stamped on every imported record with the discovered schedd's
// name, so an aggregated table stays attributable and dedupable across schedds
// (ClusterId/ProcId are unique only within a schedd; ScheddName + GlobalJobId are
// globally unique).
const ScheddNameAttr = "ScheddName"

// EnteredHistoryAttr is stamped with the Unix time the record was imported. It
// matches scheddsync's attribute of the same name so archive retention and the
// zone-mapped time index work uniformly whether a record arrived by local tail or
// remote import. Duplicated here to keep the module free of a scheddsync import.
const EnteredHistoryAttr = "EnteredHistoryTime"

// errStopScan halts a newest-first history stream once it reaches records the
// archive already holds (recovery dedup): everything older is present too.
var errStopScan = errors.New("historyimport: reached already-archived records")

// ScheddRef identifies a schedd discovered in a pool.
type ScheddRef struct {
	Name    string
	Address string
}

// Discovery finds the schedds of a pool matching a constraint ("" = all).
type Discovery interface {
	Schedds(ctx context.Context, pool, constraint string) ([]ScheddRef, error)
}

// Source streams a schedd's completed-job history NEWEST FIRST, stopping the
// backward scan at the `since` cursor ("cluster.proc"; "" scans the whole
// retained history). It delivers each ad to yield; a yield error aborts the
// stream and is returned (errStopScan is used by the importer to stop early).
// limit caps the records pulled (0 = unlimited).
type Source interface {
	History(ctx context.Context, schedd ScheddRef, constraint, since string, limit int, yield func(*classad.ClassAd) error) error
}

// Writer appends stamped records to a table and reports whether one is already
// present (by GlobalJobId), the latter used only during recovery dedup.
type Writer interface {
	Append(ctx context.Context, table string, ad *classad.ClassAd) error
	Has(ctx context.Context, table, globalJobID string) (bool, error)
}

// Cursors durably persists a per-(job, schedd) resume position.
type Cursors interface {
	Get(job, schedd string) (cursor string, ok bool)
	Set(job, schedd, cursor string) error
}

// Importer runs history-import jobs against the injected discovery, source,
// writer, and cursor store. The dependencies are interfaces so the whole loop is
// unit-testable without a pool or a database.
type Importer struct {
	Disc Discovery
	Src  Source
	W    Writer
	Cur  Cursors
	Log  *slog.Logger
	Now  func() time.Time // injectable clock; defaults to time.Now
}

// Stats reports one RunJob cycle's outcome.
type Stats struct {
	Schedds  int // schedds imported successfully
	Failures int // schedds that errored (skipped; others still run)
	Imported int // records appended
}

func (im *Importer) now() time.Time {
	if im.Now != nil {
		return im.Now()
	}
	return time.Now()
}

func (im *Importer) log() *slog.Logger {
	if im.Log != nil {
		return im.Log
	}
	return slog.Default()
}

// RunJob executes one cycle of job j: discover its pool's matching schedds, then
// import each schedd's new history. A single schedd's failure is logged and
// skipped so one unreachable schedd never blocks the rest of the pool.
func (im *Importer) RunJob(ctx context.Context, j Job) (Stats, error) {
	schedds, err := im.Disc.Schedds(ctx, j.Pool, j.ScheddConstraint)
	if err != nil {
		return Stats{}, fmt.Errorf("discover schedds in pool %q: %w", j.Pool, err)
	}
	var st Stats
	for _, sd := range schedds {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		n, err := im.importSchedd(ctx, j, sd)
		st.Imported += n
		if err != nil {
			st.Failures++
			im.log().Warn("historyimport: schedd import failed",
				"job", j.Name, "schedd", sd.Name, "imported", n, "err", err)
			continue
		}
		st.Schedds++
	}
	return st, nil
}

// importSchedd pulls one schedd's history newer than its cursor and appends it.
//
// Steady state: the cursor is set, so `since` makes the schedd stop the backward
// scan at the last-imported job — every returned record is new, no dedup needed.
// Recovery/bootstrap: with no cursor, it scans (bounded by MaxPerCycle) with
// dedup on, checking each newest-first record against the archive and stopping at
// the first one already present (older ones are present too). The cursor is
// advanced to the newest record seen and persisted only after the batch lands, so
// a crash re-imports at most the un-persisted tail (at-least-once), which the
// recovery dedup then absorbs.
func (im *Importer) importSchedd(ctx context.Context, j Job, sd ScheddRef) (int, error) {
	since, hasCursor := im.Cur.Get(j.Name, sd.Name)
	dedup := !hasCursor

	var newest string
	imported := 0
	err := im.Src.History(ctx, sd, j.Constraint, since, j.MaxPerCycle, func(ad *classad.ClassAd) error {
		id := jobKey(ad)
		if id == "" {
			return nil // no ClusterId/ProcId: not a job record, skip
		}
		if newest == "" {
			newest = id // stream is newest-first, so the first record is the new cursor
		}
		if dedup {
			has, herr := im.W.Has(ctx, j.Table, globalJobID(ad, sd.Name, id))
			if herr != nil {
				return fmt.Errorf("dedup check: %w", herr)
			}
			if has {
				return errStopScan // reached the archived prefix
			}
		}
		stamp(ad, sd.Name, im.now())
		if aerr := im.W.Append(ctx, j.Table, ad); aerr != nil {
			return fmt.Errorf("append: %w", aerr)
		}
		imported++
		return nil
	})
	if err != nil && !errors.Is(err, errStopScan) {
		return imported, err
	}

	if newest != "" && newest != since {
		if serr := im.Cur.Set(j.Name, sd.Name, newest); serr != nil {
			return imported, fmt.Errorf("persist cursor: %w", serr)
		}
	}
	return imported, nil
}

// stamp writes ScheddName (authoritative, from discovery) and the import time onto
// a record before it is archived.
func stamp(ad *classad.ClassAd, scheddName string, now time.Time) {
	ad.InsertAttrString(ScheddNameAttr, scheddName)
	ad.InsertAttr(EnteredHistoryAttr, now.Unix())
}

// jobKey returns "cluster.proc" for a job ad, or "" if it lacks the ids. This is
// the per-schedd cursor and the `since` value condor_history understands.
func jobKey(ad *classad.ClassAd) string {
	cluster, ok := ad.EvaluateAttrInt("ClusterId")
	if !ok {
		return ""
	}
	proc, ok := ad.EvaluateAttrInt("ProcId")
	if !ok {
		return ""
	}
	return fmt.Sprintf("%d.%d", cluster, proc)
}

// globalJobID is the cross-schedd-unique dedup key: the ad's own GlobalJobId when
// present, else synthesized from the schedd name and the job key.
func globalJobID(ad *classad.ClassAd, scheddName, jobkey string) string {
	if g, ok := ad.EvaluateAttrString("GlobalJobId"); ok && g != "" {
		return g
	}
	return scheddName + "#" + jobkey
}
