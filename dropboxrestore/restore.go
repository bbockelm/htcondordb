package dropboxrestore

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/htcondordb/dropboxmanifest"
)

// Writer is the subset of historyimport.Writer a restore needs: append a record, and ask whether
// one is already present. historyimport.DBWriter satisfies it.
//
// Has is what makes a restore re-runnable. The exporter's delivery is at-least-once, so a dropbox
// legitimately contains duplicate tarballs after a crash, and an operator may have to re-run a
// restore that died halfway. Deduping on GlobalJobId makes both harmless.
type Writer interface {
	Append(ctx context.Context, table string, ad *classad.ClassAd) error
	Has(ctx context.Context, table, globalJobID string) (bool, error)
}

// Options control one restore.
type Options struct {
	Dir   string // the dropbox directory
	Table string // the destination archive table

	// SkipDedup appends every record without asking whether it is already present. It is much
	// faster into an empty table, where the query can only ever answer no, and wrong into a
	// populated one.
	SkipDedup bool

	// DryRun reads and parses everything, reporting what would be written, without writing.
	DryRun bool

	// SkipChecksums skips the per-batch digest check. The check costs one extra pass over each
	// batch; skip it only when the bytes are known good and the restore is large.
	SkipChecksums bool

	// Progress, if set, is called periodically with the running result.
	Progress func(Result)
}

// Result is the outcome of a restore.
type Result struct {
	Batches     int
	Records     int // records read out of the dropbox
	Appended    int // records written
	Duplicates  int // records already present (dedup)
	BadBatches  []string
	MissingSeqs []uint64
	Losses      []LossReport
	Elapsed     time.Duration

	// ShortReads names batches that yielded fewer records than their manifest claims. This is
	// the failure a dropbox could not detect before manifests: a damaged batch simply produces
	// fewer records, and without a declared count nothing notices.
	ShortReads []string
	// Unverifiable counts batches with no manifest. They are restored normally; they just cannot
	// be checked.
	Unverifiable int
	// Checksums counts batches whose bytes matched their manifest's digest.
	Checksums int
}

// Restore reads every batch in a dropbox, in sequence order, into the destination table.
//
// A damaged batch does not stop the restore. It is recorded in BadBatches, its readable records
// are still written, and the next batch is attempted -- the whole reason to keep records
// independent is that one damaged region costs only itself.
func Restore(ctx context.Context, w Writer, opt Options) (Result, error) {
	started := time.Now()
	res := Result{}

	listing, err := List(opt.Dir)
	if err != nil {
		return res, err
	}
	res.MissingSeqs = listing.MissingSeqs
	res.Losses = listing.Losses

	for _, b := range listing.Batches {
		if err := ctx.Err(); err != nil {
			res.Elapsed = time.Since(started)
			return res, err
		}
		// Check the bytes before decoding them: a digest mismatch explains a short read that
		// would otherwise look like an empty batch, and it costs one pass over a file that is
		// about to be read anyway.
		if b.Manifest == nil {
			res.Unverifiable++
		} else if !opt.SkipChecksums {
			sum, n, herr := dropboxmanifest.HashFile(b.Path)
			switch {
			case herr != nil:
				res.BadBatches = append(res.BadBatches, fmt.Sprintf("%s: %v", filepath.Base(b.Path), herr))
			case n != b.Manifest.Bytes:
				res.BadBatches = append(res.BadBatches, fmt.Sprintf("%s: %d bytes, manifest says %d -- truncated or modified", filepath.Base(b.Path), n, b.Manifest.Bytes))
			case sum != b.Manifest.SHA256:
				res.BadBatches = append(res.BadBatches, fmt.Sprintf("%s: checksum mismatch -- the bytes are not what was written", filepath.Base(b.Path)))
			default:
				res.Checksums++
			}
		}

		recs, rerr := ReadBatch(b)
		if rerr != nil {
			res.BadBatches = append(res.BadBatches, rerr.Error())
		}
		// The assertion the manifest exists for.
		if b.Manifest != nil && len(recs) != b.Manifest.Records {
			res.ShortReads = append(res.ShortReads, fmt.Sprintf("%s: recovered %d of the %d records it declares",
				filepath.Base(b.Path), len(recs), b.Manifest.Records))
		}
		res.Batches++
		for _, r := range recs {
			res.Records++
			if opt.DryRun {
				continue
			}
			if !opt.SkipDedup {
				gjid, _ := r.Ad.EvaluateAttrString("GlobalJobId")
				if gjid != "" {
					present, herr := w.Has(ctx, opt.Table, gjid)
					if herr != nil {
						res.Elapsed = time.Since(started)
						return res, fmt.Errorf("dedup check for %s: %w", gjid, herr)
					}
					if present {
						res.Duplicates++
						continue
					}
				}
			}
			if err := w.Append(ctx, opt.Table, r.Ad); err != nil {
				res.Elapsed = time.Since(started)
				return res, fmt.Errorf("appending %s from %s: %w", r.Entry, b.Path, err)
			}
			res.Appended++
		}
		if opt.Progress != nil {
			res.Elapsed = time.Since(started)
			opt.Progress(res)
		}
	}
	res.Elapsed = time.Since(started)
	return res, nil
}

// Report renders a human-readable summary. The loss reports and sequence gaps come first because
// they are the part an operator must not miss: they say what the backup never held, and a restore
// that silently succeeds over a gap is how an incomplete archive gets trusted.
func (r Result) Report(w io.Writer) {
	for _, l := range r.Losses {
		reason := ""
		if l.Ad != nil {
			start, _ := l.Ad.EvaluateAttrInt("EstimatedLossStartTime")
			end, _ := l.Ad.EvaluateAttrInt("EstimatedLossEndTime")
			reason = fmt.Sprintf(" covering roughly %s .. %s",
				time.Unix(start, 0).UTC().Format(time.RFC3339),
				time.Unix(end, 0).UTC().Format(time.RFC3339))
		}
		fmt.Fprintf(w, "DATA LOSS: the exporter reported a gap%s -- those records are not in this dropbox and cannot be restored from it\n", reason)
	}
	if len(r.MissingSeqs) > 0 {
		fmt.Fprintf(w, "INCOMPLETE: %d batch sequence number(s) are missing from this dropbox: %v\n", len(r.MissingSeqs), r.MissingSeqs)
	}
	for _, b := range r.BadBatches {
		fmt.Fprintf(w, "DAMAGED BATCH: %s (its readable records were still restored)\n", b)
	}
	for _, s := range r.ShortReads {
		fmt.Fprintf(w, "SHORT READ: %s\n", s)
	}
	fmt.Fprintf(w, "%d batches, %d records read, %d appended, %d already present, in %s\n",
		r.Batches, r.Records, r.Appended, r.Duplicates, r.Elapsed.Round(time.Millisecond))
	if r.Unverifiable > 0 {
		fmt.Fprintf(w, "%d batch(es) had no manifest and could not be checked (written before manifests, or the manifest was lost)\n", r.Unverifiable)
	}
	if r.Checksums > 0 {
		fmt.Fprintf(w, "%d batch(es) matched their manifest\n", r.Checksums)
	}
}
