package repl

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// AdItem is one ad to write, with the storage key it goes under. AdText is new-ClassAd
// (bracketed) text: the caller holds a ClassAd, and that is the form a ClassAd library
// renders losslessly.
type AdItem struct {
	Key    string
	AdText string
}

// AdWriteReject reports an ad that was not written, by its index in the batch.
type AdWriteReject struct {
	Index  int
	Reason string
}

// AdWriteResult is the outcome of one batch.
type AdWriteResult struct {
	// Written counts the ads applied.
	Written int
	// Rejects are ads that could not be written at all -- unparseable, or holding a value
	// old-ClassAd text cannot represent. The rest of the batch still applied.
	Rejects []AdWriteReject
	// Conflicts are keys whose write lost an optimistic write-write race against another
	// committer. Their ads were NOT applied; the caller re-reads and retries just those.
	// Empty unless the batch committed on its own (inside a caller's transaction the
	// conflict surfaces at that transaction's COMMIT instead).
	Conflicts []string
}

// WriteAds upserts a batch of whole ClassAds, keyed by AdItem.Key.
//
// Upsert, not insert: an existing ad at a key is REPLACED, so an attribute the new ad
// omits is gone. That is what the store's write op does and what a reload wants; a caller
// updating a subset of attributes should use UPDATE instead.
//
// Atomicity follows the caller. Inside an open transaction the ads are staged into it, so
// the caller decides when they land and a whole multi-batch load can be one unit. Outside
// one, the batch commits on its own -- which is what lets an arbitrarily large load run
// without holding an unbounded write set open.
//
// A batch is partially applicable by design: an ad the server cannot parse is reported as
// a reject and the rest still apply, matching the wire op. That is what a bulk loader
// wants -- one malformed record should not lose the file.
func (e *Executor) WriteAds(table string, items []AdItem) (AdWriteResult, error) {
	var res AdWriteResult
	if len(items) == 0 {
		return res, nil
	}

	// Convert to the wire's old-ClassAd text here, so an ad the format cannot hold is
	// reported against its own index rather than failing the batch. See adTextForWire.
	batch := make([]dbrpc.AdKV, 0, len(items))
	index := make([]int, 0, len(items)) // batch position -> caller's index
	for i, item := range items {
		if strings.TrimSpace(item.Key) == "" {
			res.Rejects = append(res.Rejects, AdWriteReject{i, "empty key"})
			continue
		}
		text, err := adTextForWire(item.AdText)
		if err != nil {
			res.Rejects = append(res.Rejects, AdWriteReject{i, err.Error()})
			continue
		}
		batch = append(batch, dbrpc.AdKV{Key: item.Key, Ad: text})
		index = append(index, i)
	}
	if len(batch) == 0 {
		return res, nil
	}

	ctx := context.Background()
	if e.txActive {
		// Stage into the caller's transaction. It has to be the same table -- a dbrpc
		// transaction covers one -- and staging binds it just as a statement would.
		if e.txTable == "" {
			e.txTable = table
		} else if !strings.EqualFold(e.txTable, table) {
			return res, fmt.Errorf(
				"this transaction is writing to table %q; a transaction cannot span tables "+
					"(COMMIT or ROLLBACK before writing to %q)", e.txTable, table)
		}
		if e.tx == nil {
			tx, err := e.c.BeginTable(ctx, table)
			if err != nil {
				return res, err
			}
			e.tx = tx
		}
		return applyAdBatch(ctx, e.tx, batch, index, res, false)
	}

	tx, err := e.c.BeginTable(ctx, table)
	if err != nil {
		return res, err
	}
	res, err = applyAdBatch(ctx, tx, batch, index, res, true)
	if err != nil {
		_ = tx.Abort(ctx)
		return res, err
	}
	return res, nil
}

// applyAdBatch sends one batch and, when commit is set, commits it -- folding the wire's
// per-ad rejects and the commit's conflicted keys into the result.
func applyAdBatch(ctx context.Context, tx *dbrpc.Tx, batch []dbrpc.AdKV, index []int, res AdWriteResult, commit bool) (AdWriteResult, error) {
	rejects, err := tx.NewClassAdBatch(ctx, batch)
	if err != nil {
		return res, err
	}
	rejected := make(map[int]bool, len(rejects))
	for _, r := range rejects {
		if r.Index >= 0 && r.Index < len(index) {
			rejected[r.Index] = true
			res.Rejects = append(res.Rejects, AdWriteReject{index[r.Index], r.Err})
		}
	}
	res.Written = len(batch) - len(rejected)

	if !commit {
		return res, nil
	}
	if err := tx.Commit(ctx); err != nil {
		var conflict *db.ConflictError
		if errors.As(err, &conflict) {
			// A partial commit: the non-conflicted writes landed, the listed keys did not.
			res.Conflicts = conflict.Keys
			res.Written -= len(conflict.Keys)
			if res.Written < 0 {
				res.Written = 0
			}
			return res, nil
		}
		return res, err
	}
	return res, nil
}

// adTextForWire converts one ad from the new-ClassAd text a caller renders to the
// old-ClassAd text the write op carries, rejecting what the older format cannot hold.
//
// Old-ClassAd text is newline-separated and does no escape processing, so a string value
// containing a newline would end the attribute mid-value and one ending in a backslash
// would swallow the closing quote. Both are refused here, per ad, rather than written and
// found corrupt on the way back.
func adTextForWire(newFormat string) (string, error) {
	ad, err := classad.Parse(newFormat)
	if err != nil {
		return "", fmt.Errorf("parsing ad: %w", err)
	}
	for _, name := range ad.GetAttributes() {
		v := ad.EvaluateAttr(name)
		if !v.IsString() {
			continue
		}
		s, _ := v.StringValue()
		if strings.ContainsAny(s, "\n\r") {
			return "", fmt.Errorf("attribute %s contains a newline, which old-ClassAd format cannot represent", name)
		}
		if strings.HasSuffix(s, `\`) {
			return "", fmt.Errorf("attribute %s ends in a backslash, which old-ClassAd format cannot represent", name)
		}
	}
	return ad.MarshalOldWithPrivate(), nil
}
