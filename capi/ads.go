// Streaming ClassAd results: hcdb_sql_ads runs a SELECT and returns a cursor the caller
// drains one ad at a time, so a large result is walked with one ad in flight rather than
// materialized whole. This is the path a caller that wants ClassAds -- unevaluated
// expressions included -- uses instead of hcdb_sql, whose JSON rows carry evaluated
// values.
package main

/*
#include <stdlib.h>
// stdint.h for uintptr_t: glibc does not pull it in via stdlib.h the way macOS does, so
// without this the package builds on a Mac and fails on Linux with "could not determine
// what C.uintptr_t refers to".
#include <stdint.h>
*/
import "C"

import (
	"context"
	"runtime/cgo"
	"sync"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/htcondordb/repl"
)

// adCursor bridges repl's push-shaped stream to the C side's pull-shaped next().
//
// The producer goroutine runs StreamSelect and hands each ad's text to rows, which is
// unbuffered: the producer blocks until the consumer takes the previous ad, so "one ad in
// flight" is literal and an abandoned cursor cannot run the query to completion in the
// background. hcdb_sql_ads_free cancels the producer and waits for it, so a caller that
// stops early leaks neither the goroutine nor the server-side stream.
type adCursor struct {
	rows   chan string
	cancel context.CancelFunc
	done   chan struct{}

	mu  sync.Mutex
	err error // the stream's failure, readable once done is closed
}

// Runs a SELECT and returns a cursor over its ClassAds, or 0 with the reason written to
// *err (a C string the caller frees with hcdb_free). Drain with hcdb_sql_ads_next; release
// with hcdb_sql_ads_free, which is required even after the cursor is exhausted.
//
// Only a SELECT is accepted. Parameters are already substituted by the caller: the
// statement arrives as text, exactly as hcdb_sql takes it.
//
//export hcdb_sql_ads
func hcdb_sql_ads(h C.uintptr_t, sql *C.char, err **C.char) C.uintptr_t {
	return guardHandle("hcdb_sql_ads", err, func() C.uintptr_t { return hcdb_sql_adsImpl(h, sql, err) })
}

func hcdb_sql_adsImpl(h C.uintptr_t, sql *C.char, err **C.char) C.uintptr_t {
	c := handleConn(h)
	if c == nil {
		*err = C.CString("invalid connection handle")
		return 0
	}
	st, perr := repl.Parse(C.GoString(sql))
	if perr != nil {
		*err = C.CString(perr.Error())
		return 0
	}
	// Reject a non-SELECT here rather than letting the stream report it on the first
	// next(): the caller asked for ads, and finding out one row later that the statement
	// could never produce any is worse than being told at once. StreamSelect refuses it
	// too, so nothing is executed either way.
	if st.Kind != repl.StmtSelect {
		*err = C.CString("only SELECT produces ClassAds")
		return 0
	}

	ctx, cancel := context.WithCancel(c.ctx)
	cur := &adCursor{rows: make(chan string), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(cur.done)
		defer close(cur.rows)
		// The executor is per-connection state (it caches the archive-table set and holds
		// any open transaction), so a stream takes the same lock a statement does. A
		// second statement on this connection waits for the stream to finish or be freed.
		c.mu.Lock()
		defer c.mu.Unlock()

		serr := c.ex.StreamSelect(st, func(ad *classad.ClassAd) bool {
			select {
			case cur.rows <- ad.StringWithPrivate():
				return true
			case <-ctx.Done():
				return false
			}
		})
		cur.mu.Lock()
		cur.err = serr
		cur.mu.Unlock()
	}()

	*err = nil
	return C.uintptr_t(cgo.NewHandle(cur))
}

// Writes the next ad's NEW-ClassAd text (the bracketed form) to *out -- a C string the caller frees with
// hcdb_free -- and returns hcdbOK. New rather than old format because it round-trips
// escapes losslessly and is what a ClassAd library's string constructor expects. Returns
// hcdbMissing when the stream is exhausted
// (*out left NULL), or hcdbErr with the failure message in *out when the stream failed
// partway.
//
//export hcdb_sql_ads_next
func hcdb_sql_ads_next(ch C.uintptr_t, out **C.char) C.int {
	return guardStatus("hcdb_sql_ads_next", out, func() C.int { return hcdb_sql_ads_nextImpl(ch, out) })
}

func hcdb_sql_ads_nextImpl(ch C.uintptr_t, out **C.char) C.int {
	cur := handleAdCursor(ch)
	if cur == nil {
		*out = C.CString("invalid cursor handle")
		return hcdbErr
	}
	text, ok := <-cur.rows
	if ok {
		*out = C.CString(text)
		return hcdbOK
	}
	// Channel closed: the producer finished. Its error (if any) is settled by then --
	// done is closed after err is stored.
	<-cur.done
	cur.mu.Lock()
	serr := cur.err
	cur.mu.Unlock()
	if serr != nil {
		*out = C.CString(serr.Error())
		return hcdbErr
	}
	*out = nil
	return hcdbMissing
}

// Cancels the stream if it is still running, waits for the producer to stop, and frees the
// cursor. Required for every cursor, drained or not.
//
//export hcdb_sql_ads_free
func hcdb_sql_ads_free(ch C.uintptr_t) {
	guardVoid("hcdb_sql_ads_free", func() { hcdb_sql_ads_freeImpl(ch) })
}

func hcdb_sql_ads_freeImpl(ch C.uintptr_t) {
	cur := handleAdCursor(ch)
	if cur == nil {
		return
	}
	cur.cancel()
	// Drain so a producer blocked on the unbuffered send can observe the cancel and
	// return, rather than parking forever with the connection lock held.
	go func() {
		for range cur.rows {
		}
	}()
	<-cur.done
	cgo.Handle(ch).Delete()
}

// handleAdCursor resolves a cursor handle, returning nil rather than panicking on a zero,
// stale or wrong-typed one -- see handleConn.
func handleAdCursor(h C.uintptr_t) (cur *adCursor) {
	defer func() {
		if recover() != nil {
			cur = nil
		}
	}()
	cur, _ = cgo.Handle(h).Value().(*adCursor)
	return cur
}
