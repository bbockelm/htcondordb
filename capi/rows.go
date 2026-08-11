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
	"encoding/json"
	"runtime/cgo"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/htcondordb/repl"
)

// The chunked result cursor: hcdb_sql's answer for callers that cannot afford the whole
// result at once.
//
// hcdb_sql marshals every row into one JSON document, so a SELECT over a large table exists
// three times simultaneously -- as ads in Go, as JSON bytes, and as objects in the caller --
// and nothing is available until the last row arrives. This cursor splits that envelope in
// two: a header at open (columns, and for DML the affected count), then rows in batches the
// caller sizes.
//
// A statement whose rows are a property of the result set rather than of any one ad is
// executed whole and served from memory instead; see repl.RowStreamer for which shapes those
// are and why. The distinction is invisible apart from the header's "streamed" flag: the
// values, their types and their order are identical either way.

// rowChunkBytes caps a batch by size as well as by row count. A caller asking for 1000 rows of
// a column holding megabyte strings did not ask for a gigabyte of JSON; the batch stops early
// and the caller's next call picks up where it left off.
const rowChunkBytes = 4 << 20

// sqlCursor serves one statement's rows in batches, from a live stream or from a materialized
// result. Exactly one of the two is in play, chosen at open.
type sqlCursor struct {
	columns []string

	// Streaming: the producer goroutine runs StreamSelect and hands each row's cells to
	// rows, which is unbuffered -- so the producer blocks until this side takes the previous
	// row, and an abandoned cursor cannot run the query to completion in the background.
	rows   chan []any
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	err    error // the stream's failure, readable once done is closed

	// Materialized: rows already in hand, served from position i.
	buffered *repl.Result
	i        int
}

// sqlHeader is what a caller learns at open: everything in hcdb_sql's envelope except the rows.
type sqlHeader struct {
	// Select distinguishes a row-returning statement from a DML/DDL one, so a caller can tell
	// an empty SELECT from an UPDATE that matched nothing.
	Select bool `json:"select"`
	// Columns are the row headers, in cell order. Fixed for the cursor's lifetime, which is
	// why SELECT * cannot stream: its columns are the union of every matched ad's attributes.
	Columns []string `json:"columns"`
	// Affected is the row count written by INSERT/UPDATE/DELETE, which has run by the time
	// this header is produced.
	Affected int `json:"affected"`
	// Note is the executor's human-readable summary (e.g. "UPDATE 3").
	Note string `json:"note,omitempty"`
	// Star reports that the statement was SELECT *.
	Star bool `json:"star,omitempty"`
	// InTransaction reports whether an explicit transaction is open on the connection.
	InTransaction bool `json:"in_transaction"`
	// Streamed reports whether rows arrive as the server produces them (true) or were
	// materialized at open (false). Advisory: it tells a caller whether its memory is bounded,
	// and changes nothing about the rows themselves.
	Streamed bool `json:"streamed"`
	// DurationNS is the execution time for a statement that ran to completion at open -- DML,
	// or a materialized SELECT. Zero for a streamed SELECT, which has not finished yet.
	DurationNS int64 `json:"duration_ns"`
}

// Runs a statement and opens a cursor over its rows: on hcdbOK, *cursor holds the cursor handle
// and *header the result header (a C string the caller frees with hcdb_free). On failure the
// reason goes to *out and the code says what kind -- hcdbBadSQL for a statement that did not
// parse, hcdbDenied for one the daemon refused, hcdbErr for a run-time failure -- the same
// classification hcdb_sql reports, so a caller maps codes rather than matching error text.
//
// Every statement kind is accepted. A DML or DDL statement executes here, before this returns,
// and reports through the header's affected/note with no rows to drain -- so a caller can send
// everything through one entry point rather than deciding what is a SELECT by inspecting text.
//
// Drain with hcdb_sql_stream_next; release with hcdb_sql_stream_free, which is required even
// after the cursor is exhausted. The connection is held for the cursor's lifetime: a streaming
// cursor keeps the executor lock, so another statement on the same connection waits until this
// one is drained or freed.
//
//export hcdb_sql_stream
func hcdb_sql_stream(h C.uintptr_t, sql *C.char, cursor *C.uintptr_t, header **C.char, out **C.char) C.int {
	return guardStatus("hcdb_sql_stream", out, func() C.int {
		return hcdb_sql_streamImpl(h, sql, cursor, header, out)
	})
}

func hcdb_sql_streamImpl(h C.uintptr_t, sql *C.char, cursor *C.uintptr_t, header **C.char, out **C.char) C.int {
	*cursor = 0
	c := handleConn(h)
	if c == nil {
		*out = C.CString("invalid connection handle")
		return hcdbErr
	}
	st, perr := repl.Parse(C.GoString(sql))
	if perr != nil {
		*out = C.CString(perr.Error())
		return hcdbBadSQL
	}

	// A SELECT whose rows come from ads alone streams; everything else -- DML, and the
	// result-set shapes RowStreamer declines -- runs to completion here.
	var (
		columns []string
		rowOf   func(*classad.ClassAd) []classad.Value
	)
	if st.Kind == repl.StmtSelect {
		columns, rowOf, perr = c.ex.RowStreamer(st)
		if perr != nil {
			// A malformed SELECT expression: the statement parsed but cannot be compiled, which
			// is still the caller's text to fix.
			*out = C.CString(perr.Error())
			return hcdbBadSQL
		}
	}
	if rowOf == nil {
		return openBufferedCursor(c, st, cursor, header, out)
	}
	return openStreamCursor(c, st, columns, rowOf, cursor, header, out)
}

// openBufferedCursor executes the statement now and serves its rows from memory.
func openBufferedCursor(c *conn, st *repl.Statement, cursor *C.uintptr_t, header **C.char, out **C.char) C.int {
	c.mu.Lock()
	start := time.Now()
	res, execErr := c.ex.Exec(st)
	elapsed := time.Since(start)
	// Read the transaction flag under the same lock as the statement that may have changed
	// it, so the answer belongs to this statement and not a concurrent one.
	inTxn := c.ex.InTransaction()
	c.mu.Unlock()

	if execErr != nil {
		// HintFor is the single source of truth for recognizing a read-only refusal, and its
		// hint says what to check -- worth carrying to the caller verbatim.
		if hint := repl.HintFor(execErr); hint != "" {
			*out = C.CString(execErr.Error() + ": " + hint)
			return hcdbDenied
		}
		*out = C.CString(execErr.Error())
		return hcdbErr
	}
	if res == nil {
		*out = C.CString("statement produced no result")
		return hcdbErr
	}

	cur := &sqlCursor{columns: res.Columns, buffered: res}
	h := sqlHeader{
		Select:        res.IsSelect,
		Columns:       res.Columns,
		Affected:      res.Affected,
		Note:          res.Note,
		Star:          res.Star,
		InTransaction: inTxn,
		DurationNS:    elapsed.Nanoseconds(),
	}
	return finishOpen(cur, h, cursor, header, out)
}

// openStreamCursor starts the producer and returns before the first row is fetched.
func openStreamCursor(c *conn, st *repl.Statement, columns []string,
	rowOf func(*classad.ClassAd) []classad.Value, cursor *C.uintptr_t, header **C.char,
	out **C.char) C.int {

	// A SELECT cannot change the transaction state, so reading it at open is as accurate as
	// reading it at the end -- and the end may be a long way off.
	c.mu.Lock()
	inTxn := c.ex.InTransaction()
	c.mu.Unlock()

	ctx, cancel := context.WithCancel(c.ctx)
	cur := &sqlCursor{
		columns: columns,
		rows:    make(chan []any),
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go func() {
		defer close(cur.done)
		defer close(cur.rows)
		// The executor is per-connection state (it caches the archive-table set and holds any
		// open transaction), so a stream takes the same lock a statement does.
		c.mu.Lock()
		defer c.mu.Unlock()

		serr := c.ex.StreamSelect(st, func(ad *classad.ClassAd) bool {
			cells := make([]any, len(columns))
			for j, v := range rowOf(ad) {
				cells[j] = valueJSON(v)
			}
			select {
			case cur.rows <- cells:
				return true
			case <-ctx.Done():
				return false
			}
		})
		cur.mu.Lock()
		cur.err = serr
		cur.mu.Unlock()
	}()

	h := sqlHeader{
		Select:        true,
		Columns:       columns,
		InTransaction: inTxn,
		Streamed:      true,
	}
	return finishOpen(cur, h, cursor, header, out)
}

func finishOpen(cur *sqlCursor, h sqlHeader, cursor *C.uintptr_t, header **C.char, out **C.char) C.int {
	if h.Columns == nil {
		h.Columns = []string{} // marshal as [], not null
	}
	doc, err := json.Marshal(h)
	if err != nil {
		cur.release()
		*out = C.CString("encoding result header: " + err.Error())
		return hcdbErr
	}
	*header = C.CString(string(doc))
	*out = nil
	*cursor = C.uintptr_t(cgo.NewHandle(cur))
	return hcdbOK
}

// handleSQLCursor resolves a cursor handle, returning nil rather than panicking on a zero,
// stale, or wrong-typed one.
func handleSQLCursor(ch C.uintptr_t) (cur *sqlCursor) {
	defer func() {
		if recover() != nil {
			cur = nil
		}
	}()
	cur, _ = cgo.Handle(ch).Value().(*sqlCursor)
	return cur
}

// Writes the next batch of rows to *out as a JSON array of arrays -- a C string the caller
// frees with hcdb_free -- and returns hcdbOK. Returns hcdbMissing when the rows are exhausted
// (*out left NULL), or hcdbErr with the reason in *out if the stream failed.
//
// max_rows bounds the batch; a batch also stops early once it reaches an internal byte budget,
// so a short batch does not mean the end. Only hcdbMissing means that.
//
//export hcdb_sql_stream_next
func hcdb_sql_stream_next(ch C.uintptr_t, maxRows C.int, out **C.char) C.int {
	return guardStatus("hcdb_sql_stream_next", out, func() C.int {
		return hcdb_sql_stream_nextImpl(ch, maxRows, out)
	})
}

func hcdb_sql_stream_nextImpl(ch C.uintptr_t, maxRows C.int, out **C.char) C.int {
	cur := handleSQLCursor(ch)
	if cur == nil {
		*out = C.CString("invalid cursor handle")
		return hcdbErr
	}
	limit := int(maxRows)
	if limit <= 0 {
		limit = 1
	}

	batch, streamErr := cur.nextBatch(limit)
	if streamErr != nil {
		*out = C.CString(streamErr.Error())
		return hcdbErr
	}
	if len(batch) == 0 {
		*out = nil
		return hcdbMissing
	}
	doc, err := json.Marshal(batch)
	if err != nil {
		*out = C.CString("encoding rows: " + err.Error())
		return hcdbErr
	}
	*out = C.CString(string(doc))
	return hcdbOK
}

// nextBatch collects up to limit rows, stopping early at the byte budget. An empty batch with
// no error means exhausted.
func (cur *sqlCursor) nextBatch(limit int) ([][]any, error) {
	if cur.buffered != nil {
		return cur.bufferedBatch(limit), nil
	}

	batch := make([][]any, 0, min(limit, 64))
	bytes := 0
	for len(batch) < limit && bytes < rowChunkBytes {
		cells, ok := <-cur.rows
		if !ok {
			// Channel closed: the producer finished. Its error (if any) is settled by then --
			// done is closed after err is stored.
			<-cur.done
			cur.mu.Lock()
			err := cur.err
			cur.mu.Unlock()
			if err != nil {
				return nil, err
			}
			break
		}
		batch = append(batch, cells)
		bytes += estimateSize(cells)
	}
	return batch, nil
}

func (cur *sqlCursor) bufferedBatch(limit int) [][]any {
	batch := make([][]any, 0, min(limit, 64))
	bytes := 0
	for cur.i < len(cur.buffered.Rows) && len(batch) < limit && bytes < rowChunkBytes {
		cells := cellValues(cur.buffered, cur.i)
		cur.i++
		batch = append(batch, cells)
		bytes += estimateSize(cells)
	}
	return batch
}

// estimateSize approximates a row's JSON size. Rough on purpose: it exists to stop a batch of
// large strings from growing without bound, and paying for exact accounting per cell would cost
// more than the imprecision does.
func estimateSize(cells []any) int {
	n := 2 + len(cells) // brackets and separators
	for _, c := range cells {
		if s, ok := c.(string); ok {
			n += len(s) + 3 // quotes and comma
			continue
		}
		n += 8
	}
	return n
}

// Releases a cursor, stopping the stream behind it first. Required even after the cursor is
// exhausted, and safe to call on a handle that was never valid.
//
//export hcdb_sql_stream_free
func hcdb_sql_stream_free(ch C.uintptr_t) {
	guardVoid("hcdb_sql_stream_free", func() { hcdb_sql_stream_freeImpl(ch) })
}

func hcdb_sql_stream_freeImpl(ch C.uintptr_t) {
	cur := handleSQLCursor(ch)
	if cur == nil {
		return
	}
	cur.release()
	cgo.Handle(ch).Delete()
}

// release stops a live producer and waits for it, so a caller that stops early leaks neither
// the goroutine nor the server-side stream -- and, critically, does not leave the executor lock
// held by a producer parked on an unbuffered send.
func (cur *sqlCursor) release() {
	if cur.cancel == nil {
		return // materialized: nothing running
	}
	cur.cancel()
	// Drain so a producer blocked on the send can observe the cancel and return.
	go func() {
		for range cur.rows {
		}
	}()
	<-cur.done
}
