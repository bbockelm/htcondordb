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
	"bytes"
	"unsafe"

	"github.com/bbockelm/htcondordb/repl"
)

// Arrow IPC over the C API.
//
// The encoder and the type contract exist for the Flight SQL endpoint; this is what lets a
// client that is NOT speaking Flight reach them -- a Python process with no gRPC, no port to
// open and no second authentication path, over the CEDAR session it already has.
//
// Its value is not a capability nobody had. pandas already works through the DB-API, and
// DuckDB is already reachable by a pandas hop. What changes is that five representations --
// ad text, a ClassAd parse, JSON, Python objects, pandas inference -- collapse to one, and
// that the types stop being guessed at the far end.
//
// BINARY, so it does not use the C-string convention the rest of this file does. An Arrow IPC
// stream contains NUL bytes by construction (every length prefix with a zero octet in it), so
// returning it through a char* would truncate it at the first one -- silently, and at a
// different place for every query. It follows the queue path's convention instead: a buffer
// and a length, freed with hcdb_free.

// Runs one SQL statement and writes its result to *out as an Arrow IPC stream -- a buffer of
// *length bytes the caller frees with hcdb_free -- returning hcdbOK. On failure writes the
// error message to *errout (a C string, also freed with hcdb_free) and returns hcdbBadSQL for
// a parse error, hcdbDenied for an authorization refusal, or hcdbErr for anything else.
//
//export hcdb_sql_arrow
func hcdb_sql_arrow(h C.uintptr_t, sql *C.char, out *unsafe.Pointer, length *C.size_t,
	errout **C.char) C.int {
	return guardStatus("hcdb_sql_arrow", errout, func() C.int {
		return hcdb_sql_arrowImpl(h, sql, out, length, errout)
	})
}

func hcdb_sql_arrowImpl(h C.uintptr_t, sql *C.char, out *unsafe.Pointer, length *C.size_t,
	errout **C.char) C.int {
	*out = nil
	*length = 0

	c := handleConn(h)
	if c == nil {
		*errout = C.CString("invalid connection handle")
		return hcdbErr
	}

	// Parsed separately from Exec so a parse error is reported as one -- the caller wrote
	// bad SQL, which is a different failure from a statement that ran and failed. Same
	// split as hcdb_sql.
	st, err := repl.Parse(C.GoString(sql))
	if err != nil {
		*errout = C.CString(err.Error())
		return hcdbBadSQL
	}

	// REFUSED BEFORE IT RUNS. A write has no Arrow output either way, but checking after
	// Exec would mean a DELETE passed to this function deletes and then reports that it
	// cannot be rendered -- the caller gets an error and the rows are gone anyway. The
	// shape of the statement is known from the parse, so there is no reason to run it
	// first.
	if st.Kind != repl.StmtSelect {
		*errout = C.CString("arrow output is for SELECT results; this statement returns no rows")
		return hcdbBadSQL
	}

	// One statement at a time per connection: the executor caches server state and the
	// CEDAR stream underneath is a single session.
	c.mu.Lock()
	res, err := c.ex.Exec(st)
	c.mu.Unlock()
	if err != nil {
		// HintFor is the single source of truth for recognizing a read-only refusal,
		// the same way hcdb_sql uses it, and its hint says what to check -- worth
		// carrying to the caller verbatim rather than making them guess.
		if hint := repl.HintFor(err); hint != "" {
			*errout = C.CString(err.Error() + ": " + hint)
			return hcdbDenied
		}
		*errout = C.CString(err.Error())
		return hcdbErr
	}
	// Encoded under the connection's own lock as well: WriteArrow derives the table's
	// columns through the same executor, which is not safe to use concurrently with
	// another statement on this connection.
	var buf bytes.Buffer
	c.mu.Lock()
	err = c.ex.WriteArrow(&buf, st, res)
	c.mu.Unlock()
	if err != nil {
		*errout = C.CString(err.Error())
		return hcdbErr
	}

	// An empty result still produces a stream: Arrow IPC carries the SCHEMA before any
	// batch, so a caller that read zero bytes could not tell an empty table from a
	// failure, and could not learn the column types it asked for.
	*out = C.CBytes(buf.Bytes())
	*length = C.size_t(buf.Len())
	return hcdbOK
}
