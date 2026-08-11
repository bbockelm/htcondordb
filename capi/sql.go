// SQL over the C API: hcdb_sql runs one statement through the same parser and executor the
// REPL uses (package repl) and returns the result as a JSON document, so a non-Go caller
// gets the full SQL surface -- SELECT with GROUP BY / aggregates / HAVING / ORDER BY /
// LIMIT, AS OF time travel, INSERT / UPDATE / DELETE, DDL, views, MATCH -- without
// reimplementing any of it. The Python DB-API driver in python/ is the primary consumer.
//
// JSON rather than a row cursor: repl.Exec materializes the whole result anyway, the
// document is self-describing (so the C signature never has to grow a column-type API), and
// every target language decodes JSON in native code. A statement that returns more rows than
// the caller wants in memory should say so with LIMIT.
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
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/htcondordb/repl"
)

// hcdb_sql option bits (see capi.h).
const (
	// hcdbSQLAds adds the "ads" member: each result row's whole ClassAd in new
	// (bracketed) format. Only a non-aggregate SELECT has ads; an aggregate
	// computes rows that were never ads, and omits the member.
	hcdbSQLAds = 1 << 0
)

// hcdb_sql failure codes, beyond the shared hcdbErr. They exist so a caller can map a
// failure to its own error taxonomy (the Python driver's PEP 249 exceptions) without
// pattern-matching on message text, which would break the moment a message is reworded.
const (
	// hcdbBadSQL is a parse error: the statement never ran. The caller wrote bad SQL.
	hcdbBadSQL = -3
	// hcdbDenied is an authorization refusal -- a write on a connection the daemon
	// authorized READ-only. The statement is well-formed and would run for a client
	// with WRITE.
	hcdbDenied = -4
)

// sqlResult is the JSON document hcdb_sql writes. Field names are lowercase and stable:
// they are this library's wire contract with non-Go callers.
type sqlResult struct {
	// Select distinguishes a row-returning statement from a DML/DDL one, so a caller can
	// tell an empty SELECT (rows: [], select: true) from an UPDATE that matched nothing.
	Select bool `json:"select"`
	// Columns are the SELECT headers, in row-cell order.
	Columns []string `json:"columns"`
	// Rows holds one array of typed cells per row, aligned to Columns. See cellValues for
	// how a cell gets its type.
	Rows [][]any `json:"rows"`
	// Affected is the row count written by INSERT/UPDATE/DELETE.
	Affected int `json:"affected"`
	// Note is the executor's human-readable summary (e.g. "UPDATE 3").
	Note string `json:"note,omitempty"`
	// DurationNS is the server-side execution time repl measured, in nanoseconds.
	DurationNS int64 `json:"duration_ns"`
	// Star reports that the statement was SELECT *, so Columns is the union of the
	// matched ads' attributes rather than a list the caller wrote.
	Star bool `json:"star,omitempty"`
	// Ads carries each row's whole ClassAd as new-ClassAd (bracketed) text, when the
	// caller passed hcdbSQLAds and the result has ads.
	Ads []string `json:"ads,omitempty"`
	// InTransaction reports whether an explicit transaction is open on the connection
	// after this statement. Reported on every result so a caller tracks the connection's
	// transaction state from the server's answer rather than modelling it locally, where
	// it would drift the first time a statement failed partway.
	InTransaction bool `json:"in_transaction"`
}

// Runs one SQL statement on the connection and writes a JSON result document to *out -- a C
// string the caller frees with hcdb_free -- returning hcdbOK. On failure writes the error
// message (plain text, not JSON) to *out instead and returns hcdbBadSQL for a parse error,
// hcdbDenied for an authorization refusal, or hcdbErr for anything else.
//
// opts is a bitmask of the hcdbSQL* constants; pass 0 for none.
//
//export hcdb_sql
func hcdb_sql(h C.uintptr_t, sql *C.char, opts C.int, out **C.char) C.int {
	return guardStatus("hcdb_sql", out, func() C.int { return hcdb_sqlImpl(h, sql, opts, out) })
}

func hcdb_sqlImpl(h C.uintptr_t, sql *C.char, opts C.int, out **C.char) C.int {
	c := handleConn(h)
	if c == nil {
		*out = C.CString("invalid connection handle")
		return hcdbErr
	}

	// Parse separately from Exec (rather than through ExecString) so a parse error is
	// reported as one: the caller wrote bad SQL, which is a different kind of failure
	// from a statement that ran and failed. Timing then mirrors ExecString.
	st, err := repl.Parse(C.GoString(sql))
	if err != nil {
		*out = C.CString(err.Error())
		return hcdbBadSQL
	}

	// One statement at a time per connection: the executor caches server state and the
	// underlying CEDAR stream is a single session.
	c.mu.Lock()
	start := time.Now()
	res, err := c.ex.Exec(st)
	elapsed := time.Since(start)
	// Read the transaction flag under the same lock as the statement that may have
	// changed it, so the answer belongs to this statement and not a concurrent one.
	inTxn := c.ex.InTransaction()
	c.mu.Unlock()

	if err != nil {
		// HintFor is the single source of truth for recognizing a read-only refusal,
		// and its hint says what to check -- worth carrying to the caller verbatim.
		if hint := repl.HintFor(err); hint != "" {
			*out = C.CString(err.Error() + ": " + hint)
			return hcdbDenied
		}
		*out = C.CString(err.Error())
		return hcdbErr
	}
	if res == nil { // no statement kind returns (nil, nil), but do not marshal a nil
		*out = C.CString("statement produced no result")
		return hcdbErr
	}
	res.Duration = elapsed

	doc, err := json.Marshal(buildResult(res, int(opts), inTxn))
	if err != nil {
		*out = C.CString("encoding result: " + err.Error())
		return hcdbErr
	}
	*out = C.CString(string(doc))
	return hcdbOK
}

// buildResult converts an executor Result into the JSON document shape.
func buildResult(r *repl.Result, opts int, inTxn bool) *sqlResult {
	doc := &sqlResult{
		Select:        r.IsSelect,
		Columns:       r.Columns,
		Affected:      r.Affected,
		Note:          r.Note,
		DurationNS:    r.Duration.Nanoseconds(),
		Star:          r.Star,
		InTransaction: inTxn,
	}
	if doc.Columns == nil {
		doc.Columns = []string{} // marshal as [], not null
	}
	doc.Rows = make([][]any, len(r.Rows))
	for i := range r.Rows {
		doc.Rows[i] = cellValues(r, i)
	}
	if opts&hcdbSQLAds != 0 {
		for _, ad := range r.Ads {
			// New-ClassAd (bracketed) form, same as the streaming cursor: it is what a
			// ClassAd library's string constructor takes, and it round-trips escapes
			// losslessly where old format cannot.
			doc.Ads = append(doc.Ads, ad.StringWithPrivate())
		}
	}
	return doc
}

// cellValues types row i's cells.
//
// A Result carries its rows only as display strings, so the type of a cell has to be
// recovered. Where the row came from an ad -- every non-aggregate SELECT -- the ad still
// holds the real ClassAd value, and a column that names an attribute is read straight from
// it: a string attribute whose text happens to be "123" stays the string "123". Everything
// else (aggregate outputs, which the server already reduced to strings before they reached
// the client, and expression or aliased columns that name no attribute) falls back to
// parsing the display string, which cannot make that distinction.
func cellValues(r *repl.Result, i int) []any {
	row := r.Rows[i]
	var ad *classad.ClassAd
	if i < len(r.Ads) {
		ad = r.Ads[i]
	}

	cells := make([]any, len(r.Columns))
	for j, col := range r.Columns {
		if ad != nil {
			// An attribute that is genuinely undefined falls through to the display
			// string, which is "undefined" and converts to null just the same.
			if v := ad.EvaluateAttr(col); !v.IsUndefined() {
				cells[j] = valueJSON(v)
				continue
			}
		}
		if j < len(row) {
			cells[j] = parseCell(row[j])
		}
	}
	return cells
}

// valueJSON converts a ClassAd value to its natural JSON type. Undefined and error both
// become null: a reporting client treats each as a missing cell, and JSON has one spelling
// for that. Composite values (lists, nested ads, times) keep their ClassAd text, which is
// what a caller would have to parse anyway.
func valueJSON(v classad.Value) any {
	switch {
	case v.IsUndefined(), v.IsError():
		return nil
	case v.IsBool():
		b, _ := v.BoolValue()
		return b
	case v.IsString():
		s, _ := v.StringValue()
		return s
	case v.IsInteger():
		n, _ := v.IntValue()
		return n
	case v.IsReal():
		f, _ := v.RealValue()
		// JSON has no NaN or Infinity, and encoding/json refuses to marshal them -- so one
		// non-finite cell would fail the whole batch rather than just itself. Null is a lossy
		// answer (it arrives as None, indistinguishable from undefined) but a bounded one, and
		// it is documented; failing a report's entire query over one such value is not.
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return f
	default:
		return v.String()
	}
}

// parseCell recovers a type from a display string, for cells with no ad behind them. The
// order matches how repl renders values (valueDisplay), so a round trip is exact for every
// type except a string that looks like a number or a bool -- which is why cellValues
// prefers the ad whenever there is one.
func parseCell(s string) any {
	switch s {
	case "undefined", "error":
		return nil
	case "true":
		return true
	case "false":
		return false
	}
	// Integers before reals: ParseFloat accepts "3" and would widen it to 3.0.
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	// Reject the forms ParseFloat accepts but ClassAd never renders ("inf", "NaN",
	// "0x1p-2"), so a string attribute spelled that way survives as a string.
	if isPlainNumber(s) {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	}
	return s
}

// isPlainNumber reports whether s is decimal digits with at most one leading sign, one
// decimal point, and an optional decimal exponent.
func isPlainNumber(s string) bool {
	if s == "" {
		return false
	}
	body := strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+")
	if body == "" {
		return false
	}
	mantissa, exponent, hasExp := body, "", false
	if i := strings.IndexAny(body, "eE"); i >= 0 {
		mantissa, exponent, hasExp = body[:i], body[i+1:], true
	}
	if !isDecimal(mantissa, true) {
		return false
	}
	if hasExp {
		exponent = strings.TrimPrefix(strings.TrimPrefix(exponent, "-"), "+")
		return isDecimal(exponent, false)
	}
	return true
}

// isDecimal reports whether s is digits, optionally with a single '.' when allowPoint.
func isDecimal(s string, allowPoint bool) bool {
	digits, points := 0, 0
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
			digits++
		case c == '.' && allowPoint:
			points++
			if points > 1 {
				return false
			}
		default:
			return false
		}
	}
	return digits > 0
}
