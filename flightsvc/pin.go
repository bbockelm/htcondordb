package flightsvc

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// Pinning a statement's schema across the two RPCs that answer it.
//
// Flight splits a query in half. GetFlightInfo (or GetSchema, or a prepare) tells the client
// what the rows will look like; DoGet produces them. The two derive the schema independently
// from a sample of a table that is still being written to, so between them a column can appear,
// disappear or change type -- and a client that trusted the first call reads the second by
// position. One extra column at the front does not shift one value, it shifts every value after
// it, and nothing in the protocol says so.
//
// The fix is to NAME the schema that was promised and check it at delivery
// (arrowtype.Fingerprint). A mismatch becomes a refusal the client can act on -- re-plan and
// run again -- instead of rows that look right.
//
// THE NAME TRAVELS, NOT THE SCHEMA. The ticket is opaque to the client but the client holds it,
// so anything in it is something the client could edit. A fingerprint it invents cannot make
// this server produce a different shape; the worst it buys is a refusal of its own request.
// Carrying the decisions themselves would hand that control over, which is the reason the
// ticket held only the query text before this and still holds no more than a name for it.
//
// WHAT THIS IS NOT. It does not make the schema stable -- it makes instability visible. A
// schema that genuinely churns will refuse often, and the answer to that is the declared type
// map, which this is the detector for rather than the replacement.

// pinPrefix marks a statement handle that carries a fingerprint. A leading NUL cannot begin a
// statement this endpoint would recognise, so a handle written by an older server -- or by a
// client that kept one -- is still read as the plain query text it is.
const pinPrefix = "\x00fp1\x00"

// pinnedHandle encodes a query and the fingerprint of the schema it was promised.
func pinnedHandle(fp, query string) string {
	return pinPrefix + fp + "\x00" + query
}

// parsePinnedHandle splits a handle into the schema it was promised and the query. An unpinned
// handle yields an empty fingerprint, which every check below reads as "nothing was promised".
func parsePinnedHandle(h string) (fp, query string) {
	rest, ok := strings.CutPrefix(h, pinPrefix)
	if !ok {
		return "", h
	}
	fp, query, ok = strings.Cut(rest, "\x00")
	if !ok {
		// Prefix but no separator: a corrupted or truncated handle. Treating the
		// remainder as SQL would run a query the client did not write, so it is refused
		// by the recogniser instead -- an empty table name matches nothing in selectRe.
		return "", ""
	}
	return fp, query
}

// checkPin compares the schema about to be delivered against the one that was promised.
//
// An empty pin is "nothing was promised", which is the pre-pin behaviour and the behaviour an
// embedder gets when it calls the scan directly rather than through a ticket.
func checkPin(pinned string, decisions []arrowtype.Decision) error {
	if pinned == "" {
		return nil
	}
	if got := arrowtype.Fingerprint(decisions); got != pinned {
		// The message says what to do, because there is a correct response and a client
		// that is told only "mismatch" cannot find it. The table was retyped between the
		// two calls; re-planning picks up the new shape.
		return status.Errorf(codes.FailedPrecondition,
			"this table's schema changed between planning this statement and fetching its rows "+
				"(promised %s, now %s); re-plan the statement and run it again",
			pinned, got)
	}
	return nil
}
