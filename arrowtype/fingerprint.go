package arrowtype

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
)

// Naming a derived schema, so two parties can tell whether they are talking about the same one.
//
// A schema here is MEASURED rather than declared: it is derived from a sample of a table that is
// still being written to, so two derivations a minute apart can legitimately disagree. That is
// fine while a client reads the schema off the stream it is reading, which is what every Arrow
// reader does. It is NOT fine across the FlightInfo/DoGet pair, where a client is promised a
// shape by one RPC and delivered rows by another: there, a column that appeared in between does
// not shift one value, it shifts every value after it, and nothing in the protocol says so.
//
// A fingerprint makes that detectable. The promising call names the schema it promised; the
// delivering call names the schema it is about to produce; if the names differ the client is
// told, rather than handed rows under a shape it no longer has.
//
// WHY A CONTENT HASH RATHER THAN A COUNTER. A counter has to be remembered somewhere and bumped
// by whoever changes the thing, and a schema derived from a sample has no such owner -- the
// table's writers do not know they changed it. A hash needs no state, agrees across processes
// and across restarts, and has the property a counter does not: a re-derivation that produces
// the same schema produces the same name, so an unchanged table never invalidates anything.
//
// WHAT IT COVERS is what the client was promised: the result columns, in order, with their
// types. Not the reasons an attribute went untyped, which are prose and can move with the
// sample without changing what a row looks like.

// fingerprintVersion is mixed in first so the hashing scheme can change without a new hash
// colliding with an old one that a client is still holding.
const fingerprintVersion = "htcondordb.arrowtype.fp.1"

// Fingerprint names a derived schema by its content.
//
// Equal fingerprints mean the same columns, in the same order, with the same types and
// nullability -- so a consumer reading by position reads the same thing. Decisions that got no
// typed column are included too, by name and verdict: they are not result columns, but a client
// asking for one is refused rather than served, so whether an attribute has a column is part of
// what the endpoint promised about this statement.
func Fingerprint(decisions []Decision) string {
	h := sha256.New()
	h.Write([]byte(fingerprintVersion))
	var n [8]byte
	binary.LittleEndian.PutUint64(n[:], uint64(len(decisions)))
	h.Write(n[:])
	// Length-prefixed, not delimited: an attribute name is a ClassAd identifier today, but a
	// delimiter-joined hash is the kind of thing that stops being injective the moment
	// something upstream widens what a name may contain.
	write := func(s string) {
		binary.LittleEndian.PutUint64(n[:], uint64(len(s)))
		h.Write(n[:])
		h.Write([]byte(s))
	}
	for _, d := range decisions {
		write(d.Name)
		write(strconv.Itoa(int(d.Verdict)))
		if d.Tail {
			write("tail")
		} else {
			write("")
		}
		if d.Verdict != Typed {
			// An untyped decision has no field, and reading one would hash whatever a
			// zero arrow.Field stringifies to rather than a type.
			write("")
			write("")
			continue
		}
		write(d.Field.Name)
		write(d.Field.Type.String())
		if d.Field.Nullable {
			write("nullable")
		} else {
			write("")
		}
	}
	// Half a SHA-256 is 128 bits, which is far past what a collision would need to be
	// reachable here, and short enough to travel in a ticket and be read in a log line.
	return hex.EncodeToString(h.Sum(nil)[:16])
}
