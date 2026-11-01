package arrowtype

import (
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/bbockelm/htcondordb/ansi"
)

// Columns exposes a derivation's decisions as SQL column kinds, for lowering an ANSI predicate.
//
// THERE IS ONE OF THESE ON PURPOSE. Two surfaces lower SQL against these types -- the Flight SQL
// endpoint and the REPL's ANSI dialect -- and the lowering is only decidable because the type is
// known: whether `=` on a column is the case-sensitive problem or an ordinary number is a
// question only the declared type answers. A second copy of this mapping would let the two
// surfaces disagree about a column's type, and so about what a query means, which is precisely
// the divergence the ansi package exists to remove.
//
// An attribute with no TYPED column is reported as present-but-untyped rather than absent, so a
// predicate on `Requirements` is refused for the right reason -- it is a policy expression with
// no scalar form -- instead of being reported as a column that does not exist.
func Columns(decisions []Decision) ansi.Columns {
	kinds := make(map[string]ansi.Kind, len(decisions))
	for _, d := range decisions {
		kinds[d.Name] = KindOf(d)
	}
	return ansi.KindsFromNames(kinds)
}

// KindOf is the SQL kind of one decided column, or KindUnknown if it has no scalar form.
func KindOf(d Decision) ansi.Kind {
	if d.Verdict != Typed {
		return ansi.KindUnknown
	}
	switch d.Field.Type.ID() {
	case arrow.INT64:
		return ansi.KindInt
	case arrow.FLOAT64:
		return ansi.KindReal
	case arrow.STRING:
		return ansi.KindString
	case arrow.BOOL:
		return ansi.KindBool
	case arrow.TIMESTAMP:
		return ansi.KindTimestamp
	}
	return ansi.KindUnknown
}
