package repl

import (
	"fmt"
	"strings"

	"github.com/bbockelm/htcondordb/ansi"
	"github.com/bbockelm/htcondordb/arrowtype"
)

// Dialect selects how a WHERE clause's expressions are read, switchable in the REPL with
// `.dialect` the way `.format` switches the output.
//
// WHY THIS EXISTS. The REPL's SQL has always evaluated its predicates as ClassAd expressions,
// and ClassAd's `==`, `<` and `>` compare strings CASE-INSENSITIVELY. The Flight SQL endpoint
// cannot do that -- a client writing `WHERE Owner = 'bbockelm'` means SQL's `=`, which does not
// fold case -- so it lowers predicates through the ansi package instead. The two therefore
// return DIFFERENT ROWS for the same text, each correct for its own reader, with nothing in the
// results to say which you got.
//
// `.dialect ansi` makes the REPL read a predicate the way the endpoint does, which is the only
// way to see that difference without an ADBC client in hand. It is a debugging and
// compatibility tool, not an upgrade: ANSI is strictly less capable here, because the ansi
// package refuses every construct it cannot prove equivalent rather than approximating it.
// `WHERE Owner > 'R'` works today and is refused under ANSI, on purpose.
//
// THE DEFAULT STAYS CLASSAD so nothing a user has typed before changes meaning.
type Dialect int

const (
	// DialectClassAd evaluates predicates as ClassAd expressions (the default, and what
	// the REPL has always done).
	DialectClassAd Dialect = iota
	// DialectANSI lowers predicates through the ansi package, as the Flight SQL endpoint
	// does.
	DialectANSI
)

// ParseDialect parses a `.dialect` argument.
func ParseDialect(s string) (Dialect, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "classad", "":
		return DialectClassAd, nil
	case "ansi", "sql":
		return DialectANSI, nil
	default:
		return DialectClassAd, fmt.Errorf("unknown dialect %q (want classad or ansi)", s)
	}
}

func (d Dialect) String() string {
	if d == DialectANSI {
		return "ansi"
	}
	return "classad"
}

// lowerPredicate translates one predicate from SQL to a ClassAd constraint, against the types
// the table's attributes actually hold.
//
// The types come from the same derivation the Flight SQL endpoint uses, through the same
// arrowtype.Columns mapping, so a predicate means here exactly what it means there. Deriving
// them costs a profile round trip, which is why this is only done in ANSI mode -- and it is
// done per statement rather than cached, so a column added during the session is visible
// immediately instead of being refused as unknown until the REPL restarts.
func (e *Executor) lowerPredicate(table, predicate string) (string, error) {
	if strings.TrimSpace(predicate) == "" {
		return predicate, nil
	}
	rep, err := e.c.AttrTypeProfiles(e.opCtx(), table, ansiSampleMax)
	if err != nil {
		return "", fmt.Errorf("reading %s's attribute types, which the ANSI dialect needs to "+
			"lower a predicate: %w", table, err)
	}
	// A sample that came back short of its cap saw the whole table. That flag only
	// affects nullability, which a predicate does not consult -- but it is passed anyway
	// so this derivation is the same call the endpoint makes rather than a near copy.
	complete := rep.Sampled < ansiSampleMax
	ms := arrowtype.FromProfilesComplete(rep.Profiles, rep.Sampled, complete)
	_, decisions := arrowtype.Schema(ms, arrowtype.Options{})
	lowered, err := ansi.Lower(predicate, arrowtype.Columns(decisions))
	if err != nil {
		return "", err
	}
	return lowered, nil
}

// ansiSampleMax bounds the records the type derivation reads. The same cap the Flight SQL
// endpoint uses by default: large enough that a heterogeneous attribute shows both of its
// shapes, small enough to stay interactive.
const ansiSampleMax = 2000

// applyDialect rewrites a statement's predicates for the session's dialect, before anything
// executes. One place, so every path downstream -- the scan, EXPLAIN, a DELETE's selector --
// sees the same lowered constraint rather than each lowering its own copy.
func (e *Executor) applyDialect(st *Statement) error {
	if e.dialect != DialectANSI || st == nil {
		return nil
	}
	// EXPLAIN wraps the statement it reports on, and CREATE MATERIALIZED VIEW embeds the
	// SELECT that defines it. Both carry the predicate, so both are translated -- an
	// EXPLAIN that reported the untranslated plan would describe a query that never ran.
	if err := e.applyDialect(st.Inner); err != nil {
		return err
	}
	if err := e.applyDialect(st.ViewSelect); err != nil {
		return err
	}
	// A JOIN or a set operation puts columns from several tables in one predicate, and
	// which table a bare column belongs to is a question this does not answer. Refused
	// rather than lowered against the first table's types, which would silently refuse a
	// column that exists on the second or, worse, type it from the wrong one.
	if len(st.Joins) > 0 || st.IsSetOp() {
		return fmt.Errorf("the ansi dialect does not support a JOIN or a set operation yet: a " +
			"bare column could belong to either table, and lowering it against the first " +
			"table's types would type it from the wrong one. Use `.dialect classad` for this " +
			"query")
	}
	// Lowered from the RAW clause, not from st.Where: the parser has already translated
	// the text into a ClassAd expression, and feeding that to a SQL parser produces
	// nonsense rather than an error anyone can act on.
	if st.WhereRaw != "" {
		lowered, err := e.lowerPredicate(st.Table, st.WhereRaw)
		if err != nil {
			return fmt.Errorf("in the WHERE clause: %w", err)
		}
		st.Where = lowered
	}
	// A MATCH statement's TargetWhere filters the RESOURCE table, so it lowers against
	// that table's types rather than the request table's.
	if st.TargetWhereRaw != "" && st.MatchResource != "" {
		lowered, err := e.lowerPredicate(st.MatchResource, st.TargetWhereRaw)
		if err != nil {
			return fmt.Errorf("in the resource-side filter: %w", err)
		}
		st.TargetWhere = lowered
	}
	return nil
}
