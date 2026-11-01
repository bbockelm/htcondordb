package ansi

import (
	"fmt"
	"math"
	"strconv"
)

// Parameter placeholders, which ADBC clients use for most of their queries: a driver given
// `cursor.execute(sql, params)` sends the SQL with `?` holes and the values in a separate Arrow
// batch, and a server that cannot bind forces every client to inline its values as text.
//
// WHY A FRAGMENT LIST RATHER THAN A STRING WITH MARKERS. The obvious implementation lowers to a
// string containing a sentinel and substitutes at bind time. That requires proving no other part
// of the expression can contain the sentinel -- and the one caller-controlled part IS a string
// literal, whose rendered form can hold any byte that is not a quote or a backslash. The proof
// is doable and is exactly the kind that stops holding when someone adds an escape. So binding
// here is a CONCATENATION of pieces that were never adjacent, and there is nothing to search.
//
// WHY A SLOT IS A WHOLE COMPARISON, not a value hole. A NULL bound value does not make a
// comparison compare against null; it makes the comparison yield NULL, which drops the row. That
// is `false` in a ClassAd WHERE clause, and `Cpus == false` is not it. So the slot covers the
// comparison and renders either `Cpus == 4` or `false`, decided where the operator is still in
// scope. Collapsing to `false` is right for every position the lowering allows -- a negated
// equality is already refused, and under AND and OR false and NULL agree.

// Param describes one placeholder, in the order the placeholders appear.
type Param struct {
	// Column is the attribute the placeholder is compared against. Reported so a client's
	// parameter schema can name something more useful than "?1".
	Column string
	// Kind is the type a bound value must have, taken from the column's declared type.
	// This is the type map paying off a second time: without it there is no parameter
	// schema to advertise and no way to reject a bad bind before it reaches the store.
	Kind Kind
}

// frag is one piece of a lowered predicate: fixed text, or a slot whose text depends on a bound
// value.
type frag struct {
	text   string
	slot   int                       // -1 for fixed text, else the parameter index
	render func(any) (string, error) // for a slot: renders the whole comparison
}

// expr is a lowered expression under construction. The parser carries these instead of strings
// so a placeholder can survive to bind time.
type expr struct{ frags []frag }

// fixed is an expression with no placeholders.
func fixed(s string) expr { return expr{frags: []frag{{text: s, slot: -1}}} }

// seq concatenates pieces: a string is fixed text, an expr is spliced in. Adjacent fixed pieces
// are merged, so a predicate with no placeholders ends up as a single fragment.
func seq(parts ...any) expr {
	var out expr
	for _, p := range parts {
		switch v := p.(type) {
		case string:
			out.appendFixed(v)
		case expr:
			for _, f := range v.frags {
				if f.slot < 0 {
					out.appendFixed(f.text)
				} else {
					out.frags = append(out.frags, f)
				}
			}
		default:
			panic(fmt.Sprintf("ansi: seq got a %T", p))
		}
	}
	return out
}

func (e *expr) appendFixed(s string) {
	if n := len(e.frags); n > 0 && e.frags[n-1].slot < 0 {
		e.frags[n-1].text += s
		return
	}
	e.frags = append(e.frags, frag{text: s, slot: -1})
}

// Template is a lowered predicate with placeholders left open.
//
// It is immutable once built, so one prepared statement can serve concurrent fetches with
// different bindings.
type Template struct {
	body   expr
	params []Param
}

// Params returns the placeholders in order. The slice is a copy: a caller building a parameter
// schema from it must not be able to alter the template.
func (t *Template) Params() []Param {
	out := make([]Param, len(t.params))
	copy(out, t.params)
	return out
}

// Bind substitutes the values and returns a ClassAd constraint.
//
// A nil value is SQL NULL, which drops the row -- see the note above on why that collapses the
// comparison rather than the value.
func (t *Template) Bind(vals []any) (string, error) {
	if len(vals) != len(t.params) {
		return "", fmt.Errorf("the statement takes %d parameter(s) and %d were bound",
			len(t.params), len(vals))
	}
	var b []byte
	for _, f := range t.body.frags {
		if f.slot < 0 {
			b = append(b, f.text...)
			continue
		}
		text, err := f.render(vals[f.slot])
		if err != nil {
			return "", err
		}
		b = append(b, text...)
	}
	return string(b), nil
}

// bound is a bound value normalised to one of ClassAd's scalar forms. Normalising here rather
// than at the caller means the Arrow-to-ClassAd conversion exists once; a caller that has an
// int32 column should not have to know which widths this package accepts.
type bound struct {
	null bool
	kind Kind
	i    int64
	f    float64
	s    string
	b    bool
}

func normalize(v any) (bound, error) {
	switch x := v.(type) {
	case nil:
		return bound{null: true}, nil
	case int64:
		return bound{kind: KindInt, i: x}, nil
	case int32:
		return bound{kind: KindInt, i: int64(x)}, nil
	case int16:
		return bound{kind: KindInt, i: int64(x)}, nil
	case int8:
		return bound{kind: KindInt, i: int64(x)}, nil
	case int:
		return bound{kind: KindInt, i: int64(x)}, nil
	case uint64:
		// Refused rather than wrapped: a value past the signed range would otherwise
		// silently become negative and match the wrong rows.
		if x > math.MaxInt64 {
			return bound{}, unsup("the bound value %d is too large for a signed 64-bit "+
				"comparison, and wrapping it would match the wrong rows", x)
		}
		return bound{kind: KindInt, i: int64(x)}, nil
	case uint32:
		return bound{kind: KindInt, i: int64(x)}, nil
	case float64:
		return boundFloat(x)
	case float32:
		return boundFloat(float64(x))
	case string:
		return bound{kind: KindString, s: x}, nil
	case bool:
		return bound{kind: KindBool, b: x}, nil
	}
	return bound{}, unsup("a %T cannot be bound as a parameter", v)
}

// boundFloat refuses the non-finite values. SQL has no literal for them, ClassAd's parser has no
// syntax for them, and their comparison semantics differ between engines (DuckDB orders NaN
// above every number; IEEE makes every NaN comparison false). Refusing names the problem;
// rendering something would answer a question nobody can agree on.
func boundFloat(f float64) (bound, error) {
	if math.IsNaN(f) {
		return bound{}, unsup("NaN cannot be bound as a parameter: engines disagree on how it " +
			"compares, so there is no lowering that is right everywhere")
	}
	if math.IsInf(f, 0) {
		return bound{}, unsup("an infinite value cannot be bound as a parameter: ClassAd has no " +
			"literal for it")
	}
	return bound{kind: KindReal, f: f}, nil
}

// check refuses a bound value whose type cannot be what the column holds.
//
// This is REDUNDANT for safety and deliberate for the message. A bound value reaches
// lowerCompare as a literal, so checkLiteralKind would refuse exactly the same binds -- but it
// would say the column was compared "against a non-numeric literal", sending a client who bound
// a value off to inspect SQL text that is perfectly fine. Naming the bind is worth one duplicated
// switch. The tests assert this wording rather than the shared phrase, so the check cannot go
// dead without a failure.
func (bv bound) check(p Param) error {
	if bv.null {
		return nil
	}
	switch p.Kind {
	case KindString:
		if bv.kind != KindString {
			return unsup("the string column %q was bound a non-string value", p.Column)
		}
	case KindInt, KindReal, KindTimestamp:
		if bv.kind != KindInt && bv.kind != KindReal {
			return unsup("the numeric column %q was bound a non-numeric value", p.Column)
		}
	case KindBool:
		if bv.kind != KindBool {
			return unsup("the boolean column %q was bound a non-boolean value", p.Column)
		}
	}
	return nil
}

// literal renders the bound value as a ClassAd literal.
func (bv bound) literal() string {
	switch bv.kind {
	case KindInt:
		return strconv.FormatInt(bv.i, 10)
	case KindReal:
		// 'g' with -1 precision round-trips exactly, so the comparison the store runs is
		// against the value the client bound rather than a rounding of it.
		return strconv.FormatFloat(bv.f, 'g', -1, 64)
	case KindString:
		return classAdString(bv.s)
	case KindBool:
		if bv.b {
			return "true"
		}
		return "false"
	}
	return "undefined"
}
