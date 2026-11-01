// Package arrowtype decides the Arrow type of a ClassAd attribute from a measurement of what
// that attribute actually holds.
//
// A ClassAd attribute has no declared type: two records of the same attribute can genuinely
// differ, and the store's own layout chooses widths to minimize bytes rather than to present
// one type. Arrow, on the other side, fixes a schema for a whole stream. Something has to
// decide, once per attribute, what that stream will say -- and the decision has to be made from
// a measurement rather than from the first value that happens to arrive, which is how the two
// existing clients get it wrong today (the DB-API driver types a column from its first non-null
// value; the Grafana plugin parses the whole column and bails to string on the first
// non-numeric, so the same mixed column is described differently by each and correctly by
// neither).
//
// This package is that decision and nothing else: it reads a measurement, applies the rules,
// and reports what it chose AND WHY. It does no I/O, holds no state and never looks at a value.
//
// The rules come from measuring a production AP (2000 ads from each of a live queue and a
// history table). What that measurement found, and what therefore shaped these rules:
//
//   - After evaluation, 330 of 337 attributes hold exactly one kind of value. The mixed ones
//     are ALL int/real -- not one string/number or list/scalar mix in either table.
//   - int/real mixing is mostly not mixed data at all. ClassAd writes an integral float
//     without a decimal point, so a float-valued attribute arrives as a mix of int and real
//     nodes. BytesRecvd was 67 int to 1933 real. These are float columns.
//   - No integer anywhere exceeded 2^53, so int/real mixes convert to float64 losing nothing.
//     The check stays, because "no integer past 2^53 today" is not "no integer past 2^53", and
//     it costs one comparison.
//   - Stored `undefined` is the most common thing that LOOKS like heterogeneity and is not:
//     one attribute was 25 strings against 1975 undefined. It is a null, not a competing type.
//   - An attribute that is predominantly an expression evaluates to something useless.
//     Requirements evaluated to a boolean in 1685 of 2000 ads, plus 141 errors and 174
//     undefined, because a match expression with no machine to match against is not a
//     meaningful question. Its TEXT is what a reader wants, so it gets no typed column.
package arrowtype

import (
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// maxExactFloat64Int is the largest integer magnitude a float64 holds exactly. Above it,
// consecutive integers share a float64, so converting loses values outright rather than
// rounding them.
const maxExactFloat64Int = int64(1) << 53

// Measured is what a profile says about one attribute. It is deliberately a local type and not
// the store's profile struct: the decision needs only these facts, and stating them explicitly
// keeps the rules readable and the package free of a storage dependency. See FromProfile for
// the adapter.
type Measured struct {
	Name string
	// Sampled is the records in the sample; Present is how many held this attribute.
	// Present < Sampled is an absence rate.
	Sampled int
	Present int
	// ValueClasses counts the values by class, with undefined and error ALREADY EXCLUDED --
	// they carry no type (see the package comment). Keys are "int", "real", "bool",
	// "string", "list", "classad", "expr".
	ValueClasses map[string]int
	// Nulls is the stored undefined and error values: typed as nothing, served as null.
	Nulls int
	// LossyAsFloat is how many integer values exceed 2^53 in magnitude.
	LossyAsFloat int
}

// Verdict is what was decided for one attribute.
type Verdict int

const (
	// Typed: the attribute gets a typed Arrow column.
	Typed Verdict = iota
	// Untyped: the attribute gets no column of its own and belongs in the tail, which
	// preserves its source text. NOT an error -- for an expression-valued attribute it is
	// the right answer, not a failure to find one.
	Untyped
)

// Decision is the outcome for one attribute, with the reason it was reached. The reason is not
// decoration: a column that silently became a string, or silently vanished, is the failure mode
// where a plausible schema hides a type error, so every decision says why and callers are
// expected to surface it (in EXPLAIN, in a catalog report, in a log).
type Decision struct {
	Name    string
	Verdict Verdict
	// Field is the Arrow field, valid when Verdict is Typed.
	Field arrow.Field
	// Reason is a short stable identifier, suitable for grouping ("single-class",
	// "int-real-widened", "expression-valued", "mixed-kinds", ...).
	Reason string
	// Detail is a human sentence explaining this attribute's case.
	Detail string
}

// Options tunes the derivation.
type Options struct {
	// TimeAttrs are attributes whose integer values are unix-epoch seconds, declared rather
	// than guessed. A match here turns an int or real column into timestamp[s] so downstream
	// date functions work. Compared case-insensitively, because ClassAd attribute names are
	// case-insensitive.
	TimeAttrs map[string]bool
	// StrictMixedKinds refuses an attribute whose kinds genuinely disagree instead of
	// falling back to a text column. Off by default: a text column is a faithful
	// representation and a correctness investigation is what wants the refusal.
	StrictMixedKinds bool
}

func (o Options) isTime(name string) bool {
	if len(o.TimeAttrs) == 0 {
		return false
	}
	return o.TimeAttrs[strings.ToLower(name)]
}

// classes returns the value-class names present, and the single one if there is exactly one.
func classes(m Measured) (names []string, only string) {
	for name, n := range m.ValueClasses {
		if n > 0 {
			names = append(names, name)
		}
	}
	if len(names) == 1 {
		only = names[0]
	}
	return names, only
}

// dominant returns the most frequent value class, ties broken toward the lexically smaller name
// so the result does not depend on map order.
func dominant(m Measured) string {
	best, bestN := "", -1
	for name, n := range m.ValueClasses {
		if n > bestN || (n == bestN && name < best) {
			best, bestN = name, n
		}
	}
	return best
}

// Derive decides one attribute's Arrow type.
func Derive(m Measured, opts Options) Decision {
	names, only := classes(m)

	// Nothing but nulls: there is no type here. Declaring one would invite a reader to treat
	// the column as meaningful; declaring it as text would be a column of empty strings.
	if len(names) == 0 {
		return Decision{Name: m.Name, Verdict: Untyped, Reason: "no-value-observed",
			Detail: "every sampled value was undefined or error, so no type was observed"}
	}

	// Predominantly an expression. Evaluating it yields a value, and on real data that value
	// is not what anyone wants; the source text is, and the tail preserves it.
	if dominant(m) == "expr" {
		return Decision{Name: m.Name, Verdict: Untyped, Reason: "expression-valued",
			Detail: "held as an expression in most records; its text is the useful form, so it stays in the tail"}
	}

	nullable := m.Present < m.Sampled || m.Nulls > 0

	if only != "" {
		dt, ok := scalarType(only)
		if !ok {
			// A list or a nested ad: already a value, with no scalar form to coerce to.
			return Decision{Name: m.Name, Verdict: Untyped, Reason: "composite-value",
				Detail: "holds " + only + " values, which have no scalar Arrow form"}
		}
		return typed(m, opts, dt, nullable, "single-class",
			"every sampled value was "+only)
	}

	// The one mixed case that resolves: int and real together. ClassAd writes an integral
	// float without a decimal point, so this is usually one float-valued attribute rather
	// than two kinds of data -- but only if every integer survives the conversion.
	if isIntRealOnly(names) {
		if m.LossyAsFloat == 0 {
			return typed(m, opts, arrow.PrimitiveTypes.Float64, nullable, "int-real-widened",
				"mixes integer and real values; every integer is within 2^53, so float64 loses nothing")
		}
		return mixed(m, opts, nullable, "int-real-lossy",
			"mixes integer and real values, and some integers exceed 2^53, so float64 would alter them")
	}

	return mixed(m, opts, nullable, "mixed-kinds",
		"holds more than one kind of value ("+strings.Join(sorted(names), ", ")+")")
}

// mixed handles an attribute whose kinds do not reconcile: a text column carrying each value's
// ClassAd form, or a refusal under StrictMixedKinds.
//
// Measured against a real AP this case did not occur at all -- every mixed attribute was
// int/real and widened. It is kept because "did not occur in one sample" is not "cannot occur",
// and because a text column is a faithful representation of any ClassAd value. It should not be
// treated as the common case.
func mixed(m Measured, opts Options, nullable bool, reason, detail string) Decision {
	if opts.StrictMixedKinds {
		return Decision{Name: m.Name, Verdict: Untyped, Reason: reason + "-refused",
			Detail: detail + "; refused because StrictMixedKinds is set"}
	}
	return typed(m, opts, arrow.BinaryTypes.String, nullable, reason, detail)
}

// typed builds the field, applying the epoch-to-timestamp declaration last so it is visible as
// its own step rather than buried in each branch.
func typed(m Measured, opts Options, dt arrow.DataType, nullable bool, reason, detail string) Decision {
	if opts.isTime(m.Name) {
		switch dt.ID() {
		case arrow.INT64, arrow.FLOAT64:
			dt = arrow.FixedWidthTypes.Timestamp_s
			reason += "+epoch"
			detail += "; declared as unix-epoch seconds, so it is a timestamp"
		}
	}
	md := arrow.MetadataFrom(map[string]string{
		"htcondor:reason":  reason,
		"htcondor:classes": strings.Join(sorted(keys(m.ValueClasses)), ","),
	})
	return Decision{
		Name:    m.Name,
		Verdict: Typed,
		Field:   arrow.Field{Name: m.Name, Type: dt, Nullable: nullable, Metadata: md},
		Reason:  reason,
		Detail:  detail,
	}
}

func scalarType(class string) (arrow.DataType, bool) {
	switch class {
	case "int":
		return arrow.PrimitiveTypes.Int64, true
	case "real":
		return arrow.PrimitiveTypes.Float64, true
	case "bool":
		return arrow.FixedWidthTypes.Boolean, true
	case "string":
		return arrow.BinaryTypes.String, true
	}
	return nil, false
}

func isIntRealOnly(names []string) bool {
	if len(names) != 2 {
		return false
	}
	a, b := names[0], names[1]
	return (a == "int" && b == "real") || (a == "real" && b == "int")
}

func keys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func sorted(in []string) []string {
	out := append([]string(nil), in...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// Schema derives a whole table's Arrow schema. Attributes that get no typed column are
// returned in their decisions, so a caller can report what it left out and route those to the
// tail column rather than discovering the omission later.
//
// Field order follows the input order, which the store reports most-present-first, so the
// attributes a reader is most likely to want come first and the order is stable for the same
// data. A schema whose field ORDER changed between calls on unchanged data would invalidate a
// consumer's cached catalog for no reason.
func Schema(ms []Measured, opts Options) (*arrow.Schema, []Decision) {
	decisions := make([]Decision, 0, len(ms))
	fields := make([]arrow.Field, 0, len(ms))
	for _, m := range ms {
		d := Derive(m, opts)
		decisions = append(decisions, d)
		if d.Verdict == Typed {
			fields = append(fields, d.Field)
		}
	}
	return arrow.NewSchema(fields, nil), decisions
}
