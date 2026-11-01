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
	// Complete reports that the sample was the WHOLE table rather than a capped draw
	// from it. It is what makes a non-nullable column sound.
	//
	// Arrow's nullable flag is a promise to the reader, and a sample cannot support it:
	// an attribute present in every one of 10,000 sampled records may still be absent
	// from the millionth, and the encoder would then append a null into a column
	// declared to have none. Some readers check and reject; others trust it and are
	// quietly wrong. Nothing goes wrong while the sample happens to cover the table,
	// so this stays invisible until the first table that outgrows the cap. So density is only ever declared when it was observed over
	// everything -- see FromProfiles, which can tell because a sample that returned
	// fewer records than it asked for had nothing left to draw.
	Complete bool
}

// isNullable reports whether the column must admit nulls.
//
// Anything the measurement saw missing or null makes it nullable outright. Everything else
// hinges on whether the sample WAS the table: over a capped sample, "present in every record
// I looked at" is not "present in every record", and declaring non-nullable on that basis
// hands the reader a promise the encoder may then break.
func isNullable(m Measured) bool {
	if m.Present < m.Sampled || m.Nulls > 0 {
		return true
	}
	// An expression is not a non-null, for the same reason it is not a type: the
	// measurement records that the attribute HOLDS an expression, and what that expression
	// evaluates to is decided at encode time, against an ad that may not carry what it
	// refers to. Both undefined and error are nulls. So an attribute holding any expression
	// can admit a null however complete the sample was -- RequestMemory is an integer in
	// almost every record and an expression in a few, and those few evaluate to undefined.
	if m.ValueClasses["expr"] > 0 {
		return true
	}
	return !m.Complete
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

// Override is a declared decision about one attribute, for the cases inference cannot reach.
//
// Two attributes on a real AP show why this has to exist. MemoryUsage is an expression in 1996
// of 2000 records and evaluates to an integer in all of them -- a derivation whose VALUE is the
// content. Requirements is an expression in 1995 of 2000 and evaluates to a boolean -- a policy
// whose TEXT is the content, and whose boolean is meaningless without a machine to match
// against. They are indistinguishable by shape: both predominantly expressions, both evaluating
// to one clean kind. The difference is semantic, so no amount of measuring separates them and a
// heuristic that tried would be guessing.
//
// So the measurement decides what it can, and this is where the rest is stated. An override is
// a declaration, not a hint: it wins.
type Override struct {
	// Exclude gives the attribute no typed column whatever the measurement says. Its text
	// still reaches a reader through the tail.
	Exclude bool
	// Kind forces the column's type: "int", "real", "bool", "string" or "timestamp".
	// Empty leaves the type to the measurement.
	//
	// Forcing a kind the data does not hold is allowed and is not silent: the encoder
	// counts every value it then cannot store, and the Decision's Detail records what the
	// measurement actually saw, so the disagreement is visible rather than inferred from an
	// unexpectedly empty column.
	Kind string
	// Nullable forces nullability; nil leaves it to the measurement. Forcing non-nullable
	// on a column that turns out to hold a null makes the encoder fail that row rather than
	// quietly widen the declaration.
	Nullable *bool
}

// Options tunes the derivation.
type Options struct {
	// Overrides are declared decisions, keyed by attribute name (compared
	// case-insensitively, as ClassAd names are). They are consulted before the
	// measurement and they win.
	Overrides map[string]Override

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

// override returns the declared decision for an attribute, if any.
func (o Options) override(name string) (Override, bool) {
	if len(o.Overrides) == 0 {
		return Override{}, false
	}
	ov, ok := o.Overrides[strings.ToLower(name)]
	return ov, ok
}

// kindType maps an override's kind name to its Arrow type.
func kindType(kind string) (arrow.DataType, bool) {
	if kind == "timestamp" {
		return arrow.FixedWidthTypes.Timestamp_s, true
	}
	return scalarType(kind)
}

func (o Options) isTime(name string) bool {
	if len(o.TimeAttrs) == 0 {
		return false
	}
	return o.TimeAttrs[strings.ToLower(name)]
}

// AN EXPRESSION IS NOT A TYPE, any more than undefined is.
//
// It is an unevaluated form that BECOMES a type once evaluated, and the encoder evaluates
// (§5.1), so a record holding an expression contributes a value of whatever that expression
// yields -- not a distinct kind the column has to accommodate.
//
// Counting it as a competing class is a bug with teeth. Measured on a real AP, RequestMemory is
// an integer in 1994 records and an expression in ONE; RequestDisk the same; LeaveJobInQueue is
// a boolean in 1999 and an expression in one. Treating that one record as a second kind made
// each of them a TEXT column -- an integer attribute a reader cannot do arithmetic on, with no
// error anywhere to say why. That is precisely the failure this package exists to prevent: a
// plausible schema hiding a type error.
//
// What the expression share still decides is whether the attribute is typed AT ALL: an
// attribute that is PREDOMINANTLY an expression is a formula whose text is the point
// (Requirements, PeriodicHold), and Derive leaves those untyped. That check reads the raw
// classes, below; this function is only about the kinds a typed column must hold.
func typeClasses(m Measured) (names []string, only string) {
	for name, n := range m.ValueClasses {
		if n > 0 && name != "expr" {
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
	names, only := typeClasses(m)

	// A declaration wins over the measurement, and says so in its reason so an EXPLAIN or a
	// catalog report shows which columns were decided rather than derived.
	if ov, ok := opts.override(m.Name); ok {
		if ov.Exclude {
			return Decision{Name: m.Name, Verdict: Untyped, Reason: "override-excluded",
				Detail: "excluded by declaration; measured " + measuredAs(m)}
		}
		if ov.Kind != "" {
			dt, valid := kindType(ov.Kind)
			if !valid {
				return Decision{Name: m.Name, Verdict: Untyped, Reason: "override-invalid",
					Detail: "declared kind " + ov.Kind + " is not one of int, real, bool, string, timestamp"}
			}
			nullable := isNullable(m)
			if ov.Nullable != nil {
				nullable = *ov.Nullable
			}
			// Not through typed(): the epoch rule must not reach a declared kind.
			// Declaring int for an attribute that is also listed in TimeAttrs is how a
			// caller says "I know it is epoch seconds and I want the raw number", and a
			// declaration that TimeAttrs could overrule would not be a declaration.
			return Decision{
				Name:    m.Name,
				Verdict: Typed,
				Field:   arrow.Field{Name: m.Name, Type: dt, Nullable: nullable, Metadata: fieldMeta(m, "override-kind")},
				Reason:  "override-kind",
				Detail:  "declared as " + ov.Kind + "; measured " + measuredAs(m),
			}
		}
		if ov.Nullable != nil {
			// Nullability alone was declared; the kind still comes from the
			// measurement, so fall through and fix it up afterwards.
			d := deriveFromMeasurement(m, opts, names, only)
			if d.Verdict == Typed {
				d.Field.Nullable = *ov.Nullable
				d.Reason += "+override-nullable"
			}
			return d
		}
	}
	return deriveFromMeasurement(m, opts, names, only)
}

// measuredAs renders what the measurement saw, for a declaration's detail line: a forced kind
// the data does not hold should be visible as a disagreement, not inferred from an empty column.
func measuredAs(m Measured) string {
	if len(m.ValueClasses) == 0 {
		return "no values"
	}
	return strings.Join(sorted(keys(m.ValueClasses)), ",")
}

func deriveFromMeasurement(m Measured, opts Options, names []string, only string) Decision {

	// Nothing to type. Either every sampled value was a null, or every one was an expression
	// whose evaluated kind this measurement does not record -- in both cases declaring a type
	// would invite a reader to treat the column as meaningful, and declaring text would give
	// a column of empty strings.
	if len(names) == 0 {
		reason, detail := "no-value-observed", "every sampled value was undefined or error, so no type was observed"
		if m.ValueClasses["expr"] > 0 {
			reason = "expression-valued"
			detail = "every sampled value was an expression, so no evaluated kind was observed"
		}
		return Decision{Name: m.Name, Verdict: Untyped, Reason: reason, Detail: detail}
	}

	// Predominantly an expression. Evaluating it yields a value, and on real data that value
	// is not what anyone wants; the source text is, and the tail preserves it.
	if dominant(m) == "expr" {
		return Decision{Name: m.Name, Verdict: Untyped, Reason: "expression-valued",
			Detail: "held as an expression in most records; its text is the useful form, so it stays in the tail"}
	}

	nullable := isNullable(m)

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
	return Decision{
		Name:    m.Name,
		Verdict: Typed,
		Field:   arrow.Field{Name: m.Name, Type: dt, Nullable: nullable, Metadata: fieldMeta(m, reason)},
		Reason:  reason,
		Detail:  detail,
	}
}

// fieldMeta records on the field how its type was reached, so a consumer reading only the
// schema can still see whether a column was derived or declared.
func fieldMeta(m Measured, reason string) arrow.Metadata {
	return arrow.MetadataFrom(map[string]string{
		"htcondor:reason":  reason,
		"htcondor:classes": strings.Join(sorted(keys(m.ValueClasses)), ","),
	})
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
	// ClassAd attribute names are case-insensitive, so two measurements that differ only in
	// case are one attribute. Emitting both would put two columns in the schema for it,
	// with whatever types each measurement happened to reach -- a conflict presented as two
	// facts. The first wins and the rest are reported, because which spelling is "right" is
	// not this package's to decide, but publishing both is wrong either way.
	seen := make(map[string]string, len(ms))
	for _, m := range ms {
		key := strings.ToLower(m.Name)
		if first, dup := seen[key]; dup {
			decisions = append(decisions, Decision{
				Name: m.Name, Verdict: Untyped, Reason: "duplicate-name",
				Detail: "the same attribute was already measured as " + first +
					"; ClassAd names are case-insensitive, so one column covers both",
			})
			continue
		}
		seen[key] = m.Name
		d := Derive(m, opts)
		decisions = append(decisions, d)
		if d.Verdict == Typed {
			fields = append(fields, d.Field)
		}
	}
	return arrow.NewSchema(fields, nil), decisions
}
