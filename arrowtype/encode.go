package arrowtype

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/PelicanPlatform/classad/classad"
)

// Turning ads into Arrow batches against a schema Derive already chose.
//
// Two rules govern every value, and both come from the measurement that produced the types
// (see the package comment):
//
//   - EVALUATE. An attribute held as an expression is a value once evaluated, and on a real AP
//     that is most of what looks like heterogeneity. MemoryUsage is an expression in 1996 of
//     2000 history ads and an integer in all 2000 once evaluated.
//   - COERCE WHERE THE COERCION IS EXACT, and null only what cannot be coerced. An integer in
//     a float column widens; a boolean in an integer column is 1 or 0; anything becomes text.
//     What does NOT coerce is a string into a number -- see coerce for why that one is
//     deliberate rather than missing.
//
// SECURITY -- READ THIS BEFORE WIRING IT UP. The encoder evaluates expressions, and an ad may
// hold a PUBLIC attribute whose value is an expression over a PRIVATE one (`Leak = ClaimId`).
// Evaluating that resolves the private value. db.PrivateConstraintRef's own comment records
// this hole and names the fix: the private value must be absent AT EVALUATION TIME for a
// reader not entitled to it, which the redacted decode walk already arranges for rendering.
//
// This encoder cannot enforce that, because by the time it has a *classad.ClassAd the decision
// has been made. So it is the CALLER's contract: the ads handed to Append must already be the
// reader's own view. Handing it privileged ads and serving the result to an unprivileged
// reader turns every Arrow read into the oracle that private_gate.go exists to prevent, and it
// is the same hazard class as the wire scan that shipped plaintext of encrypted attributes
// once already.

// Cause is why a cell was not stored as its declared type.
type Cause int

const (
	// CauseAbsent: the ad did not carry the attribute.
	CauseAbsent Cause = iota
	// CauseUndefined: the attribute evaluated to undefined.
	CauseUndefined
	// CauseError: the attribute evaluated to error -- distinct from undefined because it
	// means a value could not be produced, not that there was none.
	CauseError
	// CauseUncoercible: a value of a kind the column's type cannot represent.
	CauseUncoercible
)

func (c Cause) String() string {
	switch c {
	case CauseAbsent:
		return "absent"
	case CauseUndefined:
		return "undefined"
	case CauseError:
		return "error"
	case CauseUncoercible:
		return "uncoercible"
	}
	return "?"
}

// ColumnStats is what happened to one column while encoding.
//
// It exists because a column that silently nulls three percent of its values is the failure
// mode where plausible output hides a real defect -- a server-side clamp hid a unit bug here
// once. Callers are expected to surface these, not just collect them.
type ColumnStats struct {
	Name string
	// Values stored as the declared type with no conversion at all.
	Direct int
	// CoercedExact: converted, information-preserving (an integer into a float column
	// within 2^53, a boolean into an integer column, any scalar into a text column).
	CoercedExact int
	// CoercedLossy: converted in a way that may ALTER the value -- an integer past 2^53
	// into a float column. Non-zero here means the column is not telling the whole truth.
	CoercedLossy int
	// Nulls by cause.
	Nulls map[Cause]int
}

// Total is every cell accounted for.
func (s ColumnStats) Total() int {
	n := s.Direct + s.CoercedExact + s.CoercedLossy
	for _, v := range s.Nulls {
		n += v
	}
	return n
}

// EncodeOptions tunes encoding.
type EncodeOptions struct {
	// Strict fails the encode on a lossy coercion or an uncoercible value instead of
	// recording it and nulling. Off by default: a partial answer with the damage counted is
	// usually what a reader wants, and strict is what a correctness investigation sets.
	Strict bool
	// Alloc is the Arrow allocator. nil uses memory.DefaultAllocator, which is UNBOUNDED --
	// an in-process server should pass a bounded one so a wide batch fails its own request
	// rather than the process.
	Alloc memory.Allocator
}

// Encoder builds Arrow record batches from ads against a fixed schema.
//
// Not safe for concurrent use. The schema is fixed at construction and never varies, which is
// the whole point: a consumer cannot accept a column changing type partway through a stream.
type Encoder struct {
	schema   *arrow.Schema
	attrs    []string // attribute name per field, in field order
	builder  *array.RecordBuilder
	stats    []ColumnStats
	opts     EncodeOptions
	rows     int
	released bool
}

// NewEncoder prepares an encoder for the typed columns of decisions. Untyped decisions are
// skipped: they carry no column, and their source text belongs in the tail column the caller
// adds (this encoder does not add it, because what the tail should contain -- text, a map, or
// nothing -- is the caller's schema decision, not a per-value one).
func NewEncoder(decisions []Decision, opts EncodeOptions) *Encoder {
	alloc := opts.Alloc
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	var fields []arrow.Field
	var attrs []string
	var stats []ColumnStats
	for _, d := range decisions {
		if d.Verdict != Typed {
			continue
		}
		fields = append(fields, d.Field)
		attrs = append(attrs, d.Name)
		stats = append(stats, ColumnStats{Name: d.Name, Nulls: map[Cause]int{}})
	}
	schema := arrow.NewSchema(fields, nil)
	return &Encoder{
		schema:  schema,
		attrs:   attrs,
		builder: array.NewRecordBuilder(alloc, schema),
		stats:   stats,
		opts:    opts,
	}
}

// Schema is the fixed schema every batch this encoder produces will carry.
func (e *Encoder) Schema() *arrow.Schema { return e.schema }

// Rows is how many ads have been appended since the last NewRecord.
func (e *Encoder) Rows() int { return e.rows }

// Stats reports what happened per column, cumulative across every batch.
func (e *Encoder) Stats() []ColumnStats { return e.stats }

// Append encodes one ad as a row. It returns an error only under Strict.
func (e *Encoder) Append(ad *classad.ClassAd) error {
	for i, attr := range e.attrs {
		if err := e.appendCell(i, attr, ad); err != nil {
			return err
		}
	}
	e.rows++
	return nil
}

func (e *Encoder) appendCell(i int, attr string, ad *classad.ClassAd) error {
	b := e.builder.Field(i)
	st := &e.stats[i]

	// Absence and an evaluated undefined are different facts and are counted apart, even
	// though both become null: "the attribute was not there" and "it was there and had no
	// value" are different answers to a question about the data.
	if _, ok := ad.Lookup(attr); !ok {
		b.AppendNull()
		st.Nulls[CauseAbsent]++
		return nil
	}
	v := ad.EvaluateAttr(attr)
	switch v.Type() {
	case classad.UndefinedValue:
		b.AppendNull()
		st.Nulls[CauseUndefined]++
		return nil
	case classad.ErrorValue:
		b.AppendNull()
		st.Nulls[CauseError]++
		return nil
	}
	return e.coerce(b, st, attr, v)
}

// coerce writes one evaluated value into a builder, by the rules in the package comment.
//
// WHAT IS DELIBERATELY NOT HERE: parsing a string into a number. `"42"` in an integer column
// stays uncoercible. A string where a number was declared means the data is heterogeneous,
// which is information -- it belongs in the exception count and, if it is common, in the
// column's declared type. Parsing it would make a type error look like clean data, which is
// exactly what counting exceptions is for. (Numeric into TEXT goes the other way and is fine:
// text is a faithful rendering of any ClassAd value.)
func (e *Encoder) coerce(b array.Builder, st *ColumnStats, attr string, v classad.Value) error {
	switch bld := b.(type) {
	case *array.Int64Builder:
		switch v.Type() {
		case classad.IntegerValue:
			n, _ := v.IntValue()
			bld.Append(n)
			st.Direct++
		case classad.BooleanValue:
			ok, _ := v.BoolValue()
			bld.Append(b2i(ok))
			st.CoercedExact++
		case classad.RealValue:
			f, _ := v.RealValue()
			// An integral real is exactly an integer; a fractional one is not, and
			// truncating it would silently change the value.
			if n, exact := exactInt(f); exact {
				bld.Append(n)
				st.CoercedExact++
			} else {
				return e.uncoercible(bld, st, attr, v)
			}
		default:
			return e.uncoercible(bld, st, attr, v)
		}

	case *array.Float64Builder:
		switch v.Type() {
		case classad.RealValue:
			f, _ := v.RealValue()
			bld.Append(f)
			st.Direct++
		case classad.IntegerValue:
			n, _ := v.IntValue()
			bld.Append(float64(n))
			// Past 2^53 consecutive integers share a float64, so this alters the
			// value rather than rounding it. Derive refuses to declare a float column
			// when the measurement saw such a value, so reaching here means the data
			// moved since it was measured -- which is precisely what the count is for.
			if n > maxExactFloat64Int || n < -maxExactFloat64Int {
				st.CoercedLossy++
				if e.opts.Strict {
					return fmt.Errorf("%s: integer %d exceeds 2^53 in a float column", attr, n)
				}
			} else {
				st.CoercedExact++
			}
		case classad.BooleanValue:
			ok, _ := v.BoolValue()
			bld.Append(float64(b2i(ok)))
			st.CoercedExact++
		default:
			return e.uncoercible(bld, st, attr, v)
		}

	case *array.BooleanBuilder:
		if v.Type() != classad.BooleanValue {
			// Not even 0/1: a bool column got a number because the data disagrees with
			// the measurement, and saying so is more useful than inventing a truth
			// value for it.
			return e.uncoercible(bld, st, attr, v)
		}
		ok, _ := v.BoolValue()
		bld.Append(ok)
		st.Direct++

	case *array.StringBuilder:
		if v.Type() == classad.StringValue {
			s, _ := v.StringValue()
			bld.Append(s)
			st.Direct++
		} else {
			// Every other kind renders as its ClassAd form, which is what a text column
			// means here -- including lists and nested ads, whose text is the only
			// faithful scalar rendering they have.
			bld.Append(v.String())
			st.CoercedExact++
		}

	case *array.TimestampBuilder:
		switch v.Type() {
		case classad.IntegerValue:
			n, _ := v.IntValue()
			bld.Append(arrow.Timestamp(n))
			st.Direct++
		case classad.RealValue:
			// An epoch written as a real: seconds, truncated. Sub-second precision is
			// not representable in a second-resolution column, so it is a lossy
			// conversion and counted as one rather than quietly dropped.
			f, _ := v.RealValue()
			bld.Append(arrow.Timestamp(int64(f)))
			if _, exact := exactInt(f); exact {
				st.CoercedExact++
			} else {
				st.CoercedLossy++
				if e.opts.Strict {
					return fmt.Errorf("%s: %v loses sub-second precision in a seconds column", attr, f)
				}
			}
		default:
			return e.uncoercible(bld, st, attr, v)
		}

	default:
		return e.uncoercible(b, st, attr, v)
	}
	return nil
}

func (e *Encoder) uncoercible(b array.Builder, st *ColumnStats, attr string, v classad.Value) error {
	b.AppendNull()
	st.Nulls[CauseUncoercible]++
	if e.opts.Strict {
		return fmt.Errorf("%s: a %s value cannot be stored as %s", attr, classKind(v), b.Type())
	}
	return nil
}

// NewRecord returns the rows appended so far as a batch and starts a new one. The caller owns
// the record and must Release it.
func (e *Encoder) NewRecord() arrow.Record {
	rec := e.builder.NewRecord()
	e.rows = 0
	return rec
}

// Release frees the builder. Records already handed out are unaffected; they are the caller's.
func (e *Encoder) Release() {
	if !e.released {
		e.builder.Release()
		e.released = true
	}
}

// exactInt reports whether f is exactly an integer, and that integer. A non-finite float is
// never exact: NaN is a member of no order and an infinity has no integer form, and converting
// either to an int64 is implementation-defined.
func exactInt(f float64) (int64, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	if f != math.Trunc(f) {
		return 0, false
	}
	if f > float64(maxExactFloat64Int) || f < -float64(maxExactFloat64Int) {
		return 0, false
	}
	return int64(f), true
}

func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func classKind(v classad.Value) string {
	switch v.Type() {
	case classad.BooleanValue:
		return "bool"
	case classad.IntegerValue:
		return "int"
	case classad.RealValue:
		return "real"
	case classad.StringValue:
		return "string"
	case classad.ListValue:
		return "list"
	case classad.ClassAdValue:
		return "classad"
	case classad.UndefinedValue:
		return "undefined"
	case classad.ErrorValue:
		return "error"
	}
	return "?"
}
