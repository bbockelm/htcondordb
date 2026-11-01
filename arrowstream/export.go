// Package arrowstream reads a table out of an htcondordb daemon and writes it as an Arrow IPC
// stream.
//
// It is the first consumer of the type machinery, and the shape the Flight SQL DoGet handler
// will take: measure what the attributes hold, declare one type per attribute from that, then
// encode the scan against it. Having it as a plain io.Writer export first means the whole read
// path can be exercised -- and its output handed to DuckDB, pandas or polars -- with no gRPC
// anywhere in the picture.
//
// NOT THE FAST PATH, deliberately. The scan hands back each ad as TEXT and this parses it, so
// a value crosses several representations before it reaches a column. That is the known cost
// of building correctness first; the columnar encoder that skips it is separate work, and it
// has to be measured against the raw-wire read rather than against this, or the comparison
// will flatter it.
//
// PRIVACY: the ads come from the daemon already redacted to what the connection may see, which
// is what arrowtype's encoder requires and cannot check for itself. Serving a stream produced
// from one connection's ads to a different, less privileged reader would defeat that.
package arrowstream

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// defaultBatchRows is how many rows accumulate before a record batch is written. Arrow's own
// guidance is batches large enough to amortize per-batch overhead and small enough to stay in
// cache; a few thousand rows of a wide job ad is the usual landing zone, and the caller can
// override it when it knows better.
const defaultBatchRows = 2048

// defaultSampleMax is how many records the type profile looks at. The schema is a declaration
// about a whole table, so the sample wants to be large enough to see an attribute's minority
// kinds -- on a real AP the attributes that matter most had their second kind in well under 1%
// of records.
const defaultSampleMax = 10000

// Options configures an export.
type Options struct {
	// Table is the table to read; empty means the daemon's default table.
	Table string
	// Constraint filters the scan. Empty reads everything -- the scan requires an
	// expression, so this becomes "true" rather than being passed through as "", which the
	// daemon rejects as a syntax error.
	Constraint string
	// Limit caps the rows read; 0 is unlimited.
	Limit int
	// SampleMax is how many records the type profile examines (0 = default).
	SampleMax int
	// BatchRows is rows per record batch (0 = default).
	BatchRows int
	// Derive and Encode tune the type decision and the encoding. Derive.Overrides is
	// where a declaration goes for the attributes inference cannot decide.
	Derive arrowtype.Options
	Encode arrowtype.EncodeOptions
}

// Result reports what an export produced.
//
// Decisions and Stats are not diagnostics to discard: between them they say which attributes
// got no column and why, and what had to be coerced or nulled to fit the ones that did. An
// export that silently dropped a third of a table's attributes looks exactly like one that did
// not, unless someone reads these.
type Result struct {
	Schema    *arrow.Schema
	Rows      int
	Batches   int
	Decisions []arrowtype.Decision
	Stats     []arrowtype.ColumnStats
	// Unparsed counts rows the scan returned that could not be parsed as a ClassAd. Not
	// expected to be non-zero; if it is, the stream is incomplete and says so rather than
	// quietly returning fewer rows.
	Unparsed int
}

// Untyped returns the attributes that got no column, with the reason for each. The caller
// decides what to do about them -- route them to a tail column, report them, or ignore them --
// but it cannot do any of that without being told which they are.
func (r *Result) Untyped() map[string]string {
	out := map[string]string{}
	for _, d := range r.Decisions {
		if d.Verdict != arrowtype.Typed {
			out[d.Name] = d.Reason
		}
	}
	return out
}

// Export streams one table to w as Arrow IPC.
//
// It measures first and scans second, which means two passes over different data: the profile
// samples, the scan reads what the constraint matches. A schema derived from a sample of the
// whole table can therefore meet a value the sample did not contain -- which is not a failure
// but the ordinary case the encoder's coercion and counters exist for. Deriving the schema
// from the constraint's own matches instead would make the schema vary by query, and a
// consumer that caches a table's schema could not then trust it.
func Export(ctx context.Context, c *dbrpc.Client, w io.Writer, opts Options) (*Result, error) {
	table := opts.Table
	if table == "" {
		table = dbrpc.DefaultTable
	}
	sampleMax := opts.SampleMax
	if sampleMax <= 0 {
		sampleMax = defaultSampleMax
	}
	batchRows := opts.BatchRows
	if batchRows <= 0 {
		batchRows = defaultBatchRows
	}
	constraint := opts.Constraint
	if constraint == "" {
		constraint = "true"
	}

	rep, err := c.AttrTypeProfiles(ctx, table, sampleMax)
	if err != nil {
		return nil, fmt.Errorf("profile %s: %w", table, err)
	}
	// db.AttrTypeProfile is a type alias of collections.AttrTypeProfile, so the report's
	// profiles feed the adapter directly.
	//
	// A sample that came back SHORT of what it asked for ran out of records rather than
	// hitting its cap, so it saw the whole table -- which is the only sound basis for
	// declaring a column non-nullable. A sample that filled its cap may have missed a
	// record where an attribute is absent, and a non-nullable column the encoder then
	// appends a null into is a promise broken to the reader.
	complete := rep.Sampled < sampleMax
	schema, decisions := arrowtype.Schema(
		arrowtype.FromProfilesComplete(rep.Profiles, rep.Sampled, complete), opts.Derive)

	enc := arrowtype.NewEncoder(decisions, opts.Encode)
	defer enc.Release()

	iw := ipc.NewWriter(w, ipc.WithSchema(schema))
	res := &Result{Schema: schema, Decisions: decisions}

	// flush writes the pending batch. Writing a zero-row batch is avoided: it is legal
	// Arrow but it is noise in a stream, and some readers treat it as the end.
	var flushErr error
	flush := func() bool {
		if enc.Rows() == 0 {
			return true
		}
		rec := enc.NewRecord()
		defer rec.Release()
		if err := iw.Write(rec); err != nil {
			flushErr = err
			return false
		}
		res.Batches++
		return true
	}

	var appendErr error
	scanErr := c.QueryTableStream(ctx, table, constraint, opts.Limit, func(row string) bool {
		// QueryTable streams ads in the BRACKETED new-ClassAd format (ClassAd.String);
		// only the projection ops emit old-ClassAd text. Using the wrong parser here
		// fails every row, which Unparsed makes visible rather than silently empty.
		ad, perr := classad.Parse(row)
		if perr != nil {
			res.Unparsed++
			return true
		}
		if err := enc.Append(ad); err != nil {
			appendErr = err // only under Encode.Strict
			return false
		}
		res.Rows++
		if enc.Rows() >= batchRows {
			return flush()
		}
		return true
	})

	switch {
	case appendErr != nil:
		return res, appendErr
	case flushErr != nil:
		return res, flushErr
	case scanErr != nil:
		return res, fmt.Errorf("scan %s: %w", table, scanErr)
	}
	if !flush() {
		return res, flushErr
	}
	if err := iw.Close(); err != nil {
		return res, err
	}
	res.Stats = enc.Stats()
	return res, nil
}
