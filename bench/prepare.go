package bench

import (
	"context"
	"fmt"

	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// Bringing a generated store to the state a maintained deployment is in, and reporting what that
// state actually buys.
//
// A FRESH STORE IS NOT A SLOW DEPLOYMENT, IT IS A DIFFERENT ONE. The indexes a table is
// configured for are not built until it is reindexed, the compression dictionary is untrained,
// and the columnar accelerator has no blocks. Measured on a real pool, a selective query visited
// every one of 100000 records before a reindex and 20000 after -- 4.4x, from maintenance alone.
// A benchmark that skips this measures the empty state and reports it as the deployment's.
//
// AND "COLUMNARIZED: YES" IS NOT THE ANSWER EITHER, because whether a read is served from columns
// depends on the READ and not only on the store. Measured over a prepared history archive, 5000
// sampled records each:
//
//	filter MemoryUsage > 100, plain projection   0% reassembled (10309 of 10310 column-decided)
//	project ResidentSetSize                      0% reassembled
//	project MemoryUsage                        100% reassembled
//	project Requirements                       100% reassembled
//
// The split is SHIPPING versus FILTERING an expression-valued attribute, not coverage. A computed
// column answers a filter on MemoryUsage from the columns and never touches a record. But an ad
// that SHIPS MemoryUsage must carry the stored expression -- `(ResidentSetSize + 1023) / 1024`,
// not the value it computes to, which is a property of the record rather than part of it -- so the
// record is rebuilt. Requirements behaves identically; the computed column's own input, being a
// stored literal, is free.
//
// That is inherent rather than a tuning gap, which is exactly why it has to be MEASURED and
// reported: two runs differing 100-fold here can both be correctly configured, and the flags say
// the same thing for both.

// PrepareOptions is what "a reasonable state" means for a generated table.
type PrepareOptions struct {
	// ValueIndexes and CategoricalIndexes are the attributes to index. Indexing decides whether
	// a selective constraint is a candidate walk or a full scan, which is usually the largest
	// single difference between two runs over the same data.
	ValueIndexes       []string
	CategoricalIndexes []string
	// HotAttrs are kept uncompressed for cheap access.
	HotAttrs []string
	// SampleMax is how many records the schema derivation samples, HotTopN how many attributes
	// it promotes to the hot set.
	SampleMax int
	HotTopN   int
	// RetrainSample is the dictionary training sample; 0 skips training. An untrained store pays
	// Huffman table setup per record, which is a large share of read CPU.
	RetrainSample int
}

// DefaultPrepareOptions is the steady state for a generated dataset of the given shape.
func DefaultPrepareOptions(shape Shape) PrepareOptions {
	o := PrepareOptions{
		SampleMax:     4000,
		HotTopN:       8,
		RetrainSample: 4000,
		HotAttrs:      []string{"Owner", "JobStatus", "ClusterId"},
	}
	switch shape {
	case ShapeHistory, ShapeEpochHistory:
		o.ValueIndexes = []string{"ClusterId", "CompletionDate"}
		o.CategoricalIndexes = []string{"Owner", "JobStatus"}
	default:
		o.ValueIndexes = []string{"ClusterId"}
		o.CategoricalIndexes = []string{"Owner", "JobStatus"}
	}
	return o
}

// PrepareReport says what was actually done, because each step can decline and a benchmark that
// assumed it succeeded reports the unprepared state under the prepared one's name.
type PrepareReport struct {
	IndexesAdded   bool
	HotAttrs       []string
	SchemaScanOn   bool
	DictTrained    bool
	DictSampleUsed int
	Notes          []string
}

// PrepareArchive brings an append-only table to steady state.
func PrepareArchive(t *db.ArchiveTable, o PrepareOptions) PrepareReport {
	var r PrepareReport
	if len(o.CategoricalIndexes) > 0 || len(o.ValueIndexes) > 0 {
		r.IndexesAdded = t.AddIndex(o.CategoricalIndexes, o.ValueIndexes)
		if !r.IndexesAdded {
			r.Notes = append(r.Notes, "AddIndex declined: the table's index set was already what was asked for, or the attributes are not indexable")
		}
	}
	if len(o.HotAttrs) > 0 {
		r.HotAttrs = t.AddHotAttrs(o.HotAttrs...)
	}
	r.SchemaScanOn = t.BuildAndEnableSchemaScan(o.SampleMax, o.HotTopN)
	if !r.SchemaScanOn {
		r.Notes = append(r.Notes, "the columnar accelerator did not build: every projected read is served from records")
	}
	return r
}

// PrepareTable brings a mutable table to steady state.
func PrepareTable(d *db.DB, o PrepareOptions) PrepareReport {
	var r PrepareReport
	if len(o.CategoricalIndexes) > 0 || len(o.ValueIndexes) > 0 {
		r.IndexesAdded = d.AddIndex(o.CategoricalIndexes, o.ValueIndexes)
		if !r.IndexesAdded {
			r.Notes = append(r.Notes, "AddIndex declined: the table's index set was already what was asked for, or the attributes are not indexable")
		}
	}
	if len(o.HotAttrs) > 0 {
		r.HotAttrs = d.AddHotAttrs(o.HotAttrs...)
	}
	r.SchemaScanOn = d.EnableSchemaScan(o.SampleMax, o.HotTopN)
	if !r.SchemaScanOn {
		r.Notes = append(r.Notes, "the columnar accelerator did not build: every projected read is served from records")
	}
	if o.RetrainSample > 0 {
		if n, err := d.RetrainDict(o.RetrainSample); err == nil {
			r.DictTrained = true
			r.DictSampleUsed = n
		} else {
			r.Notes = append(r.Notes, "dictionary training failed: "+err.Error())
		}
	}
	return r
}

// Escape is what a sampled projected read actually cost, measured rather than inferred.
type Escape struct {
	// Measured is false when the server did not return a breakdown (an older server, or an op
	// that does not thread stats). Every other field is then zero, which must NOT be read as
	// "nothing escaped".
	Measured           bool
	Visited            int
	Reassembled        int
	ClosureReassembled int
	ColumnDecided      int
	RowsMatched        int
}

// Rate is the share of visited records rebuilt from the arena rather than served from columns.
// This is the number that separates two runs whose reported coverage is identical.
func (e Escape) Rate() float64 {
	if !e.Measured || e.Visited == 0 {
		return 0
	}
	return float64(e.Reassembled) / float64(e.Visited)
}

// ClosureRate is the share rebuilt specifically because the projection named an expression
// reaching outside itself -- the avoidable half, fixable by widening the projection.
func (e Escape) ClosureRate() float64 {
	if !e.Measured || e.Visited == 0 {
		return 0
	}
	return float64(e.ClosureReassembled) / float64(e.Visited)
}

// MeasureEscape runs a bounded projected read and reports what the server did with it.
//
// Bounded because this is a probe, not the measurement: it exists to say what the sweep is about
// to measure, and a probe that read the whole table would cost as much as the sweep.
func MeasureEscape(ctx context.Context, c *dbrpc.Client, table, constraint string, projection []string, sample int) (Escape, error) {
	if sample <= 0 {
		sample = 5000
	}
	var st collections.ScanStats
	err := c.QueryRawProjectRefsStreamStats(ctx, table, constraint, projection, sample,
		func(string) bool { return true }, &st)
	if err != nil {
		return Escape{}, fmt.Errorf("sampling %q: %w", table, err)
	}
	if st.RecordsVisited == 0 {
		return Escape{}, nil // nothing visited: no breakdown to report, and not an escape of 0
	}
	return Escape{
		Measured:           true,
		Visited:            st.RecordsVisited,
		Reassembled:        st.RecordsReassembled,
		ClosureReassembled: st.RecordsClosureReassembled,
		ColumnDecided:      st.RecordsColumnDecided,
		RowsMatched:        st.RowsMatched,
	}, nil
}
