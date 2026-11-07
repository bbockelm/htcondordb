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
	// EscapeProbeAttrs and EscapeProbeSample configure the convergence probe: which projection
	// to sample and how many records. Defaults are a plain stored-attribute projection, which
	// is what shows columnar coverage rather than the cost of shipping an expression.
	EscapeProbeAttrs  []string
	EscapeProbeSample int
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
	// ColumnarPasses is how many schema-scan passes were run. One is not enough on a large
	// table: a pass rewrites a bounded number of sealed segments, so a single call leaves the
	// rest to be reassembled on every read. See PrepareArchive.
	ColumnarPasses int
	// ResidualEscape is the share of sampled records still reassembled after preparation, 0
	// when it converged. Reported because it is the number that decides what the sweep is
	// about to measure, and because the accelerator's own coverage figure does not show it.
	ResidualEscape float64
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
	// REPEATED UNTIL IT CONVERGES, which is what "steady state" means here.
	//
	// A schema-scan pass rewrites a BOUNDED number of sealed segments into columnar form -- 64
	// by default -- so that enabling the accelerator over an existing archive does not stall on
	// hours of I/O. A deployment converges over several maintenance intervals; a benchmark that
	// called it once would measure a table that is mostly NOT columnar and report it as the
	// prepared state.
	//
	// The accelerator's own coverage figure does not show this: a 478-segment archive reports
	// 478 of 478 covered after the first pass while 87% of its records are still reassembled on
	// every read. Measured here instead, by sampling the read, because that is the number that
	// decides what the sweep measures.
	// DRIVEN BY THE SEGMENT BACKLOG, not by probing between passes.
	//
	// Segments are converted oldest-first and an archive reads newest-first, so a bounded probe
	// between passes reads the segments converted LAST and reports no improvement until the
	// final pass -- it looks converged after one pass and after seven alike. The backlog is the
	// honest signal: a pass converts at most columnarPassBudget segments, so a table with N
	// sealed segments needs about N/budget of them.
	r.SchemaScanOn = t.BuildAndEnableSchemaScan(o.SampleMax, o.HotTopN)
	r.ColumnarPasses = 1
	if r.SchemaScanOn {
		sealed := t.SchemaScanInfo().SealedSegments
		want := 1
		if sealed > 0 {
			want = (sealed+columnarPassBudget-1)/columnarPassBudget + 1
		}
		if want > maxColumnarPasses {
			want = maxColumnarPasses
		}
		for r.ColumnarPasses < want {
			if !t.BuildAndEnableSchemaScan(o.SampleMax, o.HotTopN) {
				break
			}
			r.ColumnarPasses++
		}
		// One probe over the WHOLE table, after the passes. Bounded probes are biased by the
		// read order; this is the number that describes what the sweep will measure, and it
		// costs one scan at setup rather than one per pass.
		if esc, ok := sampleArchiveEscape(t, o.EscapeProbeAttrs, 0); ok {
			r.ResidualEscape = esc
		}
	}
	if !r.SchemaScanOn {
		r.Notes = append(r.Notes, "the columnar accelerator did not build: every projected read is served from records")
	} else if r.ResidualEscape > columnarResidualWarn {
		r.Notes = append(r.Notes, fmt.Sprintf(
			"columnarization did not converge in %d passes: %.1f%% of sampled records are still rebuilt from the arena, so this table is not in the state a maintained deployment reaches",
			r.ColumnarPasses, 100*r.ResidualEscape))
	}
	return r
}

// maxColumnarPasses bounds the convergence loop. Each pass rewrites a bounded number of segments,
// so a very large table needs several; the cap stops a table that cannot converge from looping
// forever, and the residual is reported rather than hidden.
const maxColumnarPasses = 32

// columnarPassBudget mirrors how many sealed segments one schema-scan pass converts. It is the
// library's default rather than a value this package controls, so it is used only to SIZE the
// loop -- the loop is bounded, and the residual is measured afterwards rather than assumed -- and
// a change to it costs a pass or two, not correctness.
const columnarPassBudget = 64

// columnarResidualWarn is the residual above which preparation says the table is not in the
// state a maintained deployment reaches. Not zero, and not a convergence target: the active
// segment is never columnarized, so a probe reading newest-first always sees some residue.
const columnarResidualWarn = 0.10

// sampleArchiveEscape reads a bounded sample through the evaluated projection and reports the
// share of visited records rebuilt from the arena.
func sampleArchiveEscape(t *db.ArchiveTable, attrs []string, sample int) (float64, bool) {
	if len(attrs) == 0 {
		attrs = []string{"ClusterId", "Owner", "JobStatus"}
	}
	// sample <= 0 means the whole table, which is what the post-pass probe wants: a bounded
	// sample of a newest-first archive reads the newest segments and misreports convergence.
	var st collections.ScanStats
	seq, err := t.QueryProjectStats("true", attrs, &st)
	if err != nil {
		return 0, false
	}
	n := 0
	for range seq {
		n++
		if sample > 0 && n >= sample {
			break
		}
	}
	if st.RecordsVisited == 0 {
		return 0, false
	}
	return float64(st.RecordsReassembled) / float64(st.RecordsVisited), true
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
	// sample <= 0 means the whole table, which is what the post-pass probe wants: a bounded
	// sample of a newest-first archive reads the newest segments and misreports convergence.
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
