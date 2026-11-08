package bench

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/collections/wire"
	"github.com/PelicanPlatform/classad/db"
)

// The self-test: the server reading its own store, with no client and no network.
//
// WHAT IT MEASURES IS NOT THROUGHPUT, AND THE DIFFERENCE IS THE POINT. A client sweep measures
// what a consumer waits for -- production, serialization, the socket, and the client's own
// decode, whichever is slowest. This measures only the first of those: how fast the store can
// PRODUCE rows. It is the number to compare when the question is about storage, CPU or the
// columnar accelerator, and the wrong one to quote as "how fast is the database", because no
// consumer ever receives at this rate.
//
// Reported as its own kind rather than mixed into a client report, for the same reason the two
// projection forms are kept apart: numbers that answer different questions must not be sorted
// into one table.
//
// It also costs nothing to run beside the real thing, which is the honest use: run both, and the
// gap between them is the serialization and transport cost. A self-test alone cannot tell a slow
// disk from a slow network, because it never touches the network.
//
// PRODUCER AND CONSUMER ARE BOTH MEASURED, because the row a store hands out is not yet anything
// a caller can use. The producer pass reads rows and touches nothing. The consumer pass reads the
// same rows and does what a client does with them: parses old-ClassAd text into an ad, decodes a
// wire row into one, reads the typed values. The difference between the two passes is the
// consumer's own cost, and it is the term that decides whether a cheaper row FORM is worth
// anything -- a relay that halves production while doubling the parse at the other end has moved
// work, not removed it. Measuring only production is how a transport gets adopted on a number its
// consumer never sees.

// SelfTestSpec is one in-process sweep.
type SelfTestSpec struct {
	Table      string
	Constraint string
	Projection []string
	Limits     []int
	Iterations int
	// Concurrency is how many readers run at once, each on its own goroutine with its own
	// iterator. One (the default) measures latency; more measures what the store does under
	// contention, which is a different question and often the one that matters -- a read path
	// that scales sublinearly looks fine at K=1.
	//
	// There is no client here to compete for cores, which is exactly why in-process
	// concurrency is meaningful: every core the readers take is a core the store is using.
	// The same is NOT true of a client sweep run on the same box; see the package comment.
	Concurrency int
}

// SelfTestSample is one in-process cell. Bytes are what the read PRODUCED, which for the
// evaluated form is not a byte count at all -- a value batch has no serialized size until
// something serializes it -- so it is left zero there and the rate is reported in rows.
type SelfTestSample struct {
	Transport   Transport
	Limit       int
	Concurrency int
	Rows        int64
	Bytes       int64
	Elapsed     time.Duration
	Err         error
	// ConsumeElapsed is the same read with the consumer's work done on every row: the time a
	// caller pays to turn the stream into usable objects, production included. ConsumeErr is
	// non-nil when that work failed, which is itself a finding -- a row form whose consumer
	// cannot decode it is not a faster transport.
	ConsumeElapsed time.Duration
	ConsumeErr     error
	ConsumedRows   int64
}

// ConsumeRowsPerSec is the end-to-end in-process rate: produced AND turned into usable objects.
func (s SelfTestSample) ConsumeRowsPerSec() float64 {
	if s.ConsumeElapsed <= 0 {
		return 0
	}
	return float64(s.ConsumedRows) / s.ConsumeElapsed.Seconds()
}

// ConsumerShare is the fraction of the consuming pass spent on the consumer's own work rather
// than on producing the rows. It is the number that says whether a row form moves work or removes
// it; it is only meaningful when both passes ran.
func (s SelfTestSample) ConsumerShare() float64 {
	if s.ConsumeElapsed <= 0 || s.Elapsed <= 0 || s.ConsumeElapsed <= s.Elapsed {
		return 0
	}
	return float64(s.ConsumeElapsed-s.Elapsed) / float64(s.ConsumeElapsed)
}

// RowsPerSec is the production rate.
func (s SelfTestSample) RowsPerSec() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.Rows) / s.Elapsed.Seconds()
}

// MBPerSec is the production rate in bytes, or 0 for a form that produces no bytes.
func (s SelfTestSample) MBPerSec() float64 {
	if s.Elapsed <= 0 || s.Bytes == 0 {
		return 0
	}
	return float64(s.Bytes) / s.Elapsed.Seconds() / (1 << 20)
}

// SelfTestReport is a whole in-process sweep.
type SelfTestReport struct {
	Spec    SelfTestSpec
	Samples []SelfTestSample
	// Escape is the scan breakdown for the stored-form projection, read directly rather than
	// over the wire -- the same measurement Probe makes for a client, without a server to ask.
	Escape Escape
}

// selfTestTransports is what the in-process sweep measures: the same set a client sweep does,
// since the point is to compare the two.
func selfTestTransports() []Transport {
	return []Transport{TextWhole, TextProjected, Wire, WireRefs, Values}
}

// SelfTestArchive runs the sweep in process against an archive table.
func SelfTestArchive(t *db.ArchiveTable, spec SelfTestSpec) SelfTestReport {
	if spec.Iterations < 1 {
		spec.Iterations = 1
	}
	if len(spec.Limits) == 0 {
		spec.Limits = []int{0}
	}
	if spec.Concurrency < 1 {
		spec.Concurrency = 1
	}
	rep := SelfTestReport{Spec: spec}

	var st collections.ScanStats
	if seq, err := t.QueryRawProjectedRefsStats(spec.Constraint, spec.Projection, false, &st); err == nil {
		n := 0
		for range seq {
			n++
			if n >= 5000 {
				break
			}
		}
		if st.RecordsVisited > 0 {
			rep.Escape = Escape{
				Measured:           true,
				Visited:            st.RecordsVisited,
				Reassembled:        st.RecordsReassembled,
				ClosureReassembled: st.RecordsClosureReassembled,
				ColumnDecided:      st.RecordsColumnDecided,
				RowsMatched:        st.RowsMatched,
			}
		}
	}

	for _, tr := range selfTestTransports() {
		for _, limit := range spec.Limits {
			rep.Samples = append(rep.Samples, selfTestCell(t, spec, tr, limit))
		}
	}
	return rep
}

func selfTestCell(t *db.ArchiveTable, spec SelfTestSpec, tr Transport, limit int) SelfTestSample {
	s := SelfTestSample{Transport: tr, Limit: limit, Concurrency: spec.Concurrency}
	// One warm pass, untimed: the segments are mmap'd and the first read of them pays page-in
	// that no later read pays. Warmed once rather than per reader -- the page-in is shared.
	if _, _, err := selfTestOnce(t, spec, tr, limit, false); err != nil {
		s.Err = err
		return s
	}
	rows, bytes, elapsed, err := selfTestPass(t, spec, tr, limit, false)
	s.Rows, s.Bytes, s.Elapsed, s.Err = rows, bytes, elapsed, err
	if err != nil {
		return s
	}

	// The consuming pass, warmed the same way, over the same rows.
	if _, _, err := selfTestOnce(t, spec, tr, limit, true); err != nil {
		s.ConsumeErr = err
		return s
	}
	crows, _, celapsed, cerr := selfTestPass(t, spec, tr, limit, true)
	s.ConsumedRows, s.ConsumeElapsed, s.ConsumeErr = crows, celapsed, cerr
	return s
}

// selfTestPass runs Concurrency readers, each doing Iterations reads, and reports the totals
// against WALL CLOCK -- not the sum of the readers' own elapsed times, which would report a
// contended run as fast as an uncontended one.
func selfTestPass(t *db.ArchiveTable, spec SelfTestSpec, tr Transport, limit int, consume bool) (int64, int64, time.Duration, error) {
	var mu sync.Mutex
	var totalRows, totalBytes int64
	var firstErr error
	var wg sync.WaitGroup
	start := time.Now()
	for k := 0; k < spec.Concurrency; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var rows, bytes int64
			var err error
			for i := 0; i < spec.Iterations; i++ {
				r, b, e := selfTestOnce(t, spec, tr, limit, consume)
				if e != nil {
					err = e
					break
				}
				rows += r
				bytes += b
			}
			mu.Lock()
			totalRows += rows
			totalBytes += bytes
			if err != nil && firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return totalRows, totalBytes, time.Since(start), firstErr
}

// selfTestOnce reads one result. With consume set it also does the CONSUMER's work on every row
// -- the work a caller must do before the row is usable -- so the two passes differ by exactly
// that and nothing else.
func selfTestOnce(t *db.ArchiveTable, spec SelfTestSpec, tr Transport, limit int, consume bool) (rows, bytes int64, err error) {
	stop := func() bool { return limit > 0 && rows >= int64(limit) }
	switch tr {
	case TextWhole:
		seq, serr := t.QueryRawProjected(spec.Constraint, nil, false)
		if serr != nil {
			return 0, 0, serr
		}
		for ad := range seq {
			rows++
			bytes += rawAdBytes(ad)
			if consume {
				if cerr := consumeRawAd(ad); cerr != nil {
					return rows, bytes, cerr
				}
			}
			if stop() {
				break
			}
		}
	case TextProjected:
		seq, serr := t.QueryRawProjectedRefs(spec.Constraint, spec.Projection, false)
		if serr != nil {
			return 0, 0, serr
		}
		for ad := range seq {
			rows++
			bytes += rawAdBytes(ad)
			if consume {
				if cerr := consumeRawAd(ad); cerr != nil {
					return rows, bytes, cerr
				}
			}
			if stop() {
				break
			}
		}
	case WireRefs:
		// The reference-chasing wire projection, which is what TextProjected is in the stored
		// form; Wire below is the cheaper subset. Comparing TextProjected against Wire would
		// charge one arm for a closure the other was not asked to produce.
		seq, serr := t.QueryRawWireRefs(spec.Constraint, spec.Projection, false)
		if serr != nil {
			return 0, 0, serr
		}
		for row := range seq {
			rows++
			bytes += int64(len(row))
			if consume {
				node, derr := wire.DecodeInline(row)
				if derr != nil {
					return rows, bytes, fmt.Errorf("decoding a wire row: %w", derr)
				}
				sink = classad.FromAST(node)
			}
			if stop() {
				break
			}
		}
	case Wire:
		seq, serr := t.QueryRawWire(spec.Constraint, spec.Projection, false)
		if serr != nil {
			return 0, 0, serr
		}
		for row := range seq {
			rows++
			bytes += int64(len(row))
			if consume {
				node, derr := wire.DecodeInline(row)
				if derr != nil {
					return rows, bytes, fmt.Errorf("decoding a wire row: %w", derr)
				}
				// FromAST is what a consumer does next; without it the decode is only half
				// the work and the wire form would look cheaper than it is in use.
				sink = classad.FromAST(node)
			}
			if stop() {
				break
			}
		}
	case Values:
		seq, serr := t.QueryProject(spec.Constraint, spec.Projection)
		if serr != nil {
			return 0, 0, serr
		}
		// No bytes: a value slice has no serialized size until something serializes it, and
		// inventing one here would put a fabricated number beside measured ones.
		for vals := range seq {
			rows++
			if consume {
				// Already typed: the consumer's work is reading them, not decoding anything.
				// That IS the finding for this form, so it is measured rather than skipped.
				for i := range vals {
					valSink = vals[i].Type()
				}
			}
			if stop() {
				break
			}
		}
	default:
		return 0, 0, fmt.Errorf("unknown transport %q", tr)
	}
	return rows, bytes, nil
}

// String renders the in-process report, saying plainly what it is not.
func (r SelfTestReport) String() string {
	var b []byte
	b = append(b, fmt.Sprintf("SELF-TEST (in process, no client and no network): table %s, constraint %q, %d reader(s)\n",
		r.Spec.Table, r.Spec.Constraint, r.Spec.Concurrency)...)
	b = append(b, "This is how fast the store PRODUCES rows, not how fast a consumer receives them.\n"...)
	b = append(b, "Run a client sweep beside it; the gap between the two is serialization and transport.\n"...)
	if r.Escape.Measured {
		b = append(b, fmt.Sprintf("sampled stored-form read: %.0f%% of %d records rebuilt from the arena\n",
			100*r.Escape.Rate(), r.Escape.Visited)...)
	}
	b = append(b, "\nPRODUCE = rows read, nothing done with them.  CONSUME = the same rows turned into\n"...)
	b = append(b, "usable objects (text parsed, wire decoded, values read). The gap is the consumer's\n"...)
	b = append(b, "own cost: a row form that halves production and doubles the parse has moved work.\n"...)
	for _, s := range r.Samples {
		lim := "all"
		if s.Limit > 0 {
			lim = fmt.Sprintf("%d", s.Limit)
		}
		form := "stored"
		if s.Transport.Evaluated() {
			form = "evaluated"
		}
		if s.Err != nil {
			b = append(b, fmt.Sprintf("  %-22s %-9s limit=%-5s FAILED: %v\n", s.Transport, form, lim, s.Err)...)
			continue
		}
		byteRate := "  (no serialized bytes)"
		if s.Bytes > 0 {
			byteRate = fmt.Sprintf("  %8.1f MB/s", s.MBPerSec())
		}
		b = append(b, fmt.Sprintf("  %-22s %-9s limit=%-5s produce %9.0f rows/s%s\n",
			s.Transport, form, lim, s.RowsPerSec(), byteRate)...)
		switch {
		case s.ConsumeErr != nil:
			b = append(b, fmt.Sprintf("  %-22s %-9s %12s consume FAILED: %v\n", "", "", "", s.ConsumeErr)...)
		case s.ConsumeElapsed > 0:
			b = append(b, fmt.Sprintf("  %-22s %-9s %12s consume %9.0f rows/s  (%.0f%% of it the consumer's own work)\n",
				"", "", "", s.ConsumeRowsPerSec(), 100*s.ConsumerShare())...)
		}
	}
	return string(b)
}

// consumeRawAd does what a client does with a stored-form row: parses the expression parts back
// into an ad. The parts alias a buffer the iteration reuses, so this happens before the iterator
// advances -- which is also why a consumer cannot defer this work in practice.
func consumeRawAd(ad collections.RawAd) error {
	var b strings.Builder
	for _, e := range ad.Exprs {
		b.Write(e)
		b.WriteByte('\n')
	}
	parsed, err := classad.ParseOld(b.String())
	if err != nil {
		return fmt.Errorf("parsing a stored-form row: %w", err)
	}
	sink = parsed
	return nil
}

// sink and valSink keep the consumer's results reachable so the compiler cannot delete the work
// being measured. A benchmark whose work is optimized away reports the cost of an empty loop.
//
// Package-level and READ by ConsumerSink, rather than written and forgotten: a variable only
// ever assigned is exactly what a linter calls unused and what an optimizer is entitled to
// remove, which would quietly turn the consuming pass back into the producing one.
var (
	sink    *classad.ClassAd
	valSink classad.ValueType
)

// ConsumerSink reports the last values the consuming pass produced. It exists so those values are
// genuinely read somewhere, which is what keeps the work from being elided.
func ConsumerSink() (*classad.ClassAd, classad.ValueType) { return sink, valSink }

// rawAdBytes is what the stored form would put on the wire for one ad: the expression parts,
// which is what a relay writes. Counted rather than rendered, so the self-test does not charge
// itself for building a string no consumer asked for.
func rawAdBytes(ad collections.RawAd) int64 {
	var n int64
	for _, e := range ad.Exprs {
		n += int64(len(e))
	}
	return n
}

var _ = classad.Value{} // the value form yields these; kept so the import documents the shape
