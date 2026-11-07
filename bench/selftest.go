package bench

import (
	"fmt"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
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

// SelfTestSpec is one in-process sweep.
type SelfTestSpec struct {
	Table      string
	Constraint string
	Projection []string
	Limits     []int
	Iterations int
}

// SelfTestSample is one in-process cell. Bytes are what the read PRODUCED, which for the
// evaluated form is not a byte count at all -- a value batch has no serialized size until
// something serializes it -- so it is left zero there and the rate is reported in rows.
type SelfTestSample struct {
	Transport Transport
	Limit     int
	Rows      int64
	Bytes     int64
	Elapsed   time.Duration
	Err       error
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

// SelfTestArchive runs the sweep in process against an archive table.
func SelfTestArchive(t *db.ArchiveTable, spec SelfTestSpec) SelfTestReport {
	if spec.Iterations < 1 {
		spec.Iterations = 1
	}
	if len(spec.Limits) == 0 {
		spec.Limits = []int{0}
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

	for _, tr := range []Transport{TextWhole, TextProjected, Wire, Values} {
		for _, limit := range spec.Limits {
			rep.Samples = append(rep.Samples, selfTestCell(t, spec, tr, limit))
		}
	}
	return rep
}

func selfTestCell(t *db.ArchiveTable, spec SelfTestSpec, tr Transport, limit int) SelfTestSample {
	s := SelfTestSample{Transport: tr, Limit: limit}
	// One warm pass, untimed: the segments are mmap'd and the first read of them pays page-in
	// that no later read pays.
	if _, _, err := selfTestOnce(t, spec, tr, limit); err != nil {
		s.Err = err
		return s
	}
	start := time.Now()
	for i := 0; i < spec.Iterations; i++ {
		rows, bytes, err := selfTestOnce(t, spec, tr, limit)
		if err != nil {
			s.Err = err
			break
		}
		s.Rows += rows
		s.Bytes += bytes
	}
	s.Elapsed = time.Since(start)
	return s
}

func selfTestOnce(t *db.ArchiveTable, spec SelfTestSpec, tr Transport, limit int) (rows, bytes int64, err error) {
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
			_ = vals
			rows++
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
	b = append(b, fmt.Sprintf("SELF-TEST (in process, no client and no network): table %s, constraint %q\n",
		r.Spec.Table, r.Spec.Constraint)...)
	b = append(b, "This is how fast the store PRODUCES rows, not how fast a consumer receives them.\n"...)
	b = append(b, "Run a client sweep beside it; the gap between the two is serialization and transport.\n"...)
	if r.Escape.Measured {
		b = append(b, fmt.Sprintf("sampled stored-form read: %.0f%% of %d records rebuilt from the arena\n",
			100*r.Escape.Rate(), r.Escape.Visited)...)
	}
	for _, s := range r.Samples {
		lim := "all"
		if s.Limit > 0 {
			lim = fmt.Sprintf("%d", s.Limit)
		}
		form := "stored"
		if s.Transport.Evaluated() {
			form = "evaluated"
		}
		switch {
		case s.Err != nil:
			b = append(b, fmt.Sprintf("  %-22s %-9s limit=%-5s FAILED: %v\n", s.Transport, form, lim, s.Err)...)
		case s.Bytes == 0:
			b = append(b, fmt.Sprintf("  %-22s %-9s limit=%-5s %10.0f rows/s  (no serialized bytes)\n",
				s.Transport, form, lim, s.RowsPerSec())...)
		default:
			b = append(b, fmt.Sprintf("  %-22s %-9s limit=%-5s %10.0f rows/s  %8.1f MB/s\n",
				s.Transport, form, lim, s.RowsPerSec(), s.MBPerSec())...)
		}
	}
	return string(b)
}

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
