// Package bench measures how fast a store hands query results to a client.
//
// WHAT IT MEASURES, AND WHY IT IS BYTES. A per-row or per-ad cost does not say how fast results
// leave: a six-column Arrow row and a forty-five-attribute old-ClassAd ad are not comparable as
// rows, only as bytes. So every transport here is measured the same way -- the bytes a CLIENT
// received, divided by wall clock -- and rows are reported beside them rather than instead.
//
// WHY THE LOAD IS GENERATED OUT OF PROCESS. The point of this package is comparing deployments:
// hardware, storage, encryption, a NIC. A generator sharing the daemon's cores measures itself as
// much as the daemon, and the error is not small -- an in-process sweep of this exact workload
// reported four concurrent readers as SLOWER than one and slower than eight, which is not a
// scaling curve but contention between the clients and the server they were timing. So the
// dialer is supplied by the caller and the intended one connects to a daemon over the network.
//
// WHY A RUN REPORTS THE STORE'S STATE. The same query against the same data differs severalfold
// on state that has nothing to do with what is being compared: an unbuilt index turns a selective
// read into a full scan, and columnar coverage decides whether a projected attribute is read from
// a column or rebuilt from each record. A number produced without saying which of those held is
// not comparable with another number, so Probe reports them and Readiness.Warnings names the ones
// that make a comparison unsound.
package bench

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
)

// Transport is one way of asking for results. They are different products, not settings of one:
// the text forms are rendered old-ClassAd ads, the wire form is the relay encoding with the
// render deferred to the consumer. Comparing them is comparing useful bytes per second.
type Transport string

const (
	// TextWhole is the whole ad as old-ClassAd text: what `condor_q -l` asks for.
	TextWhole Transport = "dbrpc/text-whole"
	// TextProjected is a named projection, reference-chased the way a schedd's is.
	TextProjected Transport = "dbrpc/text-projected"
	// Wire is the binary relay row with a plain SUBSET projection: the named attributes and
	// nothing else.
	Wire Transport = "dbrpc/wire"
	// WireRefs is the binary relay row with a REFERENCE-CHASING projection, so each row is
	// self-contained. It is the like-for-like counterpart of TextProjected; Wire is not.
	WireRefs Transport = "dbrpc/wire-refs"
	// Values is the EVALUATED projection: typed values, column-major, which is what a
	// consumer with a typed schema (Arrow, SQL) needs.
	Values Transport = "dbrpc/values"
)

// COMPARE LIKE WITH LIKE: A PROJECTION IS NOT ONE THING.
//
// A projection either chases the references its attributes make or it does not. A
// reference-chasing projection returns a SELF-CONTAINED row -- an attribute whose value is an
// expression arrives with the attributes that expression reads -- and that costs more than
// shipping the named attributes alone.
//
// So TextProjected (reference-chasing) pairs with WireRefs, and Wire is the cheaper subset form
// with no text counterpart here. Pairing TextProjected against Wire measures the closure as if
// it were the transport, and flatters the wire side for work it was not asked to do. The
// transports are listed in pairs for that reason, and anyone reading one number against another
// should check they chase references alike.
//
// TWO PROJECTION FORMS, AND THEY ARE NOT INTERCHANGEABLE.
//
// TextWhole, TextProjected and Wire ship the STORED form: an ad carries the expression it was
// written with, so `MemoryUsage = (ResidentSetSize + 1023) / 1024` ships as that expression and
// the record is rebuilt to produce it. Values ships the EVALUATED form: the number the expression
// computes to, which a computed column already holds and can serve without touching the record.
//
// The difference is not a tuning detail. Measured over the same prepared history archive, 20000
// rows: the raw form costs 215ms for a plain projection and 355ms with MemoryUsage added; the
// value form costs 22ms and 26ms. About 10x between the forms, and the expression attribute costs
// the raw form 140ms where it costs the value form 4ms.
//
// So a sweep must say which form it measured, and a reader must not carry a conclusion from one
// to the other -- which is exactly the mistake that reads a raw-form reassembly rate as if it
// described a typed consumer.
func (t Transport) Evaluated() bool { return t == Values || t == Arrow }

// DefaultTransports is what a sweep measures when the caller names none: both stored-form
// projections paired with their wire counterparts, the whole ad, and the evaluated form.
//
// ARROW IS NOT IN IT, because it is the one transport that needs a second server endpoint. A
// sweep that included it by default would report it as failed against every server without
// Flight SQL, which reads as a slow transport rather than an absent one. A caller that has a
// Flight address adds it.
func DefaultTransports() []Transport {
	return []Transport{TextWhole, TextProjected, Wire, WireRefs, Values}
}

// Conn is one measured client connection.
type Conn struct {
	Client *dbrpc.Client
	// Flight is the Arrow Flight SQL client, when the dialer opened one. A connection may
	// carry either or both; a transport the connection cannot speak declines with a note.
	Flight *flightsql.Client
	Close  func()
	// Bytes reports the bytes READ from the socket so far, or nil when the dialer does not
	// count them. Counting at the socket rather than summing row lengths is what makes the
	// transports comparable: it includes framing, and it is the only way to measure a
	// transport whose rows are not byte slices -- the value stream is column-major, and a
	// typed consumer's batches have no per-row length to add up.
	Bytes func() int64
}

// Dialer opens one client connection. Concurrency is expressed by calling it more than once:
// separate connections are what independent consumers actually are, and several reads multiplexed
// onto one connection measure that connection instead of the server.
type Dialer func(ctx context.Context) (*Conn, error)

// CountingConn wraps a net.Conn so a dialer can report bytes received. The server's bytes are
// what a consumer waits for, so only reads are counted.
type CountingConn struct {
	net.Conn
	n *atomic.Int64
}

// NewCountingConn returns c wrapped so that the returned func reports bytes read from it.
func NewCountingConn(c net.Conn) (net.Conn, func() int64) {
	var n atomic.Int64
	return &CountingConn{Conn: c, n: &n}, func() int64 { return n.Load() }
}

func (c *CountingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// Spec is one sweep.
type Spec struct {
	Table      string
	Constraint string
	// Projection is used by TextProjected and Wire; TextWhole ignores it.
	Projection []string
	// Limits are the result sizes to sweep, 0 meaning the whole result. A sweep rather than one
	// size because the delivered rate is not flat in it: both transports measured here change
	// character as the result grows, and a single size reports one point as if it were the curve.
	Limits []int
	// Concurrency is how many clients read at once, each on its own connection.
	Concurrency int
	// SQLWhere is the Arrow arm's filter, which the caller asserts is equivalent to
	// Constraint. The two languages differ, so the tool will not translate between them; see
	// arrow.go.
	SQLWhere string
	// Iterations is how many times each client repeats its read, after one warm pass that is not
	// timed. The warm pass matters: the segments are mmap'd and the first read of them pays
	// page-in no later read pays.
	Iterations int
	Transports []Transport
}

// Sample is one (transport, limit, concurrency) cell, aggregated across its clients.
type Sample struct {
	Transport   Transport
	Limit       int
	Concurrency int
	Rows        int64
	Bytes       int64
	Elapsed     time.Duration
	// Note records something that changes how the number should be read -- a transport that was
	// unavailable and fell back, most importantly. A cell with a Note is not comparable with one
	// without it, which is the whole reason it is carried beside the number rather than logged.
	Note string
	Err  error
}

// MBPerSec is the delivered rate: bytes the clients received over wall clock.
func (s Sample) MBPerSec() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.Bytes) / s.Elapsed.Seconds() / (1 << 20)
}

// RowsPerSec is the same measurement counted in rows.
func (s Sample) RowsPerSec() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.Rows) / s.Elapsed.Seconds()
}

// BytesPerRow is what makes the two rates comparable between transports.
func (s Sample) BytesPerRow() float64 {
	if s.Rows == 0 {
		return 0
	}
	return float64(s.Bytes) / float64(s.Rows)
}

// Readiness is the store state a reader needs in order to compare two runs. Warnings names the
// conditions under which the numbers are not comparable; a caller that prints a result without
// printing these is publishing a number whose meaning it has not stated.
type Readiness struct {
	Table           string
	Records         int
	ValueIndexes    []string
	CategoricalIdx  []string
	ColumnarEnabled bool
	SealedSegments  int
	CoveredSegments int
	Encryption      bool
	Plan            string
	// Escape is what a sampled projected read actually cost. Measured, not inferred: whether a
	// read is served from columns depends on the projection, not only on the accelerator's state.
	Escape   Escape
	Warnings []string
}

// Probe reads the store's state for the table and constraint a sweep will use.
func Probe(ctx context.Context, c *dbrpc.Client, table, constraint string, projection []string) (Readiness, error) {
	d, err := c.DiagnosticsTable(ctx, table)
	if err != nil {
		return Readiness{}, fmt.Errorf("diagnostics for %q: %w", table, err)
	}
	r := Readiness{
		Table:           table,
		Records:         d.Stats.Ads,
		ValueIndexes:    d.ValueIndexes,
		CategoricalIdx:  d.CategoricalIndexes,
		ColumnarEnabled: d.SchemaScan.Enabled,
		SealedSegments:  d.SchemaScan.SealedSegments,
		CoveredSegments: d.SchemaScan.CoveredSegments,
		Encryption:      d.EncryptionEnabled,
	}
	if ex, eerr := c.ExplainTable(ctx, table, constraint); eerr == nil && ex != nil {
		r.Plan = ex.Plan
	}
	if len(projection) > 0 {
		if e, eerr := MeasureEscape(ctx, c, table, constraint, projection, 5000); eerr == nil {
			r.Escape = e
		}
	}
	if r.Escape.Measured && r.Escape.Rate() > 0.25 {
		r.Warnings = append(r.Warnings, fmt.Sprintf(
			"%.0f%% of sampled records were rebuilt from the arena rather than served from columns; if the projection names an EXPRESSION-valued attribute (MemoryUsage, Requirements) that is inherent -- a shipped ad carries the stored expression, not a computed value -- and the comparison is still valid, but only against another run with the same projection",
			100*r.Escape.Rate()))
	}
	if r.Records == 0 {
		r.Warnings = append(r.Warnings, "the table is empty: this sweep measures an empty scan")
	}
	if !r.ColumnarEnabled && r.SealedSegments > 0 {
		r.Warnings = append(r.Warnings,
			"the columnar accelerator is off: projected attributes are rebuilt from each record")
	}
	if r.ColumnarEnabled && r.CoveredSegments < r.SealedSegments {
		r.Warnings = append(r.Warnings, fmt.Sprintf(
			"columnar coverage is partial (%d of %d sealed segments): the uncovered ones are read from records",
			r.CoveredSegments, r.SealedSegments))
	}
	if len(r.ValueIndexes) == 0 && len(r.CategoricalIdx) == 0 && constraint != "" && constraint != "true" {
		r.Warnings = append(r.Warnings,
			"no index is built: a selective constraint is answered by a full scan, which is a property of the store's maintenance rather than of what is being compared")
	}
	return r, nil
}

// Report is a whole sweep.
type Report struct {
	Spec      Spec
	Readiness Readiness
	Samples   []Sample
}

// Run executes the sweep. It dials Concurrency clients once and reuses them across the whole
// sweep, so connection setup is not charged to any cell.
func Run(ctx context.Context, dial Dialer, spec Spec) (Report, error) {
	if spec.Concurrency < 1 {
		spec.Concurrency = 1
	}
	if spec.Iterations < 1 {
		spec.Iterations = 1
	}
	if len(spec.Limits) == 0 {
		spec.Limits = []int{0}
	}
	if len(spec.Transports) == 0 {
		spec.Transports = DefaultTransports()
	}
	conns := make([]*Conn, 0, spec.Concurrency)
	defer func() {
		for _, c := range conns {
			if c.Close != nil {
				c.Close()
			}
		}
	}()
	for i := 0; i < spec.Concurrency; i++ {
		c, err := dial(ctx)
		if err != nil {
			return Report{}, fmt.Errorf("dialing client %d: %w", i, err)
		}
		conns = append(conns, c)
	}

	rep := Report{Spec: spec}
	if r, err := Probe(ctx, conns[0].Client, spec.Table, spec.Constraint, spec.Projection); err == nil {
		rep.Readiness = r
	} else {
		rep.Readiness = Readiness{Table: spec.Table, Warnings: []string{"readiness unavailable: " + err.Error()}}
	}

	for _, tr := range spec.Transports {
		for _, limit := range spec.Limits {
			rep.Samples = append(rep.Samples, runCell(ctx, conns, spec, tr, limit))
		}
	}
	return rep, nil
}

// runCell measures one transport at one limit across every client, after one untimed warm pass.
func runCell(ctx context.Context, conns []*Conn, spec Spec, tr Transport, limit int) Sample {
	s := Sample{Transport: tr, Limit: limit, Concurrency: len(conns)}
	// Warm on one client: the point is to fault the segments in, which every client then sees.
	if _, _, err := readOnce(ctx, conns[0], spec, tr, limit); err != nil {
		s.Err = err
		s.Note = noteForError(err)
		return s
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := time.Now()
	for _, cn := range conns {
		wg.Add(1)
		go func(cn *Conn) {
			defer wg.Done()
			var rows, rowBytes int64
			var before int64
			if cn.Bytes != nil {
				before = cn.Bytes()
			}
			var ferr error
			for i := 0; i < spec.Iterations; i++ {
				r, b, err := readOnce(ctx, cn, spec, tr, limit)
				if err != nil {
					ferr = err
					break
				}
				rows += r
				rowBytes += b
			}
			// Socket bytes when the dialer counts them, row bytes otherwise. The value stream
			// is column-major and has no per-row length, so for it only the socket count is
			// meaningful -- and leaving the cell at zero would read as "delivered nothing".
			got := rowBytes
			if cn.Bytes != nil {
				got = cn.Bytes() - before
			}
			mu.Lock()
			s.Rows += rows
			s.Bytes += got
			if ferr != nil && s.Err == nil {
				s.Err = ferr
			}
			mu.Unlock()
		}(cn)
	}
	wg.Wait()
	s.Elapsed = time.Since(start)
	return s
}

// noteForError turns a failure into the sentence a reader needs beside the empty cell. A
// transport the server will not serve must SAY so: a harness that quietly substituted another
// would publish one path's rate under another path's name, which is the failure a comparison
// cannot survive and cannot detect.
func noteForError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, dbrpc.ErrRawWireUnsupported):
		return "wire rows unavailable: the server is too old for the opcode, or this table cannot serve them"
	default:
		return "failed: " + err.Error()
	}
}

// readOnce drains one result and reports what the client received.
func readOnce(ctx context.Context, cn *Conn, spec Spec, tr Transport, limit int) (rows, bytes int64, err error) {
	c := cn.Client
	if c == nil && tr != Arrow {
		return 0, 0, fmt.Errorf("no dbrpc client for this connection")
	}
	switch tr {
	case TextWhole:
		// The projection op with NO attributes, which is defined to behave like the whole-ad
		// op and, unlike it, is served from an archive too. Asking for the whole ad through
		// the whole-ad op fails outright on a history table, which is the one most worth
		// measuring.
		err = c.QueryRawProjectStream(ctx, spec.Table, spec.Constraint, nil, limit,
			func(row string) bool {
				rows++
				bytes += int64(len(row))
				return true
			})
	case TextProjected:
		err = c.QueryRawProjectRefsStream(ctx, spec.Table, spec.Constraint, spec.Projection, limit,
			func(row string) bool {
				rows++
				bytes += int64(len(row))
				return true
			})
	case Wire:
		err = c.QueryRawWireStream(ctx, spec.Table, spec.Constraint, spec.Projection, limit, false,
			func(row []byte) bool {
				rows++
				bytes += int64(len(row))
				return true
			})
	case WireRefs:
		err = c.QueryRawWireRefsStream(ctx, spec.Table, spec.Constraint, spec.Projection, limit, false,
			func(row []byte) bool {
				rows++
				bytes += int64(len(row))
				return true
			})
	case Arrow:
		// Rows only: a column-major batch has no per-row length, so the bytes come from the
		// socket (see runCell).
		rows, err = readArrow(ctx, cn, spec, limit)
	case Values:
		// Column-major batches: rows are counted from the batch, and the BYTES come from the
		// socket (see runCell), because a typed batch has no per-row length to add up.
		err = c.QueryValuesStream(ctx, spec.Table, spec.Constraint, spec.Projection, limit, false,
			func(b *dbrpc.ValueBatch) bool {
				rows += int64(b.Rows)
				return true
			})
	default:
		err = fmt.Errorf("unknown transport %q", tr)
	}
	return rows, bytes, err
}

// SortSamples orders a report's samples for printing: by transport, then by limit.
func SortSamples(s []Sample) {
	sort.SliceStable(s, func(i, j int) bool {
		if s[i].Transport != s[j].Transport {
			return s[i].Transport < s[j].Transport
		}
		return s[i].Limit < s[j].Limit
	})
}
