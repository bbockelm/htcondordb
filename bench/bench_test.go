package bench

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

func jobAd(i int) *classad.ClassAd {
	ad, err := classad.ParseOld(fmt.Sprintf(
		"ClusterId = %d\nProcId = %d\nJobStatus = %d\nOwner = \"user%d\"\nRequestCpus = %d\n"+
			"RequestMemory = %d\nCmd = \"/bin/sleep\"\nIwd = \"/home/user%d\"",
		1000+i/10, i%10, 1+i%5, i%64, 1+i%8, 2048+(i%16)*512, i%64))
	if err != nil {
		panic(err)
	}
	return ad
}

// serve puts a store behind a real TCP listener and returns a Dialer for it. A socket rather than
// an in-process pipe because that is what the package is for: a pipe measures a rate no consumer
// can receive at.
func serve(t *testing.T, d *db.DB) Dialer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServer(d)
	// Benchmark mode enables this, and so does the harness: the server fills a read's
	// scan breakdown only while it is aggregating, so without it MeasureEscape reports
	// "not measured" and every escape assertion below would be vacuous.
	s.EnableQueryStats(dbrpc.QueryStatsOptions{})
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func() { _ = s.ServeConn(dbrpc.NewStreamConn(c)) }()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); s.Close() })
	return func(context.Context) (*Conn, error) {
		c, derr := net.Dial("tcp", ln.Addr().String())
		if derr != nil {
			return nil, derr
		}
		counted, bytes := NewCountingConn(c)
		cl := dbrpc.NewClient(dbrpc.NewStreamConn(counted))
		return &Conn{Client: cl, Close: func() { _ = cl.Close() }, Bytes: bytes}, nil
	}
}

func loaded(t *testing.T, dir string, n int) *db.DB {
	t.Helper()
	d, err := db.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	tx := d.Begin()
	for i := 0; i < n; i++ {
		tx.NewClassAd(fmt.Sprintf("%d.0", i), jobAd(i))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return d
}

// A sweep must report a rate for every transport, and the rate must be derived from bytes the
// client actually received -- so every cell has rows, bytes and a positive elapsed.
func TestRunMeasuresEveryTransport(t *testing.T) {
	const n = 2000
	dial := serve(t, loaded(t, t.TempDir(), n))
	rep, err := Run(context.Background(), dial, Spec{
		Table:       dbrpc.DefaultTable,
		Constraint:  "true",
		Projection:  []string{"ClusterId", "ProcId", "Owner", "JobStatus"},
		Limits:      []int{0},
		Concurrency: 1,
		Iterations:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Derived from the default transport list rather than written down, so adding a transport
	// does not fail this test for the wrong reason.
	if want := len(defaultTransports()); len(rep.Samples) != want {
		t.Fatalf("%d samples, want %d (one per transport)", len(rep.Samples), want)
	}
	for _, s := range rep.Samples {
		if s.Err != nil {
			t.Fatalf("%s: %v", s.Transport, s.Err)
		}
		if s.Rows != int64(n) {
			t.Errorf("%s: %d rows, want %d", s.Transport, s.Rows, n)
		}
		if s.Bytes <= 0 || s.MBPerSec() <= 0 || s.BytesPerRow() <= 0 {
			t.Errorf("%s: bytes=%d MB/s=%.2f B/row=%.1f; a rate must come from measured bytes",
				s.Transport, s.Bytes, s.MBPerSec(), s.BytesPerRow())
		}
	}
}

// A projection must ship fewer bytes per row than the whole ad. This is the check that the
// transports are not accidentally measuring the same request: an earlier version of this kind of
// harness sent the same shape under three names and reported three identical rates.
func TestProjectionShipsFewerBytesThanWholeAd(t *testing.T) {
	dial := serve(t, loaded(t, t.TempDir(), 2000))
	rep, err := Run(context.Background(), dial, Spec{
		Table:      dbrpc.DefaultTable,
		Constraint: "true",
		Projection: []string{"ClusterId", "ProcId"},
		Limits:     []int{0},
		Transports: []Transport{TextWhole, TextProjected},
	})
	if err != nil {
		t.Fatal(err)
	}
	var whole, proj float64
	for _, s := range rep.Samples {
		switch s.Transport {
		case TextWhole:
			whole = s.BytesPerRow()
		case TextProjected:
			proj = s.BytesPerRow()
		}
	}
	if !(proj < whole) {
		t.Fatalf("projected %.1f B/row is not below whole-ad %.1f B/row; the two arms are sending the same request",
			proj, whole)
	}
}

// Limits must actually bound the result, so a sweep over sizes measures different sizes.
func TestLimitsBoundTheResult(t *testing.T) {
	dial := serve(t, loaded(t, t.TempDir(), 2000))
	rep, err := Run(context.Background(), dial, Spec{
		Table: dbrpc.DefaultTable, Constraint: "true",
		Projection: []string{"ClusterId"},
		Limits:     []int{10, 500},
		Transports: []Transport{TextWhole},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := map[int]int64{}
	for _, s := range rep.Samples {
		got[s.Limit] = s.Rows
	}
	if got[10] != 10 || got[500] != 500 {
		t.Fatalf("limit 10 gave %d rows, limit 500 gave %d", got[10], got[500])
	}
}

// An unavailable transport must SAY SO rather than report a rate: a harness that quietly
// substituted another would publish one path's number under another path's name, which no reader
// of the output could detect.
//
// Driven through noteForError rather than through a table that declines, because every table kind
// this package can build now serves wire rows -- including an in-memory one, which
// TestInMemoryTableServesWireRows pins.
func TestUnavailableTransportIsReportedNotSubstituted(t *testing.T) {
	if got := noteForError(dbrpc.ErrRawWireUnsupported); !strings.Contains(got, "wire rows unavailable") {
		t.Fatalf("noteForError(ErrRawWireUnsupported) = %q, want it to name the unavailable transport", got)
	}
	if got := noteForError(fmt.Errorf("wrapped: %w", dbrpc.ErrRawWireUnsupported)); !strings.Contains(got, "wire rows unavailable") {
		t.Fatalf("a wrapped sentinel was not recognised: %q", got)
	}
	if got := noteForError(nil); got != "" {
		t.Fatalf("noteForError(nil) = %q, want empty", got)
	}
	if got := noteForError(errors.New("connection reset")); !strings.Contains(got, "connection reset") {
		t.Fatalf("an ordinary failure lost its cause: %q", got)
	}
}

// An in-memory table serves wire rows. Pinned because the opposite was true once and is still
// widely believed: a caller that skips the wire transport for an in-memory table on that belief
// is measuring the text path and calling it wire.
func TestInMemoryTableServesWireRows(t *testing.T) {
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	tx := d.Begin()
	for i := 0; i < 200; i++ {
		tx.NewClassAd(fmt.Sprintf("%d.0", i), jobAd(i))
	}
	if cerr := tx.Commit(); cerr != nil {
		t.Fatal(cerr)
	}
	dial := serve(t, d)
	rep, rerr := Run(context.Background(), dial, Spec{
		Table: dbrpc.DefaultTable, Constraint: "true",
		Projection: []string{"ClusterId"},
		Transports: []Transport{Wire},
	})
	if rerr != nil {
		t.Fatal(rerr)
	}
	s := rep.Samples[0]
	if s.Err != nil {
		t.Fatalf("an in-memory table refused wire rows: %v (note: %q)", s.Err, s.Note)
	}
	if s.Rows != 200 {
		t.Fatalf("%d rows, want 200", s.Rows)
	}
}

// Readiness must report the state that decides whether two runs are comparable, and warn when it
// does not hold. A number published without this is a number whose meaning was not stated.
func TestReadinessWarnsWhenNotComparable(t *testing.T) {
	dial := serve(t, loaded(t, t.TempDir(), 500))
	cn, err := dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()
	c := cn.Client
	r, perr := Probe(context.Background(), c, dbrpc.DefaultTable, `Owner == "user3"`, nil)
	if perr != nil {
		t.Fatal(perr)
	}
	if r.Records != 500 {
		t.Errorf("Records = %d, want 500", r.Records)
	}
	found := false
	for _, w := range r.Warnings {
		if strings.Contains(w, "no index is built") {
			found = true
		}
	}
	if !found {
		t.Fatalf("a selective constraint on an unindexed store did not warn; warnings = %v", r.Warnings)
	}
}

// The evaluated form must be measured as its own transport, with bytes counted at the socket: a
// column-major batch has no per-row length, so a harness summing row lengths would report it as
// delivering nothing. It is also a different product from the stored form -- about 10x cheaper
// here -- so a reader must never carry a conclusion from one to the other.
func TestValuesTransportIsMeasuredSeparately(t *testing.T) {
	dial := serve(t, loaded(t, t.TempDir(), 2000))
	rep, err := Run(context.Background(), dial, Spec{
		Table: dbrpc.DefaultTable, Constraint: "true",
		Projection: []string{"ClusterId", "Owner", "JobStatus"},
		Transports: []Transport{TextProjected, Values},
	})
	if err != nil {
		t.Fatal(err)
	}
	var raw, val Sample
	for _, s := range rep.Samples {
		switch s.Transport {
		case TextProjected:
			raw = s
		case Values:
			val = s
		}
	}
	if val.Err != nil {
		t.Fatalf("the value stream failed: %v (%s)", val.Err, val.Note)
	}
	if val.Rows != 2000 || raw.Rows != 2000 {
		t.Fatalf("rows: values=%d raw=%d, want 2000 each", val.Rows, raw.Rows)
	}
	if val.Bytes <= 0 {
		t.Fatal("the value transport reported no bytes; a column-major batch needs the socket count")
	}
	if !val.Transport.Evaluated() || raw.Transport.Evaluated() {
		t.Fatal("the two forms are not distinguished by Evaluated()")
	}
}

// The printed report must group by projection form and keep the store state above the numbers.
// A flat table sorted by rate invites the one conclusion the measurement does not support --
// that the evaluated form is "the server being 10x faster".
func TestReportSeparatesTheTwoForms(t *testing.T) {
	dial := serve(t, loaded(t, t.TempDir(), 2000))
	rep, err := Run(context.Background(), dial, Spec{
		Table: dbrpc.DefaultTable, Constraint: "true",
		Projection: []string{"ClusterId", "Owner"},
	})
	if err != nil {
		t.Fatal(err)
	}
	out := rep.String()
	for _, want := range []string{"STORED form", "EVALUATED form", "dbrpc/values", "dbrpc/text-whole", "MB/s"} {
		if !strings.Contains(out, want) {
			t.Errorf("report is missing %q:\n%s", want, out)
		}
	}
	// The store state has to precede the numbers, not follow them.
	if strings.Index(out, "columnar=") > strings.Index(out, "STORED form") {
		t.Errorf("the store state is printed below the rates:\n%s", out)
	}
	t.Logf("\n%s", out)
}

// Concurrency must actually run K readers at once, each on its own connection. A harness that
// dialled K and used one would report a contended rate as an uncontended one -- and the whole
// reason for separate connections is that several reads multiplexed onto one measure that
// connection rather than the server.
func TestConcurrencyRunsSeparateConnections(t *testing.T) {
	dial := serve(t, loaded(t, t.TempDir(), 2000))
	var dialed atomic.Int32
	counting := func(ctx context.Context) (*Conn, error) {
		dialed.Add(1)
		return dial(ctx)
	}
	const k = 4
	rep, err := Run(context.Background(), counting, Spec{
		Table: dbrpc.DefaultTable, Constraint: "true",
		Projection:  []string{"ClusterId", "Owner"},
		Transports:  []Transport{TextProjected},
		Concurrency: k,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := dialed.Load(); got != k {
		t.Fatalf("dialed %d connections, want %d", got, k)
	}
	s := rep.Samples[0]
	if s.Concurrency != k {
		t.Errorf("sample reports concurrency %d, want %d", s.Concurrency, k)
	}
	// K readers each drain the whole table, so the cell totals K times the rows.
	if s.Rows != int64(k*2000) {
		t.Errorf("%d rows across %d readers, want %d", s.Rows, k, k*2000)
	}
}

// serveCatalogShared is serveCatalog without a *testing.T, for a fixture built once in a
// sync.Once where no single test owns the cleanup. The listener lives for the test binary.
func serveCatalogShared(cat *db.Catalog) Dialer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return func(context.Context) (*Conn, error) { return nil, err }
	}
	s := dbrpc.NewServerCatalog(cat)
	s.EnableQueryStats(dbrpc.QueryStatsOptions{})
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func() { _ = s.ServeConn(dbrpc.NewStreamConn(c)) }()
		}
	}()
	return func(context.Context) (*Conn, error) {
		c, derr := net.Dial("tcp", ln.Addr().String())
		if derr != nil {
			return nil, derr
		}
		counted, bytes := NewCountingConn(c)
		cl := dbrpc.NewClient(dbrpc.NewStreamConn(counted))
		return &Conn{Client: cl, Close: func() { _ = cl.Close() }, Bytes: bytes}, nil
	}
}

// A projection either chases references or it does not, and the comparison must pair arms that
// make the same choice.
//
// TextProjected chases them, so each row it ships is self-contained; Wire does not. Pairing those
// two charges one arm for a closure the other was never asked to produce and reports the
// difference as the transport's. WireRefs is the arm that pairs with TextProjected.
//
// Asserted on the BYTES, which is where the closure shows: a reference-chasing projection ships
// the attributes the projected expressions read, so it cannot be narrower than the subset form
// over a projection that names an expression.
func TestWireRefsIsTheArmThatPairsWithTextProjected(t *testing.T) {
	srv, _, err := Build(ServeConfig{Dir: filepath.Join(t.TempDir(), "scratch"),
		Rows: 1000, Shapes: []Shape{ShapeHistory}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	addr, err := srv.ServeAnonymous(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rep, err := Run(context.Background(), dialAddr(t, addr), Spec{
		Table: "history", Constraint: "true",
		// MemoryUsage is an expression over ResidentSetSize, which the projection omits, so a
		// reference-chasing projection must add it and a subset projection must not.
		Projection: []string{"ClusterId", "Owner", "MemoryUsage"},
		Transports: []Transport{TextProjected, Wire, WireRefs},
	})
	if err != nil {
		t.Fatal(err)
	}
	by := map[Transport]Sample{}
	for _, s := range rep.Samples {
		if s.Err != nil {
			t.Fatalf("%s: %v (%s)", s.Transport, s.Err, s.Note)
		}
		by[s.Transport] = s
	}
	for _, tr := range []Transport{TextProjected, Wire, WireRefs} {
		if by[tr].Rows != 1000 {
			t.Fatalf("%s: %d rows, want 1000", tr, by[tr].Rows)
		}
		t.Logf("%-20s %7.1f B/row", tr, by[tr].BytesPerRow())
	}
	if !(by[WireRefs].BytesPerRow() > by[Wire].BytesPerRow()) {
		t.Errorf("wire-refs ships %.1f B/row against wire's %.1f; the reference closure is not reaching the wire arm, so it is not the counterpart of text-projected",
			by[WireRefs].BytesPerRow(), by[Wire].BytesPerRow())
	}
}
