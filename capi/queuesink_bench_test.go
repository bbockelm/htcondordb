package main

// A measurement of the response path the schedd actually takes, against a
// database that is actually running, with the ads actually going out on a
// socket. Everything cheaper than this -- net.Pipe, a discarded buffer, an
// in-process store -- leaves out one of the three things the path is made of.
//
// Gated on HTCONDORDB_SINK_BENCH naming the daemon's address (or "auto" to take
// it from the ambient configuration), because it needs a populated mirror.

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func sinkBenchConn(tb testing.TB) *conn {
	tb.Helper()
	addr := os.Getenv("HTCONDORDB_SINK_BENCH")
	if addr == "" {
		tb.Skip("set HTCONDORDB_SINK_BENCH=<addr|auto> to run against a live database")
	}
	if addr == "auto" {
		addr = ""
	}
	c, err := openConnAs(addr, "SCHEDD")
	if err != nil {
		tb.Fatalf("connecting to the database: %v", err)
	}
	return c
}

// drainedSocket is a TCP connection whose far end is read and thrown away as
// fast as it arrives, standing in for a condor_q that never stalls. It returns
// the near side's descriptor, which the sink takes ownership of.
func drainedSocket(tb testing.TB) (fd int, bytesRead *int64, wait func()) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	type res struct {
		n   int64
		err error
	}
	done := make(chan res, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- res{0, err}
			return
		}
		n, err := io.Copy(io.Discard, c)
		_ = c.Close()
		done <- res{n, err}
	}()
	cl, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		tb.Fatal(err)
	}
	tc := cl.(*net.TCPConn)
	f, err := tc.File() // a dup of the descriptor, in blocking mode
	if err != nil {
		tb.Fatal(err)
	}
	_ = tc.Close()
	var total int64
	return int(f.Fd()), &total, func() {
		_ = f.Close()
		r := <-done
		// The listener outlives the dial: closing it before Accept has run resets
		// the pending connection, which reaches the writer as a broken pipe part
		// way through a long response.
		_ = ln.Close()
		total = r.n
	}
}

// liveSink drives one query down the response-writing path and reports how long
// it took and how many ads went out.
func liveSink(tb testing.TB, c *conn, constraint string, attrs []string, counts bool) (time.Duration, int64, int64) {
	fd, read, wait := drainedSocket(tb)
	sink, err := newFDSink(fd, nil)
	if err != nil {
		tb.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &queueRequest{client: &queueClient{notifyFD: -1}, chunks: make(chan queueChunk, chunksInFlight),
		cancel: cancel, sink: sink}
	b := &adRecordBuilder{emit: true, cedarFrame: true}
	req := attrs
	if counts {
		var suppress []string
		req, suppress = withCountingAttrs(attrs)
		b.suppress = suppress
	}
	start := time.Now()
	r.wg.Add(1)
	r.stream(ctx, c.dbc, benchTable(), constraint, req, 0, b, counts)
	_ = sink.finish()
	d := time.Since(start)
	wait()
	if r.err != nil {
		tb.Fatalf("query failed: %v", r.err)
	}
	return d, r.written.Load(), *read
}

// liveChunks is the same query down the chunked path, with the caller's copy
// stood in for by a read of each chunk -- what the in-loop mode pays in Go.
func liveChunks(tb testing.TB, c *conn, constraint string, attrs []string, counts bool) (time.Duration, int64, int64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &queueRequest{client: &queueClient{notifyFD: -1}, chunks: make(chan queueChunk, chunksInFlight), cancel: cancel}
	b := &adRecordBuilder{emit: true}
	req := attrs
	if counts {
		var suppress []string
		req, suppress = withCountingAttrs(attrs)
		b.suppress = suppress
	}
	start := time.Now()
	r.wg.Add(1)
	go func() { r.stream(ctx, c.dbc, benchTable(), constraint, req, 0, b, counts); close(r.chunks) }()
	var ads, bytes int64
	for ch := range r.chunks {
		ads += int64(ch.ads)
		bytes += int64(len(ch.data))
	}
	d := time.Since(start)
	if r.err != nil {
		tb.Fatalf("query failed: %v", r.err)
	}
	return d, ads, bytes
}

// TestSinkCost times one query shape all the way to a socket, and the same
// shape down the chunked path, so the two response modes can be compared on a
// real mirror. HTCONDORDB_SINK_SHAPES names the shapes, as
// "<constraint>|<comma-separated projection>" separated by semicolons.
func TestSinkCost(t *testing.T) {
	c := sinkBenchConn(t)
	defer closeConn(c)
	for _, tc := range benchShapes() {
		var bestSink, bestChunk time.Duration = time.Hour, time.Hour
		var nads, nbytes, cbytes int64
		for i := 0; i < 3; i++ {
			d, a, bts := liveSink(t, c, tc.cons, tc.attrs, true)
			if d < bestSink {
				bestSink, nads, nbytes = d, a, bts
			}
			if d, _, bts = liveChunks(t, c, tc.cons, tc.attrs, true); d < bestChunk {
				bestChunk, cbytes = d, bts
			}
		}
		fmt.Printf("%-24s %-40s sink %8.1f ms (%6d ads, %10d B, %6.2f us/ad)  chunks %8.1f ms (%10d B)\n",
			tc.cons, strings.Join(tc.attrs, ","),
			float64(bestSink.Microseconds())/1000, nads, nbytes,
			float64(bestSink.Microseconds())/float64(nads),
			float64(bestChunk.Microseconds())/1000, cbytes)
	}
}

type benchShape struct {
	cons  string
	attrs []string
}

// benchShapes reads the shapes to measure, defaulting to the pair that says
// what a constraint costs: the same whole result set with and without one.
func benchShapes() []benchShape {
	spec := os.Getenv("HTCONDORDB_SINK_SHAPES")
	if spec == "" {
		spec = "true|ClusterId,ProcId,JobStatus;ProcId >= 0|ClusterId,ProcId,JobStatus"
	}
	var out []benchShape
	for _, part := range strings.Split(spec, ";") {
		cons, proj, _ := strings.Cut(part, "|")
		out = append(out, benchShape{cons: cons, attrs: splitList(proj)})
	}
	return out
}

// benchTable is the mirrored job table to read, defaulting to the one the
// schedd's client asks for.
func benchTable() string {
	if t := os.Getenv("HTCONDORDB_SINK_TABLE"); t != "" {
		return t
	}
	return "jobs"
}

// TestRawWireCost times the database leg on its own -- no record building, no
// socket -- for each shape, with the projection and with the whole-ad request
// the response used to be built from. It is the measurement that says whether a
// slow query is the database's doing or this library's.
func TestRawWireCost(t *testing.T) {
	c := sinkBenchConn(t)
	defer closeConn(c)
	for _, tc := range benchShapes() {
		for _, whole := range []bool{false, true} {
			attrs := tc.attrs
			if whole {
				attrs = nil
			}
			best, rows, bytes := time.Hour, 0, 0
			for i := 0; i < 3; i++ {
				n, nb := 0, 0
				s := time.Now()
				err := c.dbc.QueryRawWireStream(context.Background(), benchTable(), tc.cons, attrs, 0, true,
					func(w []byte) bool { n++; nb += len(w); return true })
				if err != nil {
					t.Fatalf("%s: %v", tc.cons, err)
				}
				if d := time.Since(s); d < best {
					best, rows, bytes = d, n, nb
				}
			}
			shape := "projected"
			if whole {
				shape = "whole ad"
			}
			fmt.Printf("%-24s %-10s %8.1f ms  %6d rows %10d B  %6.2f us/row\n",
				tc.cons, shape, float64(best.Microseconds())/1000, rows, bytes,
				float64(best.Microseconds())/float64(rows))
		}
	}
}

// BenchmarkSink is the profiling entry point: one query of the first shape per
// iteration, down the path the schedd's off-thread mode uses. Run it with the
// client library's own pprof endpoint on (HTCONDORDB_CLIENT_PPROF) to see both
// ends of the path at once.
func BenchmarkSink(b *testing.B) {
	c := sinkBenchConn(b)
	defer closeConn(c)
	sh := benchShapes()[0]
	var ads int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, a, _ := liveSink(b, c, sh.cons, sh.attrs, true)
		ads = a
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N)/float64(ads), "us/ad")
}
