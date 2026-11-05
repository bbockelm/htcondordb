package repl

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// PRICING THE LINK, which is the one input the server-side join decision has never had.
//
// The controlled sweep next door establishes that moving the pairing into the store costs no
// time: over the whole expansion range the store arm is faster or tied, and in allocations it
// crosses 1.0 between k=1 and k=2. What it cannot say anything about is bandwidth. It runs over
// net.Pipe, an in-memory pipe that charges for serialization and framing and nothing at all for
// the link, so it measures a deployment with infinite capacity between client and store.
//
// That matters because the two arms ship DIFFERENT AMOUNTS. With k right rows per left row and
// per-side projected widths l and r:
//
//	store ships   |L| * k * (l + r)      the left ad repeated once per output row
//	client ships  |L| * (l + k*r)        each side exactly once
//
// below 1 exactly when k < 1, measured at 0.63x for k=0.25 and 1.36x for k=8. So the store wins
// CPU everywhere and loses BYTES above k=1, and which of those dominates is a property of the
// connection, not of the statement. A benchmark that cannot vary the connection cannot answer it.
//
// This one varies it. Both arms run over real TCP wrapped in a writer that charges each byte
// against a fixed capacity, and the sweep reports where the store's CPU win stops covering its
// extra bytes.
//
// WHAT THIS DOES NOT MODEL, and the numbers should be read with it in mind. Capacity is charged,
// propagation delay is not: there is no artificial RTT here, because charging one per Write would
// penalize a pipelined stream for writes a real link overlaps, and detecting a genuine
// request/response turnaround from inside a conn wrapper is not something this can do honestly.
// Leaving latency out biases the result TOWARDS the client arm, which issues two whole side reads
// where the store arm issues one join -- so a crossover found here is a conservative bound, and a
// link with real RTT moves it in the store's favour.

// linkConn charges a connection's writes against a fixed capacity and counts the bytes.
//
// The debt accounting is what makes this usable at gigabit speeds. A single header write at
// 1 Gbps owes well under a microsecond, and time.Sleep cannot deliver that -- asking for it would
// measure the scheduler's granularity rather than the link. Debt accumulates instead and is paid
// only once it is worth a sleep, and the sleep's OVERRUN is credited back, so a long run converges
// on the requested rate rather than drifting slower with every rounding.
type linkConn struct {
	net.Conn
	bytesPerSec int64
	written     *atomic.Int64

	mu   sync.Mutex
	debt time.Duration // may go negative: that is credit from a previous oversleep
}

// minLinkSleep is the debt below which it is cheaper to keep owing than to sleep.
const minLinkSleep = time.Millisecond

func (c *linkConn) Write(p []byte) (int, error) {
	c.written.Add(int64(len(p)))

	c.mu.Lock()
	c.debt += time.Duration(len(p)) * time.Second / time.Duration(c.bytesPerSec)
	owed := c.debt
	if owed >= minLinkSleep {
		c.debt = 0
	}
	c.mu.Unlock()

	if owed >= minLinkSleep {
		start := time.Now()
		time.Sleep(owed)
		// Sleep overshoots. Carry the overshoot back as credit so the rate is right on
		// average instead of biased slow by one granularity per sleep.
		if over := time.Since(start) - owed; over > 0 {
			c.mu.Lock()
			c.debt -= over
			c.mu.Unlock()
		}
	}
	return c.Conn.Write(p)
}

// linkPair returns a connected pair of TCP conns, both ends charged at bytesPerSec, plus the
// byte counters for each direction.
//
// Real TCP rather than net.Pipe: net.Pipe is synchronous and unbuffered, so every Write blocks
// until the far end reads it, and adding sleeps to that serializes the two sides in a way no
// socket does.
func linkPair(tb testing.TB, bytesPerSec int64) (client, server net.Conn, fromClient, fromServer *atomic.Int64) {
	tb.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	defer l.Close()
	type res struct {
		c   net.Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, aerr := l.Accept()
		ch <- res{c, aerr}
	}()
	cc, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		tb.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		tb.Fatal(r.err)
	}
	tb.Cleanup(func() { cc.Close(); r.c.Close() })

	fromClient, fromServer = &atomic.Int64{}, &atomic.Int64{}
	return &linkConn{Conn: cc, bytesPerSec: bytesPerSec, written: fromClient},
		&linkConn{Conn: r.c, bytesPerSec: bytesPerSec, written: fromServer},
		fromClient, fromServer
}

// linkFixture is one store reachable two ways over a capacity-limited link, with the bytes each
// arm moved.
type linkFixture struct {
	exec    *Executor // client-side pairing
	srvExec *Executor // pairing in the store
	ops     *opRecorder
	bytes   map[*Executor]*atomic.Int64 // server->client bytes, which is the direction that differs
	rows    int
	cleanup func()
}

// newLinkJoinFixture builds the same history/epoch data the expansion sweep uses, and serves it
// over two separately throttled connections -- one advertising the server-side join and one not.
//
// Each arm gets its OWN link so their byte counters do not mix, and so neither arm's traffic eats
// the other's capacity.
func newLinkJoinFixture(b testing.TB, left int, k float64, bytesPerSec int64) *linkFixture {
	b.Helper()
	cat, err := db.OpenCatalog(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	hist, err := cat.CreateTable("history")
	if err != nil {
		b.Fatal(err)
	}
	epoch, err := cat.CreateTable("epoch")
	if err != nil {
		b.Fatal(err)
	}
	put := func(t *db.DB, key string, ad *classad.ClassAd) {
		if werr := t.Put(key, ad); werr != nil {
			b.Fatal(werr)
		}
	}
	rows := 0
	for i := 1; i <= left; i++ {
		put(hist, fmt.Sprint(i), benchHistoryAd(i))
		// k may be fractional: k=0.25 is one epoch row per four history rows, the selective
		// direction, which is where the store ships FEWER bytes as well as less CPU.
		n := int(float64(i)*k) - int(float64(i-1)*k)
		for inst := 0; inst < n; inst++ {
			put(epoch, fmt.Sprintf("%d.%d", i, inst), benchEpochAd(i, inst))
			rows++
		}
	}

	s := dbrpc.NewServerCatalog(cat)
	ops := &opRecorder{}
	dial := func(advertise bool) (*dbrpc.Client, *atomic.Int64) {
		cc, sc, _, fromServer := linkPair(b, bytesPerSec)
		go func() {
			_ = s.ServeConnOpts(dbrpc.NewStreamConn(sc), dbrpc.ServeOptions{
				Privileged: true,
				QueryLog:   func(q dbrpc.QueryLog) { ops.add(q.Op) },
			})
		}()
		conn := dbrpc.NewStreamConn(cc)
		if advertise {
			return dbrpc.NewClient(conn, dbrpc.WithPeerSessionAttrs(s.SessionAttrs())), fromServer
		}
		return dbrpc.NewClient(conn), fromServer
	}
	c, cBytes := dial(false)
	sc, sBytes := dial(true)
	ex := NewExecutor(c, ExecConfig{})
	sex := NewExecutor(sc, ExecConfig{ServerSideJoin: true})
	return &linkFixture{
		exec:    ex,
		srvExec: sex,
		ops:     ops,
		bytes:   map[*Executor]*atomic.Int64{ex: cBytes, sex: sBytes},
		rows:    rows,
		cleanup: func() { c.Close(); sc.Close(); s.Close(); cat.Close() },
	}
}

// linkBandwidths are the capacities swept, as bytes per second.
//
// 10 Gbps stands in for same-rack or loopback, where the link is not the constraint; 10 Mbps for
// a genuinely constrained WAN. The decision this benchmark informs is a deployment setting, so the
// range is chosen to bracket real deployments rather than to find a precise crossover.
var linkBandwidths = []struct {
	name string
	bps  int64
}{
	{"10Gbps", 10_000_000_000 / 8},
	{"1Gbps", 1_000_000_000 / 8},
	{"100Mbps", 100_000_000 / 8},
	{"10Mbps", 10_000_000 / 8},
}

// BenchmarkJoinOverLink sweeps link capacity against the expansion factor.
//
// Read it against the unthrottled sweep next door: there the store arm wins at every k. Here the
// question is whether that CPU win survives shipping k*(l+r) bytes where the client ships l+k*r,
// and at which capacity it stops doing so.
func BenchmarkJoinOverLink(b *testing.B) {
	const left = 2048
	for _, bw := range linkBandwidths {
		for _, k := range []float64{0.25, 1, 2, 8} {
			fix := newLinkJoinFixture(b, left, k, bw.bps)

			// Agreement and path evidence BEFORE timing, exactly as the unthrottled sweep
			// does it. A server arm that quietly fell back to pairing here would otherwise be
			// timed as the store arm and would move fewer bytes, which is the one failure
			// this benchmark could not tell from a result.
			fix.ops.reset()
			sres, err := fix.srvExec.ExecString(expansionJoinSQL)
			if err != nil {
				b.Fatalf("%s k=%g in-store: %v", bw.name, k, err)
			}
			cres, err := fix.exec.ExecString(expansionJoinSQL)
			if err != nil {
				b.Fatalf("%s k=%g client-side: %v", bw.name, k, err)
			}
			if len(sres.Rows) != fix.rows || len(cres.Rows) != fix.rows {
				b.Fatalf("%s k=%g: in-store %d rows, client-side %d, expected %d",
					bw.name, k, len(sres.Rows), len(cres.Rows), fix.rows)
			}
			sortRowsForCompare(sres)
			sortRowsForCompare(cres)
			if render(sres) != render(cres) {
				b.Fatalf("%s k=%g: the two arms returned different rows", bw.name, k)
			}
			if got := fix.ops.waitOpsB(b, 1); got[0] != "JoinWire" {
				b.Fatalf("%s k=%g: the in-store arm's first op was %q, not JoinWire -- it fell "+
					"back, and this would price the wrong path: %v", bw.name, k, got[0], got)
			}

			for round := 0; round < 2; round++ {
				arms := []struct {
					name string
					exec *Executor
				}{{"in-store", fix.srvExec}, {"client-side", fix.exec}}
				// The first arm warms caches and the allocator for the second, so the order
				// is flipped between rounds and the pair of readings brackets that rather
				// than hiding it.
				if round%2 == 1 {
					arms[0], arms[1] = arms[1], arms[0]
				}
				for _, arm := range arms {
					name := fmt.Sprintf("%s/k=%g/rows=%d/r%d/%s", bw.name, k, fix.rows, round, arm.name)
					b.Run(name, func(b *testing.B) {
						b.ReportAllocs()
						counter := fix.bytes[arm.exec]
						before := counter.Load()
						for b.Loop() {
							if _, eerr := arm.exec.ExecString(expansionJoinSQL); eerr != nil {
								b.Fatal(eerr)
							}
						}
						// Bytes the SERVER sent, per query: the direction the two arms
						// differ in, and the one the capacity is charged against.
						moved := counter.Load() - before
						b.ReportMetric(float64(moved)/float64(b.N), "srvBytes/op")
					})
				}
			}
			fix.cleanup()
		}
	}
}
