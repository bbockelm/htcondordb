package repl

import (
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// THE STORE'S INDEX-BASED EQUI JOIN, EXERCISED FROM HERE.
//
// The store narrows the hashed side's scan to the key values the streamed side turns out to
// hold, instead of reading that side whole to build buckets most of which are never probed.
// It decides from three numbers a client does not have: whether an index covers the attribute,
// how many records the hashed side's read visits, and how many of them the keys reach.
//
// This exercises it over the transport a deployment uses -- a statement through dbrpc, not a
// direct call into the store -- because an access path that works in the store and is not
// reachable from a statement is not a feature anyone has. Two things are checked, and they
// need different instruments:
//
//   - THAT IT RUNS. The join's own diagnostics (which attribute was probed, how many keys,
//     and why not when not) are not carried back over dbrpc, so a client cannot see them. The
//     same spec is therefore also run directly against the catalog, which does report them.
//     That is the only part of this file that does not go through the transport, and it is
//     here precisely because the transport drops the evidence.
//   - THAT IT PAYS. Measured by running the same statement against two stores that differ
//     only in whether the key attribute is indexed, which is what decides whether the probe
//     is available at all.
const probeJoinSQL = `SELECT l.ClusterId, r.ExitCode ` +
	`FROM probeleft l JOIN proberight r ON l.ClusterId == r.ClusterId`

// probeFixture is one store reachable as a statement and directly, plus what it holds.
type probeFixture struct {
	cat     *db.Catalog
	exec    *Executor
	ops     *opRecorder
	keys    []int // the left side's key values, which are what the probe narrows to
	cleanup func()
}

// probeShape is the store the fixture builds, in the terms the probe's own decision is
// priced in: how many keys it would narrow to, how many records the scan it replaces reads,
// and how many of those records those keys reach.
type probeShape struct {
	// leftKeys is how many distinct key values the streamed side holds, which is how many
	// the membership constraint would name.
	leftKeys int
	// rightRows is how many records the hashed side's scan reads. It is the denominator
	// the probe is priced against, and is held constant across key counts for that reason.
	rightRows int
	// dupPerKey is how many records of the hashed side each distinct key reaches. At 1 a
	// key reaches one record and narrowing to k keys reads k records; above it the probe
	// reads k*dupPerKey, which is the half of the price a key count alone does not say.
	dupPerKey int
	// indexed is the one difference between the benchmark's two arms: a value index on the
	// join attribute is what makes the probe possible, and without one the membership
	// constraint would be a filter over a scan that still reads every record, which costs
	// more than it saves.
	indexed bool
}

// newProbeFixture builds the store that shape describes.
//
// Both sides are archives because that is the shape a history-to-epoch join reads, and
// because the records a key reaches are what the store's decision prices: narrowing pays
// while the lookups plus the records they reach stay under a share of the records the scan
// would have read.
func newProbeFixture(t testing.TB, shape probeShape) *probeFixture {
	t.Helper()
	leftKeys, rightRows, indexed := shape.leftKeys, shape.rightRows, shape.indexed
	dup := max(shape.dupPerKey, 1)
	distinct := max(rightRows/dup, 1)
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := func(value []string) db.ArchiveConfig {
		// Small segments on purpose: a key's records are spread across many of them, which
		// is the shape a deployment's history is in and the shape the probe's lookups are
		// priced for. A fixture of one segment would price a decision no deployment takes.
		return db.ArchiveConfig{SegmentSize: 1 << 12, ValueAttrs: value}
	}
	left, err := cat.CreateArchiveTable("probeleft", cfg([]string{"ClusterId"}))
	if err != nil {
		t.Fatal(err)
	}
	var value []string
	if indexed {
		value = []string{"ClusterId"}
	}
	right, err := cat.CreateArchiveTable("proberight", cfg(value))
	if err != nil {
		t.Fatal(err)
	}
	add := func(a *db.ArchiveTable, text string) {
		ad, perr := classad.Parse(text)
		if perr != nil {
			t.Fatal(perr)
		}
		if aerr := a.Append(ad); aerr != nil {
			t.Fatal(aerr)
		}
	}
	// The left keys are spread across the right side's whole key range rather than taken
	// from its head, so narrowing to them cannot be served by reading one end of the table.
	var keys []int
	for i := 0; i < leftKeys; i++ {
		k := 1 + i*(distinct/max(leftKeys, 1))
		keys = append(keys, k)
		add(left, fmt.Sprintf(`[ ClusterId = %d; Owner = "alice" ]`, k))
	}
	for j := 1; j <= distinct; j++ {
		for d := 0; d < dup; d++ {
			add(right, fmt.Sprintf(`[ ClusterId = %d; ExitCode = %d; Pad = "%s" ]`,
				j, (j+d)%3, strings.Repeat("x", 32)))
		}
	}

	s := dbrpc.NewServerCatalog(cat)
	ops := &opRecorder{}
	cp, sp := net.Pipe()
	go func() {
		_ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{
			Privileged: true,
			QueryLog:   func(q dbrpc.QueryLog) { ops.addLog(q) },
		})
	}()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp), dbrpc.WithPeerSessionAttrs(s.SessionAttrs()))
	return &probeFixture{
		cat:  cat,
		exec: NewExecutor(c, ExecConfig{JoinPlacement: JoinPlacementStore}),
		ops:  ops,
		keys: keys,
		cleanup: func() {
			c.Close()
			s.Close()
			cat.Close()
		},
	}
}

// probeSpec is the join htcondordb sends for probeJoinSQL, as the store receives it.
func probeSpec() db.JoinSpec {
	return db.JoinSpec{
		Kind:  db.JoinInner,
		Left:  db.JoinSide{Table: "probeleft", Alias: "l", Project: []string{"ClusterId"}},
		Right: db.JoinSide{Table: "proberight", Alias: "r", Project: []string{"ExitCode", "ClusterId"}},
		Equi:  []db.JoinEqui{{Left: "ClusterId", Right: "ClusterId"}},
	}
}

// The probe has to fire for the join htcondordb actually sends, not merely for one the store
// can be handed in a unit test.
func TestStoreSideIndexProbeFiresForTheJoinHTCondorDBSends(t *testing.T) {
	for _, c := range []struct {
		name    string
		indexed bool
	}{{"indexed", true}, {"not indexed", false}} {
		t.Run(c.name, func(t *testing.T) {
			fix := newProbeFixture(t, probeShape{leftKeys: 16, rightRows: 8192, indexed: c.indexed})
			defer fix.cleanup()

			// Through the transport first: the rows are the answer, and the arm is the op.
			fix.ops.reset()
			res, err := fix.exec.ExecString(probeJoinSQL)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Rows) != len(fix.keys) {
				t.Fatalf("the join returned %d rows, expected %d", len(res.Rows), len(fix.keys))
			}
			if got := fix.ops.waitOps(t, 1); !strings.HasPrefix(got[0], "Join") {
				t.Fatalf("the statement did not reach the store's join: %v", got)
			}

			// Then the same spec directly, which is the only way to see WHICH access path
			// the store took: dbrpc does not carry the join's probe diagnostics back.
			var stats db.JoinStats
			seq, err := fix.cat.Join(probeSpec(), db.JoinLimits{}, &stats)
			if err != nil {
				t.Fatal(err)
			}
			rows := 0
			for range seq {
				rows++
			}
			if stats.Err != nil {
				t.Fatal(stats.Err)
			}
			if rows != len(fix.keys) {
				t.Fatalf("the direct join returned %d rows, expected %d", rows, len(fix.keys))
			}
			if c.indexed {
				if stats.ProbeKeys != len(fix.keys) || stats.ProbeAttr != "ClusterId" {
					t.Errorf("the scan was not narrowed: attr=%q keys=%d why=%q",
						stats.ProbeAttr, stats.ProbeKeys, stats.ProbeWhy)
				}
			} else if stats.ProbeKeys != 0 {
				t.Errorf("a side with no index on the key was narrowed anyway: keys=%d",
					stats.ProbeKeys)
			} else if stats.ProbeWhy == "" {
				t.Error("the scan was not narrowed and no reason was given")
			}
		})
	}
}

// BenchmarkStoreSideIndexProbe sweeps the key count, which is what the store's own
// measurement of this access path sweeps.
//
// The two arms are two STORES, identical but for the value index on the join attribute,
// because that is what decides whether the probe is available. Both run the same statement
// through dbrpc. Arm order is flipped between rounds: the first arm warms the allocator and
// the page cache for the second, and a single order credits whichever ran second.
func BenchmarkStoreSideIndexProbe(b *testing.B) {
	const rightRows = 8192
	for _, keys := range []int{16, 32, 64, 128, 256} {
		indexed := newProbeFixture(b, probeShape{leftKeys: keys, rightRows: rightRows, indexed: true})
		plain := newProbeFixture(b, probeShape{leftKeys: keys, rightRows: rightRows, indexed: false})
		// Both arms must return the same rows before either is timed: an arm that quietly
		// returned fewer would be timed as the faster one.
		ires, err := indexed.exec.ExecString(probeJoinSQL)
		if err != nil {
			b.Fatal(err)
		}
		pres, err := plain.exec.ExecString(probeJoinSQL)
		if err != nil {
			b.Fatal(err)
		}
		sortRowsForCompare(ires)
		sortRowsForCompare(pres)
		if render(ires) != render(pres) {
			b.Fatalf("keys=%d: the indexed and unindexed stores returned different rows", keys)
		}
		for round := 0; round < 2; round++ {
			arms := []struct {
				name string
				fix  *probeFixture
			}{{"indexed", indexed}, {"no-index", plain}}
			if round%2 == 1 {
				arms[0], arms[1] = arms[1], arms[0]
			}
			for _, arm := range arms {
				b.Run(fmt.Sprintf("keys=%d/r%d/%s", keys, round, arm.name), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if _, eerr := arm.fix.exec.ExecString(probeJoinSQL); eerr != nil {
							b.Fatal(eerr)
						}
					}
				})
			}
		}
		indexed.cleanup()
		plain.cleanup()
	}
}

// THE CROSSOVER, AND THE STORE DECLINING AT IT.
//
// A probe looks each key up and then READS the records that key reaches, so what it costs is
// the lookups plus those records, and what it saves is the scan of every record the hashed
// side holds. It pays while the first stays under a share of the second. That makes the
// decision a comparison between the records the keys reach and the records the table holds,
// NOT a property of the key count alone -- and it is why the probe is a decision at all
// rather than an optimization that is always taken.
//
// A key count alone cannot move it, which is why this fixture gives each key SIXTEEN records
// rather than one. The hashed side is 8,192 records either way; what changes is how much of
// it the keys reach. At 32 keys they reach 512 records and narrowing pays; at 128 they reach
// 2,048, a quarter of the table, and it does not.
//
// This pins both sides of that and pins that the decline SAYS SO. An access path that was
// priced and rejected reads exactly like one that was never available, unless the reason is
// reported.
func TestStoreSideIndexProbeDeclinesPastTheCrossover(t *testing.T) {
	for _, c := range []struct {
		keys     int
		narrowed bool
	}{{32, true}, {128, false}} {
		t.Run(fmt.Sprintf("keys=%d", c.keys), func(t *testing.T) {
			fix := newProbeFixture(t, probeShape{
				leftKeys: c.keys, rightRows: 8192, dupPerKey: 16, indexed: true,
			})
			defer fix.cleanup()
			var stats db.JoinStats
			seq, err := fix.cat.Join(probeSpec(), db.JoinLimits{}, &stats)
			if err != nil {
				t.Fatal(err)
			}
			for range seq {
			}
			if c.narrowed {
				if stats.ProbeKeys != c.keys {
					t.Errorf("inside the crossover the scan was not narrowed: keys=%d why=%q",
						stats.ProbeKeys, stats.ProbeWhy)
				}
				return
			}
			if stats.ProbeKeys != 0 {
				t.Errorf("past the crossover the scan was narrowed anyway: keys=%d", stats.ProbeKeys)
			}
			for _, want := range []string{"128 keys", "reaching about", "share of the 8192 records"} {
				if !strings.Contains(stats.ProbeWhy, want) {
					t.Errorf("the decline does not say %q: %q", want, stats.ProbeWhy)
				}
			}
		})
	}
}
