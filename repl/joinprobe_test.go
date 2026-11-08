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
// how many records the hashed side's read visits, and how many segments it spans.
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

// newProbeFixture builds a left side of leftKeys rows and a right side of rightRows, the
// right side's key drawn from the left side's keys in turn.
//
// indexed is the one difference between the two arms: a value index on the join attribute is
// what makes the probe possible, and without one the membership constraint would be a filter
// over a scan that still reads every record, which costs more than it saves.
//
// Both sides are archives because that is the shape a history-to-epoch join reads, and
// because the segment count is part of what the store's decision prices -- an `in` probe of
// k values across s segments costs s*k lookups against the r records the scan would read.
func newProbeFixture(t testing.TB, leftKeys, rightRows int, indexed bool) *probeFixture {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := func(value []string) db.ArchiveConfig {
		// Small segments on purpose: the store prices the probe against the number of
		// segments its lookups have to visit, so a fixture of one segment would price a
		// decision no deployment takes.
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
		k := 1 + i*(rightRows/max(leftKeys, 1))
		keys = append(keys, k)
		add(left, fmt.Sprintf(`[ ClusterId = %d; Owner = "alice" ]`, k))
	}
	for j := 1; j <= rightRows; j++ {
		add(right, fmt.Sprintf(`[ ClusterId = %d; ExitCode = %d; Pad = "%s" ]`,
			j, j%3, strings.Repeat("x", 32)))
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
			fix := newProbeFixture(t, 16, 8192, c.indexed)
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
		indexed := newProbeFixture(b, keys, rightRows, true)
		plain := newProbeFixture(b, keys, rightRows, false)
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
// A sealed segment carries its own index, so an `in` probe of k values across s segments
// costs s*k lookups where the scan it replaces costs r records. That makes the decision a
// comparison between s*k and r rather than a property of the key count alone, and it is the
// reason the probe is a decision at all instead of an optimization that is always taken.
//
// This pins both sides of it on one fixture -- 8,192 records over about 210 segments, so the
// crossover falls between 32 keys and 64 -- and pins that the decline SAYS SO. An access path
// that was priced and rejected reads exactly like one that was never available, unless the
// reason is reported.
func TestStoreSideIndexProbeDeclinesPastTheCrossover(t *testing.T) {
	for _, c := range []struct {
		keys     int
		narrowed bool
	}{{32, true}, {64, false}} {
		t.Run(fmt.Sprintf("keys=%d", c.keys), func(t *testing.T) {
			fix := newProbeFixture(t, c.keys, 8192, true)
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
			for _, want := range []string{"index lookups", "records", "costs more than it saves"} {
				if !strings.Contains(stats.ProbeWhy, want) {
					t.Errorf("the decline does not say %q: %q", want, stats.ProbeWhy)
				}
			}
		})
	}
}
