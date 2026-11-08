package repl

import (
	"fmt"
	"net"
	"os"
	"sort"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// How much of a client-side join's cost is shipping both sides and rebuilding every row
// here, rather than joining where the rows already live?
//
// The join in this package fetches both sides whole and pairs them locally, so each side's
// surviving rows are serialized by the server, framed, and rebuilt into ClassAds here
// before a single candidate pair is examined. Three arms isolate what that costs. They
// share one planner, one key-batch decision and one hash join -- the ONLY thing that
// differs is where the rows come from:
//
//	wire      -- fetched through a real dbrpc server (what ships today)
//	projected -- read in-process with the same projection, nothing serialized
//	whole     -- read in-process as whole ads, no projection
//
// Two arms would confound two variables, which is the mistake that made the first version
// of the MATCH columnar benchmark meaningless: "wire" projects, so comparing it only
// against a whole-ad in-process read would credit the wire's share with the projection's.
// "projected" is the floor for removing the wire and nothing else; "whole" prices the
// projection separately.
//
// Cardinalities straddle maxJoinKeyBatch (512), the point where key batching stops folding
// the left side's keys into the right scan and the right table is read whole instead -- so
// the sizes below and above it are different plans, not just different sizes.
//
// Two caveats on the number this produces, both of which understate the wire's cost:
// net.Pipe is an in-memory pipe, so the arm pays serialization and framing but no network
// latency or bandwidth; and the server here shares a process with the client, so the rows
// never leave a warm cache.
var benchReverseJoinArms = os.Getenv("BENCH_REVERSE_ARMS") != ""

// joinBenchFixture holds one store and the two ways of reading it.
type joinBenchFixture struct {
	cat  *db.Catalog
	exec *Executor
	// srvExec reaches the same store over a connection whose handshake advertises the
	// server-side join, so the two differ only in where the pairing happens.
	srvExec *Executor
	cleanup func()
}

// newJoinBenchFixture builds a history archive of n rows, one per cluster, and an epoch
// archive with two rows per cluster. Both are value-indexed on ClusterId, which is what
// gives the key-batch membership constraint an index to probe rather than a predicate to
// evaluate per record.
// newSelectiveJoinFixture is newJoinBenchFixture with a SPARSE right side: only every
// sparsity-th cluster has epoch rows, so the join filters instead of expanding and the output
// is far smaller than either input.
//
// This is the shape the wire argument was actually about, and the one the expanding fixture
// cannot test. Keeping them as separate fixtures rather than a parameter makes it visible at
// the call site which regime a benchmark is in, since that is what decides the answer.
func newSelectiveJoinFixture(b testing.TB, n, sparsity int) *joinBenchFixture {
	b.Helper()
	cat, err := db.OpenCatalog(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	cfg := func(zone []string) db.ArchiveConfig {
		return db.ArchiveConfig{SegmentSize: 1 << 16, ValueAttrs: []string{"ClusterId"}, ZoneAttrs: zone}
	}
	hist, err := cat.CreateArchiveTable("history", cfg([]string{"CompletionDate"}))
	if err != nil {
		b.Fatal(err)
	}
	epoch, err := cat.CreateArchiveTable("epoch", cfg(nil))
	if err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		if aerr := hist.Append(benchHistoryAd(i)); aerr != nil {
			b.Fatal(aerr)
		}
		if i%sparsity != 0 {
			continue
		}
		if aerr := epoch.Append(benchEpochAd(i, 0)); aerr != nil {
			b.Fatal(aerr)
		}
	}
	return serveJoinFixture(b, cat)
}

func newJoinBenchFixture(b testing.TB, n int) *joinBenchFixture {
	b.Helper()
	cat, err := db.OpenCatalog(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	// A small segment size so the appends below seal many segments: an unsealed segment has
	// no value index, and a single-segment archive would measure a scan either way.
	cfg := func(zone []string) db.ArchiveConfig {
		return db.ArchiveConfig{SegmentSize: 1 << 16, ValueAttrs: []string{"ClusterId"}, ZoneAttrs: zone}
	}
	hist, err := cat.CreateArchiveTable("history", cfg([]string{"CompletionDate"}))
	if err != nil {
		b.Fatal(err)
	}
	epoch, err := cat.CreateArchiveTable("epoch", cfg(nil))
	if err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		if aerr := hist.Append(benchHistoryAd(i)); aerr != nil {
			b.Fatal(aerr)
		}
		for inst := 0; inst < 2; inst++ {
			if aerr := epoch.Append(benchEpochAd(i, inst)); aerr != nil {
				b.Fatal(aerr)
			}
		}
	}

	return serveJoinFixture(b, cat)
}

// serveJoinFixture puts a server in front of a catalog and returns the two executors that
// differ only in whether the handshake advertises the join.
func serveJoinFixture(b testing.TB, cat *db.Catalog) *joinBenchFixture {
	b.Helper()
	s := dbrpc.NewServerCatalog(cat)
	dial := func(advertise bool) *dbrpc.Client {
		cp, sp := net.Pipe()
		go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
		conn := dbrpc.NewStreamConn(cp)
		if advertise {
			return dbrpc.NewClient(conn, dbrpc.WithPeerSessionAttrs(s.SessionAttrs()))
		}
		return dbrpc.NewClient(conn)
	}
	c, sc := dial(false), dial(true)
	return &joinBenchFixture{
		cat:     cat,
		exec:    NewExecutor(c, ExecConfig{}),
		srvExec: NewExecutor(sc, ExecConfig{ServerSideJoin: true}),
		cleanup: func() { c.Close(); sc.Close(); s.Close(); cat.Close() },
	}
}

// benchHistoryAd is a completed-job ad at a realistic width. Row width is most of the wire
// cost for an unprojected read, so a three-attribute ad would answer a question nobody has.
// Real history ads carry well over a hundred attributes; this is deliberately conservative.
func benchHistoryAd(cluster int) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttr("ClusterId", int64(cluster))
	ad.InsertAttr("ProcId", 0)
	ad.InsertAttrString("Owner", "alice")
	ad.InsertAttrString("User", "alice@example.org")
	ad.InsertAttr("CompletionDate", int64(1700000000+cluster))
	ad.InsertAttr("QDate", int64(1699000000+cluster))
	ad.InsertAttr("JobStatus", 4)
	ad.InsertAttr("ExitCode", int64(cluster%3))
	ad.InsertAttr("RequestCpus", 1)
	ad.InsertAttr("RequestMemory", 2048)
	ad.InsertAttr("RequestDisk", int64(1<<20))
	ad.InsertAttr("RemoteWallClockTime", int64(3600+cluster))
	ad.InsertAttr("RemoteUserCpu", int64(3000+cluster))
	ad.InsertAttr("RemoteSysCpu", int64(100))
	ad.InsertAttr("MemoryUsage", int64(1500))
	ad.InsertAttr("DiskUsage", int64(900000))
	ad.InsertAttr("NumJobStarts", 1)
	ad.InsertAttr("NumShadowStarts", 1)
	ad.InsertAttr("JobUniverse", 5)
	ad.InsertAttrString("Cmd", "/home/alice/analysis/run.sh")
	ad.InsertAttrString("Iwd", "/home/alice/analysis/workdir")
	ad.InsertAttrString("Args", "--input data.root --output hist.root --verbose")
	ad.InsertAttrString("Out", "job.out")
	ad.InsertAttrString("Err", "job.err")
	ad.InsertAttrString("UserLog", "/home/alice/analysis/job.log")
	ad.InsertAttrString("GlobalJobId", fmt.Sprintf("ap2001.example.org#%d.0#1700000000", cluster))
	ad.InsertAttrString("LastRemoteHost", "slot1_3@wn042.example.org")
	ad.InsertAttrString("RemoteHost", "slot1_3@wn042.example.org")
	ad.InsertAttrString("StartdName", "wn042.example.org")
	ad.InsertAttrString("ProjectName", "CMS_Analysis")
	ad.InsertAttrString("AccountingGroup", "group_physics.alice")
	ad.InsertAttrString("Requirements", "(TARGET.Arch == \"X86_64\") && (TARGET.OpSys == \"LINUX\")")
	ad.InsertAttrString("Environment", "PATH=/usr/bin;HOME=/home/alice;LANG=C")
	ad.InsertAttrString("TransferInput", "data.root,setup.sh,analysis.tar.gz")
	ad.InsertAttrString("TransferOutput", "hist.root")
	ad.InsertAttr("BytesSent", int64(4096*cluster))
	ad.InsertAttr("BytesRecvd", int64(8192*cluster))
	ad.InsertAttrBool("WantCheckpoint", false)
	ad.InsertAttrBool("OnExitHold", false)
	ad.InsertAttr("ImageSize", int64(2000))
	return ad
}

// benchEpochAd is a per-run-instance record: narrower than a history ad, as the real ones are.
func benchEpochAd(cluster, instance int) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttr("ClusterId", int64(cluster))
	ad.InsertAttr("ProcId", 0)
	ad.InsertAttr("RunInstanceId", int64(instance))
	ad.InsertAttr("ExitCode", int64((cluster+instance)%2))
	ad.InsertAttrString("Owner", "alice")
	ad.InsertAttr("NumShadowStarts", int64(instance+1))
	ad.InsertAttr("RemoteWallClockTime", int64(1800+cluster+instance))
	ad.InsertAttr("RemoteUserCpu", int64(1700+cluster))
	ad.InsertAttr("MemoryUsage", int64(1400+instance))
	ad.InsertAttr("DiskUsage", int64(800000))
	ad.InsertAttr("BytesSent", int64(2048*cluster))
	ad.InsertAttr("BytesRecvd", int64(4096*cluster))
	ad.InsertAttrString("LastRemoteHost", "slot1_3@wn042.example.org")
	ad.InsertAttrString("StartdName", "wn042.example.org")
	ad.InsertAttrString("TerminationReason", "normal exit")
	ad.InsertAttr("EpochWriteDate", int64(1700000000+cluster))
	return ad
}

// joinFetch reads one side of a join. The three arms are three of these and differ in
// nothing else.
type joinFetch func(st *Statement, s joinSide, side int) ([]*classad.ClassAd, error)

func (f *joinBenchFixture) fetchWire(st *Statement, s joinSide, side int) ([]*classad.ClassAd, error) {
	return f.exec.fetchJoinSide(st, s, side)
}

// fetchWhole reads the side's ads in-process. Nothing is serialized, framed or parsed.
func (f *joinBenchFixture) fetchWhole(_ *Statement, s joinSide, _ int) ([]*classad.ClassAd, error) {
	t, ok := f.cat.ArchiveTable(s.ref.Table)
	if !ok {
		return nil, fmt.Errorf("no archive %q", s.ref.Table)
	}
	seq, err := t.Query(constraint(s.where))
	if err != nil {
		return nil, err
	}
	var out []*classad.ClassAd
	for ad := range seq {
		out = append(out, ad)
	}
	return out, nil
}

// fetchProjected reads the same attributes the wire arm projects and assembles each row
// into an ad in-process, so the arms differ only by the serialize/frame/rebuild round trip.
func (f *joinBenchFixture) fetchProjected(st *Statement, s joinSide, side int) ([]*classad.ClassAd, error) {
	if s.project == nil {
		return f.fetchWhole(st, s, side)
	}
	t, ok := f.cat.ArchiveTable(s.ref.Table)
	if !ok {
		return nil, fmt.Errorf("no archive %q", s.ref.Table)
	}
	seq, err := t.QueryProject(constraint(s.where), s.project)
	if err != nil {
		return nil, err
	}
	var out []*classad.ClassAd
	for vals := range seq {
		ad := classad.New()
		for i, name := range s.project {
			if i < len(vals) {
				insertBenchValue(ad, name, vals[i])
			}
		}
		out = append(out, ad)
	}
	return out, nil
}

// insertBenchValue stores a projected value under name. A value with no scalar form is left
// out, which reads back as undefined -- the same thing a projection that could not supply
// it would produce.
func insertBenchValue(ad *classad.ClassAd, name string, v classad.Value) {
	switch {
	case v.IsBool():
		x, _ := v.BoolValue()
		ad.InsertAttrBool(name, x)
	case v.IsInteger():
		x, _ := v.IntValue()
		ad.InsertAttr(name, x)
	case v.IsReal():
		x, _ := v.RealValue()
		ad.InsertAttrFloat(name, x)
	case v.IsString():
		x, _ := v.StringValue()
		ad.InsertAttrString(name, x)
	}
}

// runBenchJoin is execJoin's fetch-batch-join core, with the fetch left open. Keeping the
// planning, the key-batch decision and the hash join identical across arms is the whole
// point: whatever the arms differ by, it is not any of those.
//
// The key-batch decision is the shipped one, so this measures the plan a real query gets --
// including the cheap row count it asks the server for, which every arm pays equally.
func (f *joinBenchFixture) runBenchJoin(st *Statement, fetch joinFetch) (int, error) {
	p, err := planJoin(st)
	if err != nil {
		return 0, err
	}
	leftAds, err := fetch(st, p.sides[0], 0)
	if err != nil {
		return 0, err
	}
	right := p.sides[1]
	if d := f.exec.decideKeyBatch(&p.steps[0], p.sides[1], p.wrapFirst(leftAds)); d.constraint != "" {
		right.where = andText(right.where, d.constraint)
	}
	rightAds, err := fetch(st, right, 1)
	if err != nil {
		return 0, err
	}
	rows, err := joinTwoSides(p, leftAds, rightAds)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

func BenchmarkJoinWireCost(b *testing.B) {
	shapes := []struct {
		name string
		sql  string
		// projected says whether this shape pushes a projection, and so whether the
		// in-process projected arm differs from the whole-ad one at all.
		projected bool
	}{
		{
			name:      "narrow",
			sql:       `SELECT h.ClusterId, h.Owner, e.RunInstanceId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`,
			projected: true,
		},
		{
			// SELECT * projects nothing, so both sides cross the wire whole. This is the
			// shape a person writes when they want the joined records rather than a
			// summary of them.
			name: "star",
			sql:  `SELECT * FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`,
		},
	}

	// 128 and 400 keep key batching (it gives up at maxJoinKeyBatch = 512 distinct keys);
	// 2048 and 8192 are past it, where the right side is read whole.
	for _, n := range []int{128, 400, 2048, 8192} {
		fix := newJoinBenchFixture(b, n)
		for _, shape := range shapes {
			st, err := Parse(shape.sql)
			if err != nil {
				b.Fatal(err)
			}
			arms := []struct {
				name  string
				fetch joinFetch
			}{
				{"wire", fix.fetchWire},
				{"inproc-whole", fix.fetchWhole},
			}
			if shape.projected {
				arms = append(arms, struct {
					name  string
					fetch joinFetch
				}{"inproc-projected", fix.fetchProjected})
			}
			if benchReverseJoinArms {
				for i, j := 0, len(arms)-1; i < j; i, j = i+1, j-1 {
					arms[i], arms[j] = arms[j], arms[i]
				}
			}

			// Agreement before timing. Arms that answer different questions cannot be
			// compared, and a join whose fetch quietly drops rows would otherwise look
			// like a fast one.
			want := -1
			for _, arm := range arms {
				got, rerr := fix.runBenchJoin(st, arm.fetch)
				if rerr != nil {
					b.Fatalf("n=%d %s/%s: %v", n, shape.name, arm.name, rerr)
				}
				if want == -1 {
					want = got
				} else if got != want {
					b.Fatalf("n=%d %s: arm %s produced %d rows, another produced %d",
						n, shape.name, arm.name, got, want)
				}
			}
			if want != 2*n {
				b.Fatalf("n=%d %s: joined %d rows, expected %d", n, shape.name, want, 2*n)
			}

			for _, arm := range arms {
				b.Run(fmt.Sprintf("n=%d/%s/%s", n, shape.name, arm.name), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						if _, rerr := fix.runBenchJoin(st, arm.fetch); rerr != nil {
							b.Fatal(rerr)
						}
					}
				})
			}
		}
		fix.cleanup()
	}
}

// Key batching folds the left side's key values into one membership constraint on the right
// scan, so a selective left side reads a slice of the right table instead of all of it. The
// wire benchmark above shows every arm slowing down together at 400 keys -- a cost all three
// share, and the constraint is the only candidate, since it is the one thing runBenchJoin
// applies before any arm's fetch runs.
//
// This prices the trade directly, in-process on both arms so nothing else is in the way. The
// right table is held at a fixed, large size and the LEFT side is narrowed to k clusters, so
// batching has something real to exclude: it reads ~2k epoch rows where the unbatched plan
// reads all of them. That is the shape the optimization exists for, and the question is where
// the constraint's own cost overtakes the scan it saves -- because maxJoinKeyBatch currently
// answers that with a constant, which cannot depend on how much of the right side is excluded.
func BenchmarkJoinKeyBatch(b *testing.B) {
	// One store for every sub-benchmark: a large right side is the whole point, and
	// rebuilding it per case would add variance without adding information.
	const clusters = 8192
	fix := newJoinBenchFixture(b, clusters)
	defer fix.cleanup()

	for _, k := range []int{32, 128, 256, 400, 511} {
		sql := fmt.Sprintf(
			`SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId WHERE h.ClusterId <= %d`, k)
		st, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
		for _, batching := range []bool{true, false} {
			name := "batched"
			if !batching {
				name = "unbatched"
			}
			// Agreement first: suppressing the constraint must not change the answer, only
			// the work. If it does, the constraint is not a pure narrowing and it is
			// unsound rather than merely mispriced.
			got, rerr := runBenchJoinBatch(st, fix.fetchWhole, batching)
			if rerr != nil {
				b.Fatal(rerr)
			}
			if got != 2*k {
				b.Fatalf("keys=%d %s: joined %d rows, expected %d", k, name, got, 2*k)
			}
			b.Run(fmt.Sprintf("keys=%d/%s", k, name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := runBenchJoinBatch(st, fix.fetchWhole, batching); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// runBenchJoinBatch is runBenchJoin with the key-batch constraint made optional.
func runBenchJoinBatch(st *Statement, fetch joinFetch, batching bool) (int, error) {
	p, err := planJoin(st)
	if err != nil {
		return 0, err
	}
	leftAds, err := fetch(st, p.sides[0], 0)
	if err != nil {
		return 0, err
	}
	right := p.sides[1]
	if batching {
		if batch := p.steps[0].keyBatchConstraint(p.wrapFirst(leftAds)); batch != "" {
			right.where = andText(right.where, batch)
		}
	}
	rightAds, err := fetch(st, right, 1)
	if err != nil {
		return 0, err
	}
	rows, err := joinTwoSides(p, leftAds, rightAds)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// The k-sweep above finds break-even near 500 keys, which is suspiciously close to
// maxJoinKeyBatch. That could mean the constant is well chosen, or it could mean it happens
// to be right for one right-table size -- and those have opposite consequences, so the
// difference is worth measuring rather than assuming.
//
// Break-even is where the constraint's own cost (which grows with the KEY COUNT) overtakes
// the scan it avoids (which grows with the RIGHT TABLE's size). So it cannot be a constant
// in the key count alone. This holds the key count fixed and grows the right side: if 512 is
// principled, the batched arm should win by about the same margin everywhere; if it is a
// coincidence of scale, the margin should move with the table.
func BenchmarkJoinKeyBatchByRightSize(b *testing.B) {
	const keys = 400
	sql := fmt.Sprintf(
		`SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId WHERE h.ClusterId <= %d`, keys)
	st, err := Parse(sql)
	if err != nil {
		b.Fatal(err)
	}
	// Each cluster contributes two epoch rows, so the right table is 2*clusters rows while
	// the left side stays pinned at the same 400 keys.
	for _, clusters := range []int{512, 2048, 8192, 32768} {
		fix := newJoinBenchFixture(b, clusters)
		for _, batching := range []bool{true, false} {
			name := "batched"
			if !batching {
				name = "unbatched"
			}
			got, rerr := runBenchJoinBatch(st, fix.fetchWhole, batching)
			if rerr != nil {
				b.Fatal(rerr)
			}
			if got != 2*keys {
				b.Fatalf("right=%d %s: joined %d rows, expected %d", 2*clusters, name, got, 2*keys)
			}
			b.Run(fmt.Sprintf("right=%d/%s", 2*clusters, name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := runBenchJoinBatch(st, fix.fetchWhole, batching); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
		fix.cleanup()
	}
}

// Does the join opcode actually collect the wire cost BenchmarkJoinWireCost measured?
//
// That benchmark priced the gap between joining over a connection and joining in-process:
// a flat ~2.2x on a narrow projection and ~4.2x unprojected. The prediction was that moving
// the pairing into the store collects it, because only the joined rows then cross. This runs
// the SAME statement through two executors over the SAME store, differing only in whether the
// handshake advertises the join -- so the difference is where the pairing happened and
// nothing else.
//
// Both arms go through ExecString, not an internal helper: the question is what a query
// costs, and the planning, rendering and ordering around the pairing are part of that.
func BenchmarkJoinServerSide(b *testing.B) {
	shapes := []struct{ name, sql string }{
		{"narrow", `SELECT h.ClusterId, h.Owner, e.RunInstanceId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`},
		{"star", `SELECT * FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`},
	}
	for _, n := range []int{400, 2048, 8192} {
		fix := newJoinBenchFixture(b, n)
		for _, shape := range shapes {
			arms := []struct {
				name string
				exec *Executor
			}{
				{"client-side", fix.exec},
				{"in-store", fix.srvExec},
			}
			if benchReverseJoinArms {
				arms[0], arms[1] = arms[1], arms[0]
			}
			// Agreement before timing: two arms that answer differently cannot be compared,
			// and a join that quietly returned fewer rows would otherwise look like a fast one.
			want := -1
			for _, arm := range arms {
				res, err := arm.exec.ExecString(shape.sql)
				if err != nil {
					b.Fatalf("n=%d %s/%s: %v", n, shape.name, arm.name, err)
				}
				if want == -1 {
					want = len(res.Rows)
				} else if len(res.Rows) != want {
					b.Fatalf("n=%d %s: %s returned %d rows, the other %d",
						n, shape.name, arm.name, len(res.Rows), want)
				}
			}
			if want != 2*n {
				b.Fatalf("n=%d %s: joined %d rows, expected %d", n, shape.name, want, 2*n)
			}
			for _, arm := range arms {
				b.Run(fmt.Sprintf("n=%d/%s/%s", n, shape.name, arm.name), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						if _, err := arm.exec.ExecString(shape.sql); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
		fix.cleanup()
	}
}

// Does a SELECTIVE join win, as BenchmarkJoinServerSide's negative result left open?
//
// That benchmark measured an EXPANDING join -- two right rows per left row -- where the
// composite output repeats the left ad per output row and the store moved more data than the
// client did. The reasoning it was built on applies to the other regime: a join whose output
// is far smaller than its inputs. Here only every fiftieth cluster has an epoch row, so the
// store streams n/50 joined rows where the client must fetch all n of one side and all n/50
// of the other before pairing anything.
//
// Asserting that without measuring it would be the same mistake twice.
func BenchmarkJoinServerSideSelective(b *testing.B) {
	const sparsity = 50
	const sql = `SELECT h.ClusterId, h.Owner, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`
	for _, n := range []int{2048, 8192} {
		fix := newSelectiveJoinFixture(b, n, sparsity)
		arms := []struct {
			name string
			exec *Executor
		}{
			{"client-side", fix.exec},
			{"in-store", fix.srvExec},
		}
		if benchReverseJoinArms {
			arms[0], arms[1] = arms[1], arms[0]
		}
		want := -1
		for _, arm := range arms {
			res, err := arm.exec.ExecString(sql)
			if err != nil {
				b.Fatalf("n=%d/%s: %v", n, arm.name, err)
			}
			if want == -1 {
				want = len(res.Rows)
			} else if len(res.Rows) != want {
				b.Fatalf("n=%d: %s returned %d rows, the other %d", n, arm.name, len(res.Rows), want)
			}
		}
		if want != n/sparsity {
			b.Fatalf("n=%d: joined %d rows, expected %d; the fixture is not selective", n, want, n/sparsity)
		}
		for _, arm := range arms {
			b.Run(fmt.Sprintf("n=%d/rows=%d/%s", n, want, arm.name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := arm.exec.ExecString(sql); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
		fix.cleanup()
	}
}

// How expensive is a join with NO equality to hash on?
//
// It is the one shape with no measurement behind it. Everything measured so far has been an
// equi join -- the hash path -- while an equality-free ON falls to a different loop
// entirely: every accumulated row against every row of the next table, tuple at a time,
// with both sides materialized. That loop is the fallback for every non-equi join and every
// CROSS JOIN, and it is also where block-level zone skipping would have to live, because it
// is the only loop that ever holds a (left block, right block) pair.
//
// So before deciding whether to block it, this measures what it spends, against the hash
// path over the same data. The arms are:
//
//   - equi: hashed. Examines n pairs.
//   - band: a range join, which is what a real non-equi join usually is. Examines n*2n
//     pairs and keeps a handful per left row.
//   - expanding: nearly every pair survives, so the loop pays for output as well as for
//     candidates. Capped at a smaller n, because n*2n surviving pairs reaches the
//     output-row limit before the pair limit.
//
// The band arm's predicate is a band and not an impossibility on purpose: an arm that keeps
// zero rows measures only rejection, and reads as "selective" while being degenerate.
func BenchmarkJoinNonEqui(b *testing.B) {
	shapes := []struct {
		name, sql string
		maxN      int
	}{
		{"equi", `SELECT h.ClusterId, e.ExitCode FROM history h
			JOIN epoch e ON h.ClusterId == e.ClusterId`, 1 << 30},
		{"band", `SELECT h.ClusterId, e.ExitCode FROM history h
			JOIN epoch e ON h.ClusterId > e.ClusterId && h.ClusterId < e.ClusterId + 5`, 1 << 30},
		{"expanding", `SELECT h.ClusterId, e.ExitCode FROM history h
			JOIN epoch e ON h.ClusterId > e.ClusterId - 100000`, 256},
	}
	for _, n := range []int{256, 1024} {
		fix := newJoinBenchFixture(b, n)
		for _, shape := range shapes {
			if n > shape.maxN {
				continue
			}
			res, err := fix.exec.ExecString(shape.sql)
			if err != nil {
				b.Fatalf("n=%d %s: %v", n, shape.name, err)
			}
			rows := len(res.Rows)
			if rows == 0 {
				b.Fatalf("n=%d %s kept no rows; the arm measures only rejection", n, shape.name)
			}
			b.Run(fmt.Sprintf("n=%d/%s", n, shape.name), func(b *testing.B) {
				// Reported so a reader can see what each arm was actually doing: the equi
				// arm examines n pairs where the others examine n*2n.
				b.ReportMetric(float64(rows), "rows")
				for i := 0; i < b.N; i++ {
					if _, err := fix.exec.ExecString(shape.sql); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
		fix.cleanup()
	}
}

// WHERE THE STORE-SIDE JOIN STOPS BEING WORTH IT, IN TIME.
//
// ExecConfig.ServerSideJoin is off on the strength of one measurement at two right rows per
// left row: the store moved 1.33x more data and ran 1.4x slower end to end. The BYTES half
// has since been settled in classad and is not re-derived here. With k right rows per left
// row and per-side projected widths l and r the server ships |L|*k*(l+r) -- the left ad once
// per output row -- against the client's |L|*(l + k*r), a ratio below 1 exactly when k < 1.
// The crossover is at k=1, independent of l and r, and no encoding moves it.
//
// The TIME half was open, and it is the half that needed this package. The classad
// measurement's client arm hand-rolls an int-keyed hash join; a fetch-only arm priced the
// gap at 16% of that arm at k=0.25 and 37% at k=8, so a general join two or three times more
// expensive than a map would flip the comparison at most k. The join here IS that general
// one: a ClassAd hash join over composite ads, behind the shipped planner, the shipped
// key-batch decision and the shipped rendering.
//
// SO BOTH ARMS ARE ExecString. The question is what a statement costs, and the planning,
// the pairing, the ordering and the rendering are all part of that; an arm that stopped at
// the rows would be comparing a finished answer with an unfinished one. The two executors
// reach the SAME store over the same kind of connection and differ in one thing: whether the
// handshake advertises the join.
//
// TWO LEFT SIZES, because they are two PLANS and not two sizes. Key batching folds the left
// side's keys into the right scan and gives up past maxJoinKeyBatch (512) distinct keys, so
// at 400 left rows the client arm reads a slice of the right table and at 2048 it reads the
// whole thing. Measuring only above the cutoff would credit the store with a narrowing the
// client arm was denied.
//
// Allocations are the number to read first. This machine runs at high load and its timings
// move between runs; the allocation counts do not.

// newExpansionJoinFixture builds `left` history rows and k epoch rows per history row, with
// k allowed to be fractional: k=0.25 means one epoch row for every four jobs, which is the
// SELECTIVE direction -- the one the store is supposed to win.
//
// ops records what the server was actually asked to run, so the server arm can be required
// to have used the wire reply rather than quietly measuring the text one.
func newExpansionJoinFixture(b testing.TB, left int, k float64, archive bool) (*joinBenchFixture, *opRecorder, int) {
	b.Helper()
	cat, err := db.OpenCatalog(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	// addHistory/addEpoch hide which kind of table this is; everything below is the same.
	var addHistory, addEpoch func(ad *classad.ClassAd, key string)
	if archive {
		cfg := func(zone []string) db.ArchiveConfig {
			return db.ArchiveConfig{SegmentSize: 1 << 16, ValueAttrs: []string{"ClusterId"}, ZoneAttrs: zone}
		}
		hist, herr := cat.CreateArchiveTable("history", cfg([]string{"CompletionDate"}))
		if herr != nil {
			b.Fatal(herr)
		}
		epoch, eerr := cat.CreateArchiveTable("epoch", cfg(nil))
		if eerr != nil {
			b.Fatal(eerr)
		}
		addHistory = func(ad *classad.ClassAd, _ string) {
			if aerr := hist.Append(ad); aerr != nil {
				b.Fatal(aerr)
			}
		}
		addEpoch = func(ad *classad.ClassAd, _ string) {
			if aerr := epoch.Append(ad); aerr != nil {
				b.Fatal(aerr)
			}
		}
	} else {
		hist, herr := cat.CreateTable("history")
		if herr != nil {
			b.Fatal(herr)
		}
		epoch, eerr := cat.CreateTable("epoch")
		if eerr != nil {
			b.Fatal(eerr)
		}
		addHistory = func(ad *classad.ClassAd, key string) {
			if werr := hist.Put(key, ad); werr != nil {
				b.Fatal(werr)
			}
		}
		addEpoch = func(ad *classad.ClassAd, key string) {
			if werr := epoch.Put(key, ad); werr != nil {
				b.Fatal(werr)
			}
		}
	}
	rows := 0
	for i := 1; i <= left; i++ {
		addHistory(benchHistoryAd(i), fmt.Sprint(i))
		n := int(float64(i)*k) - int(float64(i-1)*k)
		for inst := 0; inst < n; inst++ {
			addEpoch(benchEpochAd(i, inst), fmt.Sprintf("%d.%d", i, inst))
			rows++
		}
	}
	s := dbrpc.NewServerCatalog(cat)
	ops := &opRecorder{}
	dial := func(advertise bool) *dbrpc.Client {
		cp, sp := net.Pipe()
		go func() {
			_ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{
				Privileged: true,
				QueryLog:   func(q dbrpc.QueryLog) { ops.addLog(q) },
			})
		}()
		conn := dbrpc.NewStreamConn(cp)
		if advertise {
			return dbrpc.NewClient(conn, dbrpc.WithPeerSessionAttrs(s.SessionAttrs()))
		}
		return dbrpc.NewClient(conn)
	}
	c, sc := dial(false), dial(true)
	return &joinBenchFixture{
		cat:     cat,
		exec:    NewExecutor(c, ExecConfig{}),
		srvExec: NewExecutor(sc, ExecConfig{ServerSideJoin: true}),
		cleanup: func() { c.Close(); sc.Close(); s.Close(); cat.Close() },
	}, ops, rows
}

// expansionJoinSQL is a narrow projection: the shape the original decision was taken on, and
// the one where the composite row's repetition of the left ad is least diluted by width.
const expansionJoinSQL = `SELECT h.ClusterId, h.Owner, e.RunInstanceId, e.ExitCode ` +
	`FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`

// BenchmarkJoinExpansionCrossover is the CONTROLLED comparison, over mutable tables.
//
// Mutable rather than archive on purpose, and this is the confound the archive sweep below
// exists to name. A read of a mutable table takes the wire row stream; an archive has no wire
// row stream (wireEligible excludes it), so the client arm's two side reads come back as
// rendered text while the store-side arm's reply is wire rows either way. Over archives the
// two arms therefore differ in the ENCODING as well as in where the pairing happened, and a
// comparison that moves two things cannot attribute the difference to one. Over mutable
// tables both arms carry wire rows and the only remaining difference is the pairing.
func BenchmarkJoinExpansionCrossover(b *testing.B) {
	benchJoinExpansion(b, false, []int{400, 2048}, []float64{0.25, 0.5, 1, 2, 4, 8})
}

// BenchmarkJoinExpansionArchive is the same sweep over ARCHIVE tables, which is what a
// history-to-epoch join actually reads. It measures the shipped paths as they stand, so its
// numbers include the encoding gap named above rather than isolating the pairing: the client
// arm pays a render and a parse per row that the store-side arm does not. Read it as what the
// two code paths cost today, not as what the pairing costs.
func BenchmarkJoinExpansionArchive(b *testing.B) {
	benchJoinExpansion(b, true, []int{400, 2048}, []float64{0.25, 0.5, 1, 2, 4, 8})
}

func benchJoinExpansion(b *testing.B, archive bool, lefts []int, ks []float64) {
	sql := expansionJoinSQL
	for _, left := range lefts {
		for _, k := range ks {
			fix, ops, want := newExpansionJoinFixture(b, left, k, archive)

			// Agreement before timing, and evidence of which path each arm took. Two arms
			// that answer differently cannot be compared, and a server arm that fell back
			// to the text reply would be measured as the wire one.
			ops.reset()
			sres, err := fix.srvExec.ExecString(sql)
			if err != nil {
				b.Fatalf("left=%d k=%g in-store: %v", left, k, err)
			}
			cres, err := fix.exec.ExecString(sql)
			if err != nil {
				b.Fatalf("left=%d k=%g client-side: %v", left, k, err)
			}
			if len(sres.Rows) != want || len(cres.Rows) != want {
				b.Fatalf("left=%d k=%g: in-store %d rows, client-side %d, expected %d",
					left, k, len(sres.Rows), len(cres.Rows), want)
			}
			// The same rows, not merely the same number of them: an arm that returned the
			// right count of the wrong rows would otherwise be timed as the fast one.
			sortRowsForCompare(sres)
			sortRowsForCompare(cres)
			if render(sres) != render(cres) {
				b.Fatalf("left=%d k=%g: the two arms returned different rows", left, k)
			}
			if got := ops.waitOpsB(b, 1); got[0] != "JoinWire" {
				b.Fatalf("left=%d k=%g: the in-store arm's first op was %q, not JoinWire -- it "+
					"fell back and this would measure the text reply: %v", left, k, got[0], got)
			}

			for round := 0; round < 2; round++ {
				arms := []struct {
					name string
					exec *Executor
				}{{"in-store", fix.srvExec}, {"client-side", fix.exec}}
				// A Go benchmark's first arm warms the page cache and the allocator for the
				// second, so the order is flipped between rounds and the two readings of
				// each arm bracket that effect rather than hiding it.
				if round%2 == 1 {
					arms[0], arms[1] = arms[1], arms[0]
				}
				for _, arm := range arms {
					b.Run(fmt.Sprintf("left=%d/k=%g/rows=%d/r%d/%s", left, k, want, round, arm.name),
						func(b *testing.B) {
							b.ReportAllocs()
							for b.Loop() {
								if _, err := arm.exec.ExecString(sql); err != nil {
									b.Fatal(err)
								}
							}
						})
				}
			}
			fix.cleanup()
		}
	}
}

// sortRowsForCompare puts a result's rows in a canonical order, since neither arm promises
// one without ORDER BY and the agreement check is about WHICH rows, not their sequence.
func sortRowsForCompare(r *Result) {
	sort.Slice(r.Rows, func(i, j int) bool {
		return fmt.Sprint(r.Rows[i]) < fmt.Sprint(r.Rows[j])
	})
}
