package repl

import (
	"fmt"
	"net"
	"os"
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
	cat     *db.Catalog
	exec    *Executor
	cleanup func()
}

// newJoinBenchFixture builds a history archive of n rows, one per cluster, and an epoch
// archive with two rows per cluster. Both are value-indexed on ClusterId, which is what
// gives the key-batch membership constraint an index to probe rather than a predicate to
// evaluate per record.
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

	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	return &joinBenchFixture{
		cat:     cat,
		exec:    NewExecutor(c, ExecConfig{}),
		cleanup: func() { c.Close(); s.Close(); cat.Close() },
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
func runBenchJoin(st *Statement, fetch joinFetch) (int, error) {
	p, err := planJoin(st)
	if err != nil {
		return 0, err
	}
	leftAds, err := fetch(st, p.left, 0)
	if err != nil {
		return 0, err
	}
	right := p.right
	if batch := p.keyBatchConstraint(leftAds); batch != "" {
		right.where = andText(right.where, batch)
	}
	rightAds, err := fetch(st, right, 1)
	if err != nil {
		return 0, err
	}
	rows, err := p.join(leftAds, rightAds)
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
				got, rerr := runBenchJoin(st, arm.fetch)
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
						if _, rerr := runBenchJoin(st, arm.fetch); rerr != nil {
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
	leftAds, err := fetch(st, p.left, 0)
	if err != nil {
		return 0, err
	}
	right := p.right
	if batching {
		if batch := p.keyBatchConstraint(leftAds); batch != "" {
			right.where = andText(right.where, batch)
		}
	}
	rightAds, err := fetch(st, right, 1)
	if err != nil {
		return 0, err
	}
	rows, err := p.join(leftAds, rightAds)
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
