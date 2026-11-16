package repl

import (
	"fmt"
	"net"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// WHERE THE TWO ARMS CROSS, which is the number defaultMaxInListValues is standing in for.
//
// The membership arm reads the subquery's key column and inlines it as `member(k, {...})`.
// That is the cheaper answer while the set is small -- one narrow read plus one index probe,
// against hashing a whole side -- and stops being so as the set grows, because the probe's
// candidate estimate runs over and the outer read degrades to a full scan carrying a very
// large constraint.
//
// The sweep varies the one quantity that decides it: how many DISTINCT keys the subquery
// matches. The outer table is held fixed, because the model says the crossover is a property
// of the set and not of the table it filters -- and a sweep that moved both could not say so.
//
// MEASURED, 20,000 outer rows, every row matching, Apple M2 Pro, in-process pipe:
//
//	keys     membership   store (wire)   store (text)
//	    16      17.1 ms       17.2 ms        52.6 ms
//	    64      17.2 ms       17.2 ms        50.1 ms
//	   256      17.2 ms       17.2 ms        50.1 ms
//	  1024      18.9 ms       18.4 ms        57.6 ms
//	  4096      21.9 ms       19.3 ms        53.1 ms
//	 16384      35.9 ms       21.5 ms        55.7 ms
//
// Two things to read off it.
//
// THE CROSSOVER IS AROUND A THOUSAND KEYS, which is where defaultMaxInListValues already
// sits. Below it the two arms are indistinguishable -- both pay for the same outer read, and
// the set is too small to matter. Above it the membership arm degrades with the set (2.1x
// from 16 to 16,384 keys) while the store arm is nearly flat (1.25x), so the store wins by
// 1.67x at the top of the sweep and by more past it.
//
// THE WIRE REPLY IS WHAT MAKES THE STORE ARM WORTH TAKING AT ALL. The last column is the same
// store arm on the text reply, which is what a server without the semi-join wire fix serves:
// 2.4x to 3.1x SLOWER than the membership arm at every size, because every joined row is
// rendered at one end and parsed at the other. Rendering 20,000 rows costs about 35 ms, which
// is more than the whole query. A store arm on the text path is a pessimization, not a
// fallback worth preferring.
//
// Both of the sweep's biases favour the membership arm -- an in-process pipe charges nothing
// for bytes, and every outer row matches, which is the most the store arm can ever ship -- so
// the crossover on a real link is at or below what is measured here.
//
// Run: go test ./repl/ -run XXX -bench BenchmarkSubqueryArms -benchtime 5x
func benchSubqueryPair(b *testing.B, outerRows, innerKeys int) (store, member *Executor, cleanup func()) {
	b.Helper()
	cat, err := db.OpenCatalog(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	jobs, err := cat.CreateTable("jobs")
	if err != nil {
		b.Fatal(err)
	}
	users, err := cat.CreateTable("users")
	if err != nil {
		b.Fatal(err)
	}
	// Each job's Owner is one of innerKeys names, so the subquery's set is exactly that
	// many and every job is a candidate. A job ad is given a few more attributes so a row
	// costs something to carry, as a real one does.
	for i := 0; i < outerRows; i++ {
		ad, perr := classad.Parse(fmt.Sprintf(
			`[ ClusterId = %d; Owner = "user%d"; JobStatus = %d; RequestCpus = %d; `+
				`Cmd = "/usr/bin/thing%d"; QDate = %d ]`,
			i, i%innerKeys, i%6, 1+i%8, i%13, 1700000000+i))
		if perr != nil {
			b.Fatal(perr)
		}
		if werr := jobs.Put(fmt.Sprint(i), ad); werr != nil {
			b.Fatal(werr)
		}
	}
	for i := 0; i < innerKeys; i++ {
		ad, perr := classad.Parse(fmt.Sprintf(`[ Owner = "user%d"; Priority = %d ]`, i, 500+i))
		if perr != nil {
			b.Fatal(perr)
		}
		if werr := users.Put(fmt.Sprint(i), ad); werr != nil {
			b.Fatal(werr)
		}
	}
	s := dbrpc.NewServerCatalog(cat)
	store = benchExecutor(b, s, s.SessionAttrs())
	member = benchExecutor(b, s, nil)
	member.maxInList = 1 << 20 // the sweep measures the cost, not the cap
	store.maxInList = 1 << 20
	return store, member, func() { s.Close(); cat.Close() }
}

func benchExecutor(b *testing.B, s *dbrpc.Server, attrs map[string]string) *Executor {
	b.Helper()
	cp, sp := net.Pipe()
	go func() {
		_ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true})
	}()
	conn := dbrpc.NewStreamConn(cp)
	if attrs == nil {
		return NewExecutor(dbrpc.NewClient(conn), ExecConfig{})
	}
	return NewExecutor(dbrpc.NewClient(conn, dbrpc.WithPeerSessionAttrs(attrs)),
		ExecConfig{ServerSideJoin: true})
}

func BenchmarkSubqueryArms(b *testing.B) {
	const outerRows = 20000
	query := `SELECT ClusterId, Owner FROM jobs WHERE Owner IN (SELECT Owner FROM users)`
	for _, keys := range []int{16, 64, 256, 1024, 4096, 16384} {
		if keys > outerRows {
			continue
		}
		store, member, cleanup := benchSubqueryPair(b, outerRows, keys)
		// The arms are run in BOTH orders across the sweep, because a benchmark that
		// always runs one first charges it with whatever the other warmed.
		arms := []struct {
			name string
			e    *Executor
		}{{"store", store}, {"membership", member}}
		if keys%2 == 0 {
			arms[0], arms[1] = arms[1], arms[0]
		}
		for _, arm := range arms {
			b.Run(fmt.Sprintf("keys=%d/%s", keys, arm.name), func(b *testing.B) {
				// Prove the arm under test is the one that ran, once, outside the
				// timed loop: a silent fallback would benchmark one arm twice.
				_, got := subqueryRowsAndArmB(b, arm.e, query)
				if want := arm.name; !hasArmPrefix(got, want) {
					b.Fatalf("arm %q ran as %q", want, got)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					st, perr := Parse(query)
					if perr != nil {
						b.Fatal(perr)
					}
					if _, eerr := arm.e.Exec(st); eerr != nil {
						b.Fatal(eerr)
					}
				}
			})
		}
		cleanup()
	}
}

func hasArmPrefix(arm, want string) bool {
	switch want {
	case "store":
		return len(arm) >= 10 && arm[:10] == "store-side"
	default:
		return len(arm) >= 10 && arm[:10] == "membership"
	}
}

// subqueryRowsAndArmB is subqueryRowsAndArm for a benchmark.
func subqueryRowsAndArmB(b *testing.B, e *Executor, query string) (int, string) {
	b.Helper()
	st, err := Parse(query)
	if err != nil {
		b.Fatal(err)
	}
	tr := &explainTrace{}
	e.explain = tr
	defer func() { e.explain = nil }()
	r, err := e.Exec(st)
	if err != nil {
		b.Fatal(err)
	}
	return len(r.Rows), tr.subqueryArm
}
