// End-to-end query benchmarks. Each case runs a whole statement against an in-process server
// over a pipe, which is the only way to see where a query's time actually goes: the store
// scan, the wire, and the client-side reduction are all in play, and the shapes that fall back
// to a client-side reduction cost orders of magnitude more than the ones the server can
// answer itself. A microbenchmark of any one layer hides that.
//
// The fixture is deliberately large (20k rows), so run with an explicit iteration count:
//
//	go test -run XXX -bench BenchmarkQueries -benchtime 10x ./repl/
package repl

import (
	"fmt"
	"net"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// benchExec builds an in-process server holding n job-like ads and a client Executor over a
// pipe, so a benchmark measures the whole path a real query takes: parse, plan, wire, the
// server-side scan or aggregate, and rendering the result.
func benchExec(b *testing.B, n int) (*Executor, func()) {
	b.Helper()
	cat, err := db.OpenCatalog("")
	if err != nil {
		b.Fatal(err)
	}
	t, err := cat.CreateTable("jobs")
	if err != nil {
		b.Fatal(err)
	}
	owners := []string{"alice", "bob", "carol", "dave", "eve"}
	hosts := []string{"h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8"}
	for i := 0; i < n; i++ {
		ad := fmt.Sprintf(`[ Key = "%d.0"; ClusterId = %d; ProcId = 0; Owner = "%s"; Host = "%s"; `+
			`JobStatus = %d; RequestMemory = %d; RequestCpus = %d; QDate = %d; `+
			`Cmd = "/bin/sleep"; Args = "%d"; Iwd = "/home/%s/run" ]`,
			i, i, owners[i%len(owners)], hosts[i%len(hosts)], (i%5)+1,
			((i%16)+1)*512, (i%8)+1, 1700000000+i, i, owners[i%len(owners)])
		parsed, perr := classad.Parse(ad)
		if perr != nil {
			b.Fatal(perr)
		}
		if err := t.Put(fmt.Sprintf("%d.0", i), parsed); err != nil {
			b.Fatal(err)
		}
	}
	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	return NewExecutor(c, ExecConfig{}), func() { c.Close(); s.Close(); cat.Close() }
}

// benchQueries are the shapes a dashboard or an operator actually runs.
var benchQueries = []struct{ name, sql string }{
	{"count", `SELECT COUNT(*) FROM jobs`},
	{"count_where", `SELECT COUNT(*) FROM jobs WHERE JobStatus == 2`},
	{"group_count", `SELECT Owner, COUNT(*) AS n FROM jobs GROUP BY Owner`},
	{"group_multi_agg", `SELECT Owner, COUNT(*) AS n, SUM(RequestCpus) AS c, AVG(RequestMemory) AS m FROM jobs GROUP BY Owner`},
	{"pivot_filter", `SELECT Owner, COUNT(*) AS total, ` +
		`COUNT(*) FILTER (WHERE JobStatus == 2) AS running, ` +
		`COUNT(*) FILTER (WHERE JobStatus == 1) AS idle FROM jobs GROUP BY Owner`},
	{"count_distinct", `SELECT COUNT(DISTINCT Host) AS hosts FROM jobs`},
	{"having", `SELECT Owner, COUNT(*) AS n FROM jobs GROUP BY Owner HAVING COUNT(*) > 10`},
	{"group_expr", `SELECT CASE WHEN RequestMemory > 4096 THEN 'big' ELSE 'small' END AS sz, ` +
		`COUNT(*) AS n FROM jobs GROUP BY sz`},
	{"project_rows", `SELECT ClusterId, Owner, JobStatus FROM jobs WHERE JobStatus == 2 LIMIT 200`},
	{"select_star", `SELECT * FROM jobs WHERE JobStatus == 2 LIMIT 200`},
	{"expr_column", `SELECT Owner, RequestMemory / 1024 AS gb FROM jobs WHERE JobStatus == 2 LIMIT 200`},
	{"window_topn", `SELECT Owner, ClusterId, ` +
		`ROW_NUMBER() OVER (PARTITION BY Owner ORDER BY QDate DESC) AS n FROM jobs QUALIFY n <= 5`},
}

func BenchmarkQueries(b *testing.B) {
	const n = 20000
	e, cleanup := benchExec(b, n)
	defer cleanup()
	for _, q := range benchQueries {
		b.Run(q.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := e.ExecString(q.sql); err != nil {
					b.Fatalf("%s: %v", q.sql, err)
				}
			}
		})
	}
}

// BenchmarkParse isolates the front end: how much of a query's cost is parsing and planning
// before anything touches the store.
func BenchmarkParse(b *testing.B) {
	for _, q := range benchQueries {
		b.Run(q.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := Parse(q.sql); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
