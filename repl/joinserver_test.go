package repl

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// The server-side join must return exactly what joining here returns.
//
// It is a change of WHERE the pairing happens, not a second implementation of the statement:
// the rows that come back are the same composite ads, and everything after -- grouping,
// DISTINCT, ordering, the limit, rendering -- is the same code. So the test is differential.
// One executor is given the handshake and uses the opcode; the other is not and joins here.
// Every query must agree.
//
// Comparing against the client-side path rather than against expected literals is deliberate:
// the client-side join is already tested against literals, and an oracle that is the other
// implementation cannot drift from the thing under test by accident.
func newJoinPair(t *testing.T) (server, client *Executor, serverOps, clientOps *opRecorder, cleanup func()) {
	t.Helper()
	s, closeCat := newJoinCatalog(t)
	mk := func(advertise bool, ops *opRecorder) *Executor {
		attrs := s.SessionAttrs()
		if !advertise {
			attrs = nil
		}
		return joinExecutor(t, s, attrs, true, ops)
	}
	serverOps, clientOps = &opRecorder{}, &opRecorder{}
	return mk(true, serverOps), mk(false, clientOps), serverOps, clientOps, closeCat
}

// newJoinCatalog builds the two-table fixture and puts a server in front of it.
func newJoinCatalog(t *testing.T) (*dbrpc.Server, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	hist, err := cat.CreateTable("hist")
	if err != nil {
		t.Fatal(err)
	}
	put := func(d *db.DB, key, text string) {
		ad, perr := classad.Parse(text)
		if perr != nil {
			t.Fatal(perr)
		}
		if werr := d.Put(key, ad); werr != nil {
			t.Fatal(werr)
		}
	}
	// One job pairs with nothing, one history row pairs with nothing, one job has two
	// history rows, and one row on each side has no join key at all.
	put(jobs, "1", `[ ClusterId = 1; Owner = "alice"; Cpus = 2 ]`)
	put(jobs, "2", `[ ClusterId = 2; Owner = "bob";   Cpus = 4 ]`)
	put(jobs, "3", `[ ClusterId = 3; Owner = "alice"; Cpus = 8 ]`)
	put(jobs, "4", `[ Owner = "carol"; Cpus = 1 ]`)
	put(hist, "a", `[ ClusterId = 1; ExitCode = 0 ]`)
	put(hist, "b", `[ ClusterId = 2; ExitCode = 1 ]`)
	put(hist, "c", `[ ClusterId = 2; ExitCode = 7 ]`)
	put(hist, "d", `[ ClusterId = 9; ExitCode = 0 ]`)
	put(hist, "e", `[ ExitCode = 42 ]`)

	s := dbrpc.NewServerCatalog(cat)
	return s, func() { s.Close(); cat.Close() }
}

// joinExecutor dials one connection to s and wraps it in an Executor.
//
// attrs is what the application carries over CEDAR; nil is a peer that advertised nothing,
// which the client must read as "cannot join" rather than as "unknown". privileged is the
// server's view of the connection, which decides both what it will show and -- for a join
// asked for as whole ads -- whether the wire-form reply is available at all.
func joinExecutor(t *testing.T, s *dbrpc.Server, attrs map[string]string, privileged bool, ops *opRecorder) *Executor {
	t.Helper()
	cp, sp := net.Pipe()
	go func() {
		_ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{
			Privileged: privileged,
			QueryLog:   func(q dbrpc.QueryLog) { ops.add(q.Op) },
		})
	}()
	conn := dbrpc.NewStreamConn(cp)
	if attrs == nil {
		return NewExecutor(dbrpc.NewClient(conn), ExecConfig{})
	}
	return NewExecutor(dbrpc.NewClient(conn, dbrpc.WithPeerSessionAttrs(attrs)),
		ExecConfig{ServerSideJoin: true})
}

// opRecorder notes which ops a server was actually asked to run, which is the only direct
// evidence of which path a statement took. EXPLAIN says what WILL happen; this says what did.
type opRecorder struct {
	mu  sync.Mutex
	ops []string
}

func (r *opRecorder) add(op string) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

func (r *opRecorder) saw(op string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, o := range r.ops {
		if o == op {
			return true
		}
	}
	return false
}

func (r *opRecorder) reset() {
	r.mu.Lock()
	r.ops = nil
	r.mu.Unlock()
}

func (r *opRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ops...)
}

// waitOps blocks until the server has logged n ops, and returns them.
//
// A server logs an op as its handler returns, which is after it has written the reply -- so
// the client's call can return first and the log read straight after it is empty. Reading it
// without waiting is a test that passes on timing. Waiting for a KNOWN count rather than for
// a quiet interval is what keeps the assertion exact: the statements below issue a fixed
// number of ops, and one more would be a second request this test is not expecting.
func (r *opRecorder) waitOps(t *testing.T, n int) []string {
	t.Helper()
	return r.waitOpsB(t, n)
}

// waitOpsB is waitOps for a benchmark, which has no *testing.T.
func (r *opRecorder) waitOpsB(t testing.TB, n int) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got := r.all(); len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited for %d server op(s), saw %v", n, r.all())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestServerSideJoinAgreesWithTheClientSideJoin(t *testing.T) {
	server, client, _, _, cleanup := newJoinPair(t)
	defer cleanup()

	queries := []string{
		`SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`,
		`SELECT j.Owner, h.ExitCode FROM jobs j LEFT JOIN hist h ON j.ClusterId == h.ClusterId`,
		`SELECT j.Owner, h.ExitCode FROM jobs j CROSS JOIN hist h`,
		`SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId < h.ClusterId`,
		`SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId WHERE h.ExitCode == 0`,
		`SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId WHERE j.Owner == "bob"`,
		`SELECT j.Owner, h.ExitCode FROM jobs j LEFT JOIN hist h ON j.ClusterId == h.ClusterId WHERE h.ExitCode == 0`,
		`SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId ORDER BY h.ExitCode DESC`,
		`SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId ORDER BY h.ExitCode LIMIT 2`,
		`SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId LIMIT 2`,
		`SELECT DISTINCT j.Owner FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`,
		`SELECT j.Owner, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId GROUP BY j.Owner`,
		`SELECT * FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`,
		`SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId && h.ExitCode > 0`,
		// A residual WHERE: it names both sides, so it can be pushed to neither and has to be
		// evaluated against the pair. The matrix otherwise only has filters that reach a side.
		//
		// Note what this does NOT reach. The store used to apply such a filter to the
		// PROJECTED row, so a filter over an attribute the projection left out silently
		// matched nothing. The planner here scans WHERE into both side projections, so
		// j.Cpus and h.ExitCode cross even though the SELECT names neither -- which is why
		// these two queries answered correctly before the store was fixed as well as after.
		`SELECT j.Owner FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId WHERE j.Cpus > h.ExitCode`,
		`SELECT j.Owner FROM jobs j LEFT JOIN hist h ON j.ClusterId == h.ClusterId WHERE j.Cpus > h.ExitCode`,
	}
	for _, q := range queries {
		t.Run(shortName(q), func(t *testing.T) {
			sres, serr := server.ExecString(q)
			cres, cerr := client.ExecString(q)
			if (serr == nil) != (cerr == nil) {
				t.Fatalf("server err = %v, client err = %v", serr, cerr)
			}
			if serr != nil {
				return
			}
			if got, want := render(sres), render(cres); got != want {
				t.Errorf("the two joins disagree.\n server: %s\n client: %s", got, want)
			}
		})
	}
}

// render flattens a result to a comparable string. Row ORDER is preserved rather than sorted:
// ORDER BY is in the matrix above, and a server-side join that returned the right rows in the
// wrong order would be a real difference, not a presentational one.
func render(r *Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "cols=%v", r.Columns)
	for _, row := range r.Rows {
		fmt.Fprintf(&b, " |%v", row)
	}
	return b.String()
}

func shortName(q string) string {
	q = strings.TrimPrefix(q, "SELECT ")
	if i := strings.Index(q, "FROM "); i >= 0 {
		q = q[i:]
	}
	q = strings.ReplaceAll(q, " ", "_")
	if len(q) > 60 {
		q = q[:60]
	}
	return q
}

// And the server path must actually be taken, or the matrix above compares the client-side
// join with itself and proves nothing.
//
// EXPLAIN says what WILL happen, so it cannot show what did; the server's own query log can.
// The advertising executor must issue a Join, and the other must not -- it must read the two
// sides instead.
func TestServerSideJoinIsActuallyUsed(t *testing.T) {
	server, client, serverOps, clientOps, cleanup := newJoinPair(t)
	defer cleanup()

	const q = `SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`
	serverOps.reset()
	clientOps.reset()
	if _, err := server.ExecString(q); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ExecString(q); err != nil {
		t.Fatal(err)
	}
	// Either join opcode counts: which reply form is used is the next test's question, and
	// this one is only asking whether the pairing crossed to the store at all.
	sops := serverOps.waitOps(t, 1)
	if !serverOps.saw("Join") && !serverOps.saw("JoinWire") {
		t.Errorf("the advertising executor issued no join: saw %v.\nThe differential matrix "+
			"would then be comparing the client-side join against itself.", sops)
	}
	// The non-advertising executor reads the two sides, so it logs ops of its own; waiting
	// for those is what makes "it issued no join" a statement about a finished request.
	cops := clientOps.waitOps(t, 2)
	if clientOps.saw("Join") || clientOps.saw("JoinWire") {
		t.Errorf("the non-advertising executor issued a join: saw %v", cops)
	}
}

// EXPLAIN has to say which way a join will go, and why when it cannot go to the store.
// "client-side" alone leaves a reader unable to tell a server that cannot join from a
// statement that cannot be sent, and those have different answers.
func TestExplainSaysWhichJoinPath(t *testing.T) {
	server, client, _, _, cleanup := newJoinPair(t)
	defer cleanup()

	const q = `EXPLAIN SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`
	sres, err := server.ExecString(q)
	if err != nil {
		t.Fatal(err)
	}
	cres, err := client.ExecString(q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(render(sres), "executed in the store") {
		t.Errorf("EXPLAIN against an advertising server does not say the store runs it:\n%s", render(sres))
	}
	if !strings.Contains(render(cres), "not enabled") {
		t.Errorf("EXPLAIN against an executor with the server-side join off does not say why it "+
			"runs here:\n%s", render(cres))
	}
}
