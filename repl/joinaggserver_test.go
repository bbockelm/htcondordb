package repl

import (
	"strings"
	"testing"
)

// A GROUPED join reduced in the store must answer exactly what reducing it here answers.
//
// Same differential harness as the join itself: one executor has the handshake and sends
// opJoinAgg, the other does not and folds the joined rows in this process. The two paths share
// no code below the reply -- the store runs db's reduce engine, this runs aggregateJoinRows --
// so the only thing holding them together is that they agree, query by query.
func TestTheGroupedJoinArmsAgree(t *testing.T) {
	server, client, _, _, cleanup := newJoinPair(t)
	defer cleanup()

	queries := []string{
		`SELECT j.Owner, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId GROUP BY j.Owner`,
		`SELECT j.Owner, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId GROUP BY j.Owner ORDER BY j.Owner`,
		// An aggregate over the side that is folded in, which is the argument that has to say
		// which table it comes from.
		`SELECT j.Owner, SUM(h.ExitCode), MIN(h.ExitCode), MAX(h.ExitCode), COUNT(h.ExitCode)
			FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId GROUP BY j.Owner ORDER BY j.Owner`,
		`SELECT j.Owner, AVG(h.ExitCode) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
			GROUP BY j.Owner ORDER BY j.Owner`,
		// No GROUP BY at all: one group over the whole join.
		`SELECT COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`,
		`SELECT SUM(h.ExitCode) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`,
		// Two group columns, one from each side.
		`SELECT j.Owner, h.ExitCode, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
			GROUP BY j.Owner, h.ExitCode ORDER BY j.Owner, h.ExitCode`,
		// A LEFT join, whose unmatched rows group under an undefined column.
		`SELECT j.Owner, COUNT(h.ExitCode) FROM jobs j LEFT JOIN hist h ON j.ClusterId == h.ClusterId
			GROUP BY j.Owner ORDER BY j.Owner`,
		// A WHERE that reaches a side, and one that reaches neither and is checked per pair.
		`SELECT j.Owner, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
			WHERE h.ExitCode > 0 GROUP BY j.Owner ORDER BY j.Owner`,
		`SELECT j.Owner, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
			WHERE j.Cpus > h.ExitCode GROUP BY j.Owner ORDER BY j.Owner`,
		// HAVING filters groups, which happens here on both arms -- but only because the
		// aggregate it reads was sent with the rest.
		`SELECT j.Owner, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
			GROUP BY j.Owner HAVING COUNT(*) > 1 ORDER BY j.Owner`,
		`SELECT j.Owner, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
			GROUP BY j.Owner HAVING SUM(h.ExitCode) > 0 ORDER BY j.Owner`,
		// A conditional aggregate: the filter is an expression over the composite row, which
		// is the same kind of thing the group columns are.
		`SELECT j.Owner, COUNT(*) FILTER (WHERE h.ExitCode > 0) FROM jobs j
			JOIN hist h ON j.ClusterId == h.ClusterId GROUP BY j.Owner ORDER BY j.Owner`,
		// An aggregate inside an expression: one COUNT per group, the arithmetic over it.
		`SELECT j.Owner, 2 * COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
			GROUP BY j.Owner ORDER BY j.Owner`,
		// ORDER BY the aggregate rather than the group, and a LIMIT after it -- both applied
		// to the reduced rows, so a limit pushed into the join would keep the wrong ones.
		`SELECT j.Owner, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
			GROUP BY j.Owner ORDER BY COUNT(*) DESC LIMIT 1`,
		// A group column that is an expression rather than an attribute.
		`SELECT j.Cpus * 2, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
			GROUP BY j.Cpus ORDER BY j.Cpus`,
		// COUNT(DISTINCT), whose reduction is a set rather than a running total.
		`SELECT j.Owner, COUNT(DISTINCT h.ExitCode) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
			GROUP BY j.Owner ORDER BY j.Owner`,
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
				t.Errorf("the two reductions disagree.\n store: %s\n here:  %s", got, want)
			}
		})
	}
}

// And the store's reduction must actually be the thing running, or the matrix above compares
// the client-side reduction with itself.
//
// The op name is the evidence. A grouped join that fell back would log a Join or a JoinWire
// instead and still answer correctly, which is exactly the failure a differential test cannot
// see.
func TestTheGroupedJoinIsReducedInTheStore(t *testing.T) {
	server, client, serverOps, clientOps, cleanup := newJoinPair(t)
	defer cleanup()

	const q = `SELECT j.Owner, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId GROUP BY j.Owner`
	serverOps.reset()
	clientOps.reset()
	if _, err := server.ExecString(q); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ExecString(q); err != nil {
		t.Fatal(err)
	}
	sops := serverOps.waitOps(t, 1)
	if !serverOps.saw("JoinAggregate") {
		t.Errorf("the advertising executor did not reduce in the store: saw %v", sops)
	}
	// One op, not two: a reduction that ran and THEN shipped the rows would have asked for
	// both, which is the shape a half-taken path has.
	if serverOps.saw("Join") || serverOps.saw("JoinWire") {
		t.Errorf("the advertising executor also streamed the joined rows: saw %v", sops)
	}
	cops := clientOps.waitOps(t, 2)
	if clientOps.saw("JoinAggregate") {
		t.Errorf("the non-advertising executor reduced in the store: saw %v", cops)
	}
}

// A statement the reduction cannot carry must still be answered, by the path that always
// answered it -- and it must not reach the store as a reduction first.
func TestAShapeTheReductionCannotCarryFallsBack(t *testing.T) {
	server, _, serverOps, _, cleanup := newJoinPair(t)
	defer cleanup()

	// Grouping with nothing to reduce. Both arms answer it as a DISTINCT over the joined
	// rows; sending it would buy nothing and would have to agree about a shape neither arm
	// otherwise runs.
	const q = `SELECT j.Owner FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId GROUP BY j.Owner`
	serverOps.reset()
	res, err := server.ExecString(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("the fallback answered nothing: %s", render(res))
	}
	ops := serverOps.waitOps(t, 1)
	if serverOps.saw("JoinAggregate") {
		t.Errorf("a statement the reduction declines was sent as one anyway: saw %v", ops)
	}
}

// EXPLAIN must say where a grouped join is reduced, and why when it is not reduced in the
// store. The two cases read alike otherwise: a server that cannot reduce and a statement that
// cannot be sent both end up folding rows here, and they are fixed differently.
func TestExplainSaysWhereAGroupedJoinIsReduced(t *testing.T) {
	server, client, _, _, cleanup := newJoinPair(t)
	defer cleanup()

	const q = `EXPLAIN SELECT j.Owner, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId GROUP BY j.Owner`
	sres, err := server.ExecString(q)
	if err != nil {
		t.Fatal(err)
	}
	cres, err := client.ExecString(q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(render(sres), "grouping: in the store") {
		t.Errorf("EXPLAIN does not say the store reduces it:\n%s", render(sres))
	}
	if !strings.Contains(render(cres), "does not advertise a server-side join") {
		t.Errorf("EXPLAIN does not say why the reduction happens here:\n%s", render(cres))
	}
}

// The session's own placement override reaches the reduction too.
//
// It is the switch an operator has when the store is the wrong place for this query, and a
// reduction that ignored it would leave them no way to say so.
func TestForcingThePairingHereAlsoKeepsTheReductionHere(t *testing.T) {
	server, _, serverOps, _, cleanup := newJoinPair(t)
	defer cleanup()
	server.placement = JoinPlacementClient

	const q = `SELECT j.Owner, COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId GROUP BY j.Owner`
	serverOps.reset()
	if _, err := server.ExecString(q); err != nil {
		t.Fatal(err)
	}
	ops := serverOps.waitOps(t, 2)
	if serverOps.saw("JoinAggregate") {
		t.Errorf("a session that forced the pairing here still reduced in the store: saw %v", ops)
	}
}
