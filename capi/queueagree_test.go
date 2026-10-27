package main

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// The database can answer a query two ways -- binary wire rows rendered here, or
// old-ClassAd text rendered at the source -- and the caller cannot tell which it
// got. That only holds if the two produce the same bytes, so this runs the same
// queries down both paths against one database and compares the CEDAR records.
//
// A divergence here is a condor_q that answers differently depending on which
// database version it happens to be talking to, which is the worst outcome this
// code has: the wrong answer, silently, to a question the user believed.

// newSocketServer serves a database over a real TCP connection rather than an
// in-memory pipe, because the wire-row stream's batching is a property of the
// transport's framing and a pipe's synchronous handoff hides it.
func newSocketServer(t *testing.T, ads map[string]string) *dbrpc.Client {
	t.Helper()
	d := newStore(t, ads)
	return serveStore(t, d)
}

// newStore opens a database and fills it.
func newStore(t *testing.T, ads map[string]string) *db.DB {
	t.Helper()
	d, err := db.Open(t.TempDir())
	if err != nil {
		t.Fatalf("opening the database: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	for key, text := range ads {
		if err := d.UpdateOld(key, text); err != nil {
			t.Fatalf("storing %s: %v", key, err)
		}
	}
	return d
}

func serveStore(t *testing.T, d *db.DB) *dbrpc.Client {
	t.Helper()
	srv := dbrpc.NewServer(d)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = srv.ServeConn(dbrpc.NewStreamConn(c)) }()
		}
	}()
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	cl := dbrpc.NewClient(dbrpc.NewStreamConn(raw))
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// agreeAds is a query's result in a form two paths can be compared by: the
// expressions of each ad, sorted, since the two walk a stored ad in different
// orders and the order of a ClassAd's attributes carries no meaning.
type agreeAds [][]string

func normalizeAds(ads [][]string) agreeAds {
	out := make(agreeAds, 0, len(ads))
	for _, ad := range ads {
		cp := append([]string(nil), ad...)
		sort.Strings(cp)
		out = append(out, cp)
	}
	// The two paths also need not visit the ADS in the same order.
	sort.Slice(out, func(i, j int) bool {
		return strings.Join(out[i], "\x00") < strings.Join(out[j], "\x00")
	})
	return out
}

// runBothPaths answers one query twice, once over wire rows and once over text
// rows, and returns (wire, text) results.
func runBothPaths(t *testing.T, cl *dbrpc.Client, constraint string, attrs []string,
	limit int, newBuilder func() *adRecordBuilder) (agreeAds, string, agreeAds, string) {
	t.Helper()

	wireAds, wireCounts := runStream(t, cl, constraint, attrs, limit, newBuilder())

	forceTextRows = true
	defer func() { forceTextRows = false }()
	textAds, textCounts := runStream(t, cl, constraint, attrs, limit, newBuilder())

	return normalizeAds(wireAds), wireCounts, normalizeAds(textAds), textCounts
}

func assertAgree(t *testing.T, label string, wire agreeAds, wireCounts string, text agreeAds, textCounts string) {
	t.Helper()
	if len(wire) != len(text) {
		t.Fatalf("%s: wire path returned %d ads, text path %d", label, len(wire), len(text))
	}
	for i := range wire {
		if len(wire[i]) != len(text[i]) {
			t.Fatalf("%s: ad %d has %d expressions over wire rows and %d over text rows:\n wire: %q\n text: %q",
				label, i, len(wire[i]), len(text[i]), wire[i], text[i])
		}
		for j := range wire[i] {
			if wire[i][j] != text[i][j] {
				t.Fatalf("%s: ad %d expression %d differs:\n wire: %q\n text: %q",
					label, i, j, wire[i][j], text[i][j])
			}
		}
	}
	if got, want := sortedLines(wireCounts), sortedLines(textCounts); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("%s: counts differ:\n wire: %q\n text: %q", label, got, want)
	}
}

// agreementAds exercises what the two renderers can disagree about: expression
// values that reference siblings (the projection closure), every literal kind,
// a string holding the characters the two ClassAd dialects quote differently, a
// private attribute, and the type fields.
func agreementAds() map[string]string {
	return map[string]string{
		"1.0": "ClusterId = 1\nProcId = 0\nOwner = \"alice\"\nJobStatus = 1\nJobUniverse = 5\n" +
			"MyType = \"Job\"\nTargetType = \"Machine\"\n" +
			"RequestMemory = 2048\nRequestDisk = 100000\nRequestCpus = 1\n" +
			"Requirements = (TARGET.Memory >= RequestMemory) && (TARGET.Disk >= RequestDisk)\n" +
			"Rank = 0.5\nNiceUser = false\nWantCheckpoint = true\n" +
			"Args = \"C:\\tmp\\run.exe --flag\"\nEnv = \"PATH=/bin\tHOME=/home\"\n" +
			"Big = 9007199254740993\nNeg = -17\nSci = 1.5e-9\n" +
			"ListAttr = { 1, 2, \"three\", RequestCpus }\n" +
			"RecAttr = [ a = 1; b = RequestMemory ]\n" +
			"Missing = undefined\nBroken = error\n" +
			"ClaimId = \"<secret-claim>\"\n_condor_privThing = \"also secret\"\n",
		"1.1": "ClusterId = 1\nProcId = 1\nOwner = \"alice\"\nJobStatus = 2\nJobUniverse = 5\n" +
			"MyType = \"Job\"\nRequestMemory = 4096\nRequestDisk = 200000\n" +
			"Requirements = TARGET.Disk >= RequestDisk\n",
		"2.0": "ClusterId = 2\nProcId = 0\nOwner = \"bob\"\nJobStatus = 5\nJobUniverse = 12\n" +
			"RequestMemory = 1024\nRequirements = true\nHoldReason = \"failed\"\n",
		// An ad with nothing but the counting attributes, so an empty projection
		// result still has to line up.
		"3.0": "JobStatus = 1\nJobUniverse = 9\n",
	}
}

func TestWireAndTextPathsAgree(t *testing.T) {
	cl := newSocketServer(t, agreementAds())
	requireWireRows(t, cl, dbrpc.DefaultTable)
	runAgreementCases(t, cl, "true")
}

// The same comparison against a store whose segments have been recompacted with
// an intern dictionary, which is what a mirror that has been up for a while is.
// Wire rows are assembled differently there -- the server has to convert each
// record back to inline names before it can ship one -- and a fresh store never
// takes that branch, so a fixture that only ever tests the fresh one would pass
// on a divergence every production mirror hits.
//
// The retrain needs a store with enough repeated literals to find any, hence the
// filler ads; the comparison itself runs on the four real ones.
func TestWireAndTextPathsAgreeOnAnInternedStore(t *testing.T) {
	d := newStore(t, agreementAds())
	for i := 0; i < 500; i++ {
		if err := d.UpdateOld(fmt.Sprintf("filler%d.0", i), "Filler = true\n"+benchAd(i, 0, 1, 5)); err != nil {
			t.Fatalf("storing filler: %v", err)
		}
	}
	n, err := d.RetrainDict(4000)
	if err != nil {
		t.Fatalf("recompacting with an intern dictionary: %v", err)
	}
	if n == 0 {
		t.Fatal("the store was not recompacted, so this would repeat the fresh-store test")
	}

	cl := serveStore(t, d)
	requireWireRows(t, cl, dbrpc.DefaultTable)
	runAgreementCases(t, cl, "Filler =!= true")
}

// runAgreementCases answers every shape of query twice, once down each path, and
// requires the two results to be the same. only narrows the queries to the ads
// under test.
func runAgreementCases(t *testing.T, cl *dbrpc.Client, only string) {
	t.Helper()
	cases := []struct {
		name       string
		constraint string
		attrs      []string
		limit      int
		builder    func() *adRecordBuilder
	}{
		{
			name: "whole ad", constraint: "true", limit: -1,
			builder: func() *adRecordBuilder { return &adRecordBuilder{emit: true} },
		},
		{
			// Literal-valued attributes only: nothing to expand, so the two paths
			// have the easiest case to agree on.
			name: "literal projection", constraint: "true", limit: -1,
			attrs:   []string{"ClusterId", "ProcId", "Owner", "JobStatus"},
			builder: func() *adRecordBuilder { return &adRecordBuilder{emit: true} },
		},
		{
			// The case the wire stream's plain projection gets wrong on its own:
			// Requirements refers to RequestMemory and RequestDisk, which the client
			// did not ask for and still needs to evaluate what it was sent.
			name: "projection whose expressions reference siblings", constraint: "true", limit: -1,
			attrs:   []string{"ClusterId", "ProcId", "Requirements"},
			builder: func() *adRecordBuilder { return &adRecordBuilder{emit: true} },
		},
		{
			name: "projection of composite values", constraint: "true", limit: -1,
			attrs:   []string{"ListAttr", "RecAttr", "Missing", "Broken", "Args", "Env"},
			builder: func() *adRecordBuilder { return &adRecordBuilder{emit: true} },
		},
		{
			name: "constrained with a server time", constraint: `Owner == "alice"`, limit: -1,
			builder: func() *adRecordBuilder {
				return &adRecordBuilder{emit: true, serverTime: serverTimeExpr(1700000000)}
			},
		},
		{
			name: "limited", constraint: "true", limit: 2,
			builder: func() *adRecordBuilder { return &adRecordBuilder{emit: true} },
		},
		{
			name: "summary only", constraint: "true", limit: -1,
			attrs:   []string{attrJobStatus, attrJobUniverse},
			builder: func() *adRecordBuilder { return &adRecordBuilder{emit: false} },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			constraint := tc.constraint
			if only != "true" {
				constraint = "(" + constraint + ") && (" + only + ")"
			}
			attrs := tc.attrs
			newBuilder := tc.builder
			if len(attrs) > 0 && tc.builder().emit {
				// What hcdb_queue_begin does for a counted query with a projection.
				var suppress []string
				attrs, suppress = withCountingAttrs(attrs)
				inner := tc.builder
				newBuilder = func() *adRecordBuilder {
					b := inner()
					b.suppress = suppress
					return b
				}
			}
			w, wc, x, xc := runBothPaths(t, cl, constraint, attrs, tc.limit, newBuilder)
			assertAgree(t, tc.name, w, wc, x, xc)
		})
	}
}

// The agreement test above proves the paths match each other; this one proves
// what they match on, for the handful of properties a reader of either renderer
// would want nailed down.
func TestWirePathContent(t *testing.T) {
	cl := newSocketServer(t, agreementAds())

	t.Run("expression references ship with the expression", func(t *testing.T) {
		attrs, suppress := withCountingAttrs([]string{"ClusterId", "ProcId", "Requirements"})
		ads, _ := runStream(t, cl, `ClusterId == 1 && ProcId == 0`, attrs, -1,
			&adRecordBuilder{emit: true, suppress: suppress})
		if len(ads) != 1 {
			t.Fatalf("got %d ads, want 1", len(ads))
		}
		got := map[string]string{}
		for _, e := range ads[0] {
			got[attrNameOf(e)] = valueOf(e)
		}
		for _, name := range []string{"ClusterId", "ProcId", "Requirements", "RequestMemory", "RequestDisk"} {
			if _, ok := got[name]; !ok {
				t.Errorf("%s missing from a projection of Requirements: %q", name, ads[0])
			}
		}
		// The counting attributes were added behind the client's back and must not
		// come back out, reference closure or not.
		for _, name := range []string{"JobStatus", "JobUniverse", "Owner"} {
			if _, ok := got[name]; ok {
				t.Errorf("unasked-for %s reached the wire: %q", name, ads[0])
			}
		}
	})

	t.Run("old-ClassAd string quoting", func(t *testing.T) {
		// The old reader does no escape processing, so a stored backslash or tab
		// must come back as itself. Rendering for the new reader instead would
		// double the backslash on every round trip through a condor_q.
		ads, _ := runStream(t, cl, `ClusterId == 1 && ProcId == 0`, []string{"Args", "Env"}, -1,
			&adRecordBuilder{emit: true})
		if len(ads) != 1 {
			t.Fatalf("got %d ads, want 1", len(ads))
		}
		want := map[string]string{
			"Args": `"C:\tmp\run.exe --flag"`,
			"Env":  "\"PATH=/bin\tHOME=/home\"",
		}
		for _, e := range ads[0] {
			if w, ok := want[attrNameOf(e)]; ok {
				if got := valueOf(e); got != w {
					t.Errorf("%s = %q, want %q", attrNameOf(e), got, w)
				}
				delete(want, attrNameOf(e))
			}
		}
		for name := range want {
			t.Errorf("%s never arrived", name)
		}
	})

	t.Run("private attributes never ship", func(t *testing.T) {
		ads, _ := runStream(t, cl, "true", nil, -1, &adRecordBuilder{emit: true})
		for _, ad := range ads {
			for _, e := range ad {
				switch strings.ToLower(attrNameOf(e)) {
				case "claimid", "_condor_privthing":
					t.Errorf("a private attribute reached the wire: %q", e)
				}
			}
		}
	})
}

// A query whose rows the renderer cannot read must fail rather than answer with
// fewer jobs than matched.
func TestWireRowUnreadableFailsTheQuery(t *testing.T) {
	b := &adRecordBuilder{emit: true}
	if _, ok := b.appendWireRecord(nil, []byte{0xff, 0xff, 0xff}); ok {
		t.Fatal("a malformed wire row was accepted")
	}
}

// requireWireRows fails the test if this server does not actually serve wire
// rows. Without it a comparison of the two paths is vacuous: a silent fallback
// would make the wire path BE the text path, and the test would pass on a bug
// that disabled the fast path entirely.
func requireWireRows(t *testing.T, cl *dbrpc.Client, table string) {
	t.Helper()
	rows := 0
	err := cl.QueryRawWireStream(context.Background(), table, "true", []string{"ClusterId"}, 1, true,
		func([]byte) bool { rows++; return true })
	if err != nil {
		t.Fatalf("this server does not serve wire rows (%v), so the comparison would prove nothing", err)
	}
	if rows == 0 {
		t.Fatal("the wire-row stream returned nothing, so the comparison would prove nothing")
	}
}

// A table that cannot serve wire rows must be answered anyway, from the text
// stream, with no sign of it reaching the caller.
func TestWireRowFallbackIsSilent(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatalf("opening the catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	srv := dbrpc.NewServerCatalog(cat)
	a, b := net.Pipe()
	go func() { _ = srv.ServeConn(dbrpc.NewStreamConn(a)) }()
	cl := dbrpc.NewClient(dbrpc.NewStreamConn(b))
	t.Cleanup(func() { _ = cl.Close() })

	ctx := context.Background()
	if err := cl.CreateTableInMemory(ctx, "ram"); err != nil {
		t.Fatalf("creating an in-memory table: %v", err)
	}
	tx, err := cl.BeginTable(ctx, "ram")
	if err != nil {
		t.Fatal(err)
	}
	for key, text := range agreementAds() {
		if err := tx.NewClassAd(ctx, key, text); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	attrs, suppress := withCountingAttrs([]string{"ClusterId", "ProcId", "Requirements"})
	r := &queueRequest{client: &queueClient{notifyFD: -1}, chunks: make(chan queueChunk, 16)}
	r.wg.Add(1)
	go r.stream(ctx, cl, "ram", "true", attrs, -1, &adRecordBuilder{emit: true, suppress: suppress}, true)
	go func() { r.wg.Wait(); close(r.chunks); r.closed.Store(true) }()

	ads, sawClosure := 0, false
	for ch := range r.chunks {
		rest := ch.data
		for len(rest) > 0 {
			var exprs []string
			exprs, _, rest = decodeRecord(t, rest)
			names := map[string]bool{}
			for _, e := range exprs {
				names[attrNameOf(e)] = true
			}
			if names["Requirements"] && names["RequestDisk"] {
				sawClosure = true
			}
			ads++
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		t.Fatalf("the fallback failed instead of answering: %v", r.err)
	}
	if ads != len(agreementAds()) {
		t.Fatalf("got %d ads, want %d", ads, len(agreementAds()))
	}
	// The text fallback expands the projection at the source, so the attribute
	// Requirements refers to has to be in the answer too -- the same guarantee the
	// wire path builds for itself.
	if !sawClosure {
		t.Error("the fallback dropped the attributes the projected expressions reference")
	}
}

// The one place the two paths are known NOT to agree, recorded so a change to
// either side is noticed rather than discovered by a user.
//
// MyType and TargetType do not travel as ordinary expressions on the text path:
// the database lifts them out of the ad and the text stream writes them back as
// lines, but only when they are non-empty. An ad that really stores an empty one
// therefore loses the attribute on the way through the text stream, while the
// wire path -- which sees the ad's entries and nothing else -- keeps it.
//
// Keeping it is the faithful answer (HTCondor carries the real types as ordinary
// attributes in the ad body), so this is a difference the wire path is on the
// right side of, not one to reproduce.
func TestEmptyTypeFieldIsKeptOnlyByTheWirePath(t *testing.T) {
	cl := newSocketServer(t, map[string]string{
		"1.0": "ClusterId = 1\nMyType = \"\"\nTargetType = \"\"\nJobStatus = 1\nJobUniverse = 5\n",
	})
	requireWireRows(t, cl, dbrpc.DefaultTable)

	wire, _, text, _ := runBothPaths(t, cl, "true", nil, -1,
		func() *adRecordBuilder { return &adRecordBuilder{emit: true} })
	if len(wire) != 1 || len(text) != 1 {
		t.Fatalf("got %d wire ads and %d text ads, want one of each", len(wire), len(text))
	}
	has := func(ad []string, expr string) bool {
		for _, e := range ad {
			if e == expr {
				return true
			}
		}
		return false
	}
	for _, expr := range []string{`MyType = ""`, `TargetType = ""`} {
		if !has(wire[0], expr) {
			t.Errorf("the wire path dropped %s: %q", expr, wire[0])
		}
		if has(text[0], expr) {
			t.Errorf("the text path now keeps %s; the paths agree here and this test is stale: %q", expr, text[0])
		}
	}
}
