package repl

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// The store-side join has two reply forms. The rows are the same rows; what differs is that
// the wire form is not rendered by the server or parsed here, and many rows ride in one
// frame instead of one row per frame. The client prefers it and falls back, so there are
// three things to pin: that it is preferred, that each way of losing it falls back rather
// than failing, and that the fallback answers identically.

// withoutFeature is a peer's advertisement with one feature struck out, which is how a
// server that joins but cannot batch presents itself.
func withoutFeature(attrs map[string]string, f dbrpc.Feature) map[string]string {
	out := map[string]string{}
	for k, v := range attrs {
		out[k] = v
	}
	var keep []string
	for _, name := range strings.Split(out[dbrpc.SessionAttrFeatures], ",") {
		if name != string(f) {
			keep = append(keep, name)
		}
	}
	out[dbrpc.SessionAttrFeatures] = strings.Join(keep, ",")
	return out
}

func TestJoinPrefersTheWireReply(t *testing.T) {
	s, cleanup := newJoinCatalog(t)
	defer cleanup()

	ops := &opRecorder{}
	e := joinExecutor(t, s, s.SessionAttrs(), true, ops)
	if _, err := e.ExecString(
		`SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`); err != nil {
		t.Fatal(err)
	}
	// Exactly one op: the wire reply, and no text reply beside it.
	if got := ops.waitOps(t, 1); len(got) != 1 || got[0] != "JoinWire" {
		t.Errorf("a server advertising the wire-form join should be asked for it once and for "+
			"nothing else: saw %v", got)
	}
}

// A server that joins but does not advertise the batched reply must get the text one, and
// the client must not spend a round trip finding that out.
func TestJoinFallsBackWhenTheWireReplyIsNotAdvertised(t *testing.T) {
	s, cleanup := newJoinCatalog(t)
	defer cleanup()

	const q = `SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`
	full := &opRecorder{}
	wireless := &opRecorder{}
	eFull := joinExecutor(t, s, s.SessionAttrs(), true, full)
	eOld := joinExecutor(t, s, withoutFeature(s.SessionAttrs(), dbrpc.FeatJoinWire), true, wireless)

	want, err := eFull.ExecString(q)
	if err != nil {
		t.Fatal(err)
	}
	got, err := eOld.ExecString(q)
	if err != nil {
		t.Fatal(err)
	}
	// One op, and it is the text one: a client that asked for the unadvertised form first
	// would log two, having bought a guaranteed round-trip rejection.
	if got := wireless.waitOps(t, 1); len(got) != 1 || got[0] != "Join" {
		t.Errorf("a server that does not advertise the wire reply should be asked for the text "+
			"one and nothing else: saw %v", got)
	}
	if render(got) != render(want) {
		t.Errorf("the two reply forms disagree.\n  text: %s\n  wire: %s", render(got), render(want))
	}
}

// The other way the wire reply goes away, and the one advertisement cannot cover: the server
// says yes and then cannot honour it for THIS request. An unprivileged connection is redacted
// whatever it asked for, and a join over whole ads has no projection to withhold the private
// attributes from, so the store refuses that one request. The refusal arrives before any row,
// so the text reply -- whose render withholds them -- is still open.
func TestJoinFallsBackWhenTheServerRefusesTheWireReply(t *testing.T) {
	s, cleanup := newJoinCatalog(t)
	defer cleanup()

	ops := &opRecorder{}
	e := joinExecutor(t, s, s.SessionAttrs(), false, ops)
	res, err := e.ExecString(`SELECT * FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`)
	if err != nil {
		t.Fatalf("a refused wire reply must fall back, not fail: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("the fallback returned no rows, which is what a silently-dropped stream looks like")
	}
	// The wire reply is tried, refused, and the text one takes over.
	//
	// Asserted on WHAT EACH OP DID, not on the order they were logged in. The log is written
	// from a deferred call in each request's own handler goroutine, so the refused wire op can
	// be recorded after the text one that followed it -- the client issues them in order, the
	// logging does not promise to record them in that order, and a test that required it failed
	// in CI as "saw [Join JoinWire]".
	//
	// What distinguishes a fallback from a plain text join is that BOTH ops ran and the wire one
	// yielded nothing, which is stronger evidence than their order and does not depend on
	// scheduling.
	got := ops.waitOps(t, 2)
	if len(got) != 2 {
		t.Fatalf("want two server ops, saw %v", got)
	}
	wire, sawWire := ops.logFor("JoinWire")
	text, sawText := ops.logFor("Join")
	if !sawWire || !sawText {
		t.Fatalf("want the wire reply tried and the text one to take over, saw %v", got)
	}
	if wire.Rows != 0 {
		t.Errorf("the wire reply returned %d rows; it was supposed to be refused before any row",
			wire.Rows)
	}
	if text.Rows == 0 {
		t.Error("the text reply returned no rows, so nothing took over from the refused wire one")
	}
}

// The two arms of an expansion benchmark must differ in ONE thing, and over an archive they
// do not. A read of a mutable table takes the wire row stream; an archive is served by its
// own opcode and wireEligible excludes it, so the client-side join's side reads come back as
// rendered text while the store-side reply is wire rows whichever kind of table it is.
//
// That is a real property of the shipped paths, not a flaw in them -- but it means an archive
// comparison moves the encoding as well as the pairing, and a measurement that moves two
// things cannot attribute its difference to one. This pins which ops each arm issues, so the
// benchmark's framing is revisited if an archive ever gains a wire row stream rather than
// quietly becoming wrong.
func TestJoinArmsReadArchivesAsTextAndTablesAsWire(t *testing.T) {
	for _, tc := range []struct {
		name    string
		archive bool
		want    string
	}{
		{"table", false, "QueryRawWire"},
		{"archive", true, "QueryRawProjectRefs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fix, ops, _ := newExpansionJoinFixture(t, 50, 2, tc.archive)
			defer fix.cleanup()

			ops.reset()
			if _, err := fix.exec.ExecString(expansionJoinSQL); err != nil {
				t.Fatal(err)
			}
			// Two side reads; a row count for the key-batch decision may sit between them.
			got := ops.waitOpsB(t, 2)
			reads := 0
			for _, op := range got {
				if op == tc.want {
					reads++
				}
			}
			if reads != 2 {
				t.Errorf("the client-side join read its two sides with %v, want two %s", got, tc.want)
			}

			ops.reset()
			if _, err := fix.srvExec.ExecString(expansionJoinSQL); err != nil {
				t.Fatal(err)
			}
			if sops := ops.waitOpsB(t, 1); sops[0] != "JoinWire" {
				t.Errorf("the store-side arm issued %v, want JoinWire whichever kind of table", sops)
			}
		})
	}
}
