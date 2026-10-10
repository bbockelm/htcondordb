package federate

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// fakeCollector answers queries from canned ads, keyed by ad type. A nil slice with err set fails
// that query.
type fakeCollector struct {
	schedds    []*classad.ClassAd
	dbs        []*classad.ClassAd
	scheddErr  error
	dbErr      error
	presentAll []*classad.ClassAd // unconstrained schedd query; defaults to schedds
}

func (f *fakeCollector) Query(_ context.Context, adType, constraint string, _ []string) ([]*classad.ClassAd, error) {
	switch adType {
	case "Schedd":
		if f.scheddErr != nil {
			return nil, f.scheddErr
		}
		if constraint == "" && f.presentAll != nil {
			return f.presentAll, nil
		}
		return f.schedds, nil
	case "HTCondorDB":
		return f.dbs, f.dbErr
	}
	return nil, fmt.Errorf("unexpected ad type %q", adType)
}

func scheddAd(t *testing.T, name, addr string) *classad.ClassAd {
	return parseAd(t, fmt.Sprintf(`Name = %q; MyAddress = %q`, name, addr))
}

func spokeAd(t *testing.T, name, addr, mirrored string, syncing, caughtUp bool) *classad.ClassAd {
	return parseAd(t, fmt.Sprintf(`Name = %q; MyAddress = %q; MirroredScheddName = %q; Syncing = %v; JobQueueCaughtUp = %v; HistoryCaughtUp = %v`,
		name, addr, mirrored, syncing, caughtUp, caughtUp))
}

func noResolve(context.Context, string) ([]string, error) { return nil, errors.New("no DNS in tests") }

func TestDiscoveryPairsByName(t *testing.T) {
	fc := &fakeCollector{
		schedds: []*classad.ClassAd{
			scheddAd(t, "ap1.example.org", "<10.0.0.1:9618?alias=ap1.example.org>"),
			scheddAd(t, "ap2.example.org", "<10.0.0.2:9618>"),
		},
		dbs: []*classad.ClassAd{
			// the claimed host resolves to the spoke's primary address
			spokeAd(t, "db1", "<10.0.0.1:9620?alias=ap1.example.org>", "ap1.example.org", true, true),
			spokeAd(t, "db2", "<10.0.0.2:9620>", "ap2.example.org", true, true),
			// claims a schedd outside the AP set: ignored, not rejected
			spokeAd(t, "db9", "<10.0.0.9:9620>", "ap9.example.org", true, true),
		},
	}
	d := &Discovery{Collector: fc, ScheddConstraint: `true`,
		Resolve: resolveMap(map[string][]string{"ap1.example.org": {"10.0.0.1"}, "ap2.example.org": {"10.0.0.2"}})}
	snap, err := d.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Spokes) != 2 || snap.Spokes["ap1.example.org"].Address != "<10.0.0.1:9620?alias=ap1.example.org>" ||
		snap.Spokes["ap2.example.org"].SpokeName != "db2" {
		t.Fatalf("spokes = %+v", snap.Spokes)
	}
	if len(snap.Rejected) != 0 {
		t.Errorf("rejected = %+v, want none", snap.Rejected)
	}
	if !snap.Matched["ap1.example.org"] || snap.Matched["ap9.example.org"] {
		t.Errorf("matched = %v", snap.Matched)
	}
}

// TestDiscoveryRejectsHostMismatch: a spoke on another host claiming an AP's schedd must not be
// paired, or it could publish rows under that AP's name.
func TestDiscoveryRejectsHostMismatch(t *testing.T) {
	fc := &fakeCollector{
		schedds: []*classad.ClassAd{scheddAd(t, "ap1.example.org", "<10.0.0.1:9618?alias=ap1.example.org>")},
		dbs:     []*classad.ClassAd{spokeAd(t, "evil", "<10.6.6.6:9620?alias=evil.example.org>", "ap1.example.org", true, true)},
	}
	d := &Discovery{Collector: fc, ScheddConstraint: `true`, Resolve: noResolve}
	snap, err := d.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Spokes["ap1.example.org"]; ok {
		t.Fatal("a spoke on another host was paired with ap1")
	}
	if len(snap.Rejected) != 1 || snap.Rejected[0].Reason != RejectHostMismatch {
		t.Fatalf("rejected = %+v, want one host_mismatch", snap.Rejected)
	}
	if snap.Untrusted["ap1.example.org"] == "" {
		t.Error("ap1 not marked untrusted")
	}

	// The same claim passes when the claimed host resolves to the spoke's address.
	d.Resolve = func(_ context.Context, host string) ([]string, error) {
		if host == "ap1.example.org" {
			return []string{"10.6.6.6"}, nil
		}
		return nil, errors.New("nx")
	}
	snap, _ = d.Discover(context.Background())
	if _, ok := snap.Spokes["ap1.example.org"]; !ok {
		t.Fatal("DNS-validated spoke not paired")
	}
}

// TestDiscoveryHATie: two valid spokes for one schedd -- prefer the syncing, caught-up one;
// decline rather than guess when both (or neither) are.
func TestDiscoveryHATie(t *testing.T) {
	sd := []*classad.ClassAd{scheddAd(t, "ap1.example.org", "<10.0.0.1:9618>")}
	run := func(a, b *classad.ClassAd) Snapshot {
		d := &Discovery{Collector: &fakeCollector{schedds: sd, dbs: []*classad.ClassAd{a, b}}, ScheddConstraint: `true`,
			Resolve: resolveMap(map[string][]string{"ap1.example.org": {"10.0.0.1"}})}
		snap, err := d.Discover(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return snap
	}
	snap := run(spokeAd(t, "dbA", "<10.0.0.1:9620>", "ap1.example.org", true, true),
		spokeAd(t, "dbB", "<10.0.0.1:9621>", "ap1.example.org", true, false))
	if snap.Spokes["ap1.example.org"].SpokeName != "dbA" {
		t.Errorf("caught-up spoke not preferred: %+v", snap.Spokes)
	}
	snap = run(spokeAd(t, "dbA", "<10.0.0.1:9620>", "ap1.example.org", true, true),
		spokeAd(t, "dbB", "<10.0.0.1:9621>", "ap1.example.org", true, true))
	if _, ok := snap.Spokes["ap1.example.org"]; ok {
		t.Errorf("tie resolved by guessing: %+v", snap.Spokes)
	}
	if snap.Declined["ap1.example.org"] == "" {
		t.Error("tie not reported as declined")
	}
	ties := 0
	for _, r := range snap.Rejected {
		if r.Reason == RejectHATie {
			ties++
		}
	}
	if ties != 2 {
		t.Errorf("ha_tie rejections = %d, want 2", ties)
	}
}

// TestDiscoveryCollectorFailure: a failed query is not an empty AP set.
func TestDiscoveryCollectorFailure(t *testing.T) {
	d := &Discovery{Collector: &fakeCollector{scheddErr: errors.New("connection refused")}, ScheddConstraint: `true`}
	if _, err := d.Discover(context.Background()); err == nil {
		t.Fatal("a failed collector query reported success")
	}
	// With static spokes the pass still yields them, but the constraint part is unknown.
	d.Static = []Spoke{{Schedd: "s1", Address: "<127.0.0.1:1>"}}
	snap, err := d.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.MatchKnown || !snap.Spokes["s1"].Static {
		t.Errorf("snapshot = %+v", snap)
	}
	// Spoke ads failing alone leaves the match set known but the spokes unknown.
	d = &Discovery{Collector: &fakeCollector{schedds: []*classad.ClassAd{scheddAd(t, "ap1", "<10.0.0.1:1>")}, dbErr: errors.New("x")}, ScheddConstraint: `true`}
	snap, _ = d.Discover(context.Background())
	if !snap.MatchKnown || snap.SpokesKnown || !snap.Matched["ap1"] {
		t.Errorf("snapshot = %+v", snap)
	}
}

func TestDiscoveryConstraintAndStatic(t *testing.T) {
	d := &Discovery{ScheddConstraint: `regexp("^ap", Name)`, Static: []Spoke{{Schedd: `we"ird`, Address: "<1.2.3.4:5>"}}}
	if got := d.Constraint(); got != `(regexp("^ap", Name)) || Name == "we\"ird"` {
		t.Errorf("Constraint() = %s", got)
	}
	if _, err := classad.Parse("[c = " + d.Constraint() + "]"); err != nil {
		t.Errorf("constraint does not parse: %v", err)
	}
	snap, err := (&Discovery{Static: d.Static}).Discover(context.Background())
	if err != nil || !snap.MatchKnown || !snap.Spokes[`we"ird`].Static {
		t.Errorf("static-only discovery: %+v, %v", snap, err)
	}
}

func TestPrimaryHost(t *testing.T) {
	for in, want := range map[string]string{
		"<10.0.0.1:9618?addrs=10.0.0.9-9618&alias=AP9.example.org&sock=schedd_1>": "10.0.0.1",
		"<[2001:db8::1]:9618>":   "2001:db8::1",
		"<AP1.Example.org:9618>": "ap1.example.org",
		"":                       "",
	} {
		if got := primaryHost(in); got != want {
			t.Errorf("primaryHost(%q) = %q, want %q", in, got, want)
		}
	}
}
