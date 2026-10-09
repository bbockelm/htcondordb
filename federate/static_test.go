package federate

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/cedarsync"
)

// TestStaticFlagFollowsConfig: a source's Static flag is the current configuration's, refreshed on
// every pass. A schedd moved from HTCONDORDB_FEDERATE_SPOKES to the constraint kept Static = true,
// so the next collector outage -- which drops it from the match set -- read as "the static spoke was
// removed" and put it into retirement.
func TestStaticFlagFollowsConfig(t *testing.T) {
	disc := &fakeDiscovery{}
	snap := members("ap1")
	snap.Spokes["ap1"] = Spoke{Schedd: "ap1", Address: "static-addr", Static: true}
	disc.set(snap)
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	seedHubRows(t, cat, "ap1", 2)
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: unreachable})
	defer hub.stop(t)
	waitFor(t, "static ap1", func() bool { v, _ := sourceRowAttrBool(cat, "ap1", "Static"); return v })

	// The admin moves ap1 to the constraint; its spoke ad does not pair (no spoke this pass).
	snap = members("ap1")
	delete(snap.Spokes, "ap1")
	disc.set(snap)
	hub.hub.Rediscover()
	waitFor(t, "ap1 no longer static", func() bool {
		v, ok := sourceRowAttrBool(cat, "ap1", "Static")
		return ok && !v
	})

	// The collector restarts and lists nothing: absent, not retiring.
	disc.set(members())
	hub.hub.Rediscover()
	waitFor(t, "ap1 absent", func() bool { return sourceState(cat, "ap1") == StateAbsent })
	if n := scheddRows(t, cat, TableJobs, "ap1"); n != 2 {
		t.Errorf("ap1 rows = %d, want 2", n)
	}
}

func sourceRowAttrBool(cat *db.Catalog, schedd, attr string) (bool, bool) {
	row, ok := sourceRow(cat, schedd)
	if !ok {
		return false, false
	}
	return row.EvaluateAttrBool(attr)
}

// TestStaticSpokeWithCollectorDown: static spokes are configuration, not collector data -- they are
// federated (and streamed) while the collector is unreachable.
func TestStaticSpokeWithCollectorDown(t *testing.T) {
	fc := &fakeCollector{scheddErr: errors.New("collector down")}
	disc := &Discovery{Collector: fc, ScheddConstraint: `true`, Resolve: noResolve,
		Static: []Spoke{{Schedd: "ap9", Address: "static-addr"}}}
	var dialed atomic.Int32
	dial := func(addr string) cedarsync.Dial {
		return func(context.Context) (*dbrpc.Client, func(), error) {
			if addr == "static-addr" {
				dialed.Add(1)
			}
			return nil, nil, errors.New("spoke unreachable (test)")
		}
	}
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	// A constraint member the hub knew before it restarted (no spoke paired yet).
	src := mustTable(t, cat, TableSources)
	tx := src.Begin()
	tx.NewClassAd("ap1", parseAd(t, `ScheddName = "ap1"; LastSeen = `+itoa(int(time.Now().Unix()))))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: dial})
	defer hub.stop(t)
	waitFor(t, "static ap9 known", func() bool { return sourceState(cat, "ap9") != "" })
	waitFor(t, "static ap9 dialed", func() bool { return dialed.Load() > 0 })
	row, _ := sourceRow(cat, "ap9")
	if v, _ := row.EvaluateAttrBool("Static"); !v {
		t.Errorf("ap9 row = %v, want Static", row)
	}
	// The constraint's members are unknown while the collector is down: kept, not leaving.
	hub.hub.Rediscover()
	hub.statePasses(t, 2)
	if st := sourceState(cat, "ap1"); st != StateAbsent {
		t.Errorf("constraint member with the collector down: state %q, want %q", st, StateAbsent)
	}
}

// seqCollector answers its first Schedd query with before and every later one with after: a schedd
// (re)advertising between two queries of one discovery pass.
type seqCollector struct {
	calls         int
	before, after []*classad.ClassAd
}

func (s *seqCollector) Query(_ context.Context, adType, _ string, _ []string) ([]*classad.ClassAd, error) {
	if adType != "Schedd" {
		return nil, nil
	}
	s.calls++
	if s.calls == 1 {
		return s.before, nil
	}
	return s.after, nil
}

// TestDiscoveryPresentBeforeMatched: the unconstrained (present) query runs before the matched one,
// so a schedd advertising between them reads as matched, never as "present but not matching" --
// which is leaving the AP set and starts retirement.
func TestDiscoveryPresentBeforeMatched(t *testing.T) {
	sc := &seqCollector{after: []*classad.ClassAd{scheddAd(t, "ap1.example.org", "<10.0.0.1:9618>")}}
	snap, err := (&Discovery{Collector: sc, ScheddConstraint: `true`, Resolve: noResolve}).Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Present["ap1.example.org"] && !snap.Matched["ap1.example.org"] {
		t.Fatalf("a schedd advertising mid-pass reads as leaving the AP set: matched=%v present=%v", snap.Matched, snap.Present)
	}
}

// TestRemovedStaticSpokeRetires: a static spoke removed from the configuration (and not matched by
// the constraint) is leaving the AP set, even though the collector never listed it.
func TestRemovedStaticSpokeRetires(t *testing.T) {
	disc := &fakeDiscovery{}
	snap := members()
	snap.Matched["ap1"] = true
	snap.Spokes["ap1"] = Spoke{Schedd: "ap1", Address: "static-addr", Static: true}
	disc.set(snap)
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: unreachable})
	defer hub.stop(t)
	waitFor(t, "static ap1", func() bool { v, _ := sourceRowAttrBool(cat, "ap1", "Static"); return v })

	disc.set(members())
	hub.hub.Rediscover()
	waitFor(t, "ap1 retiring", func() bool { return sourceState(cat, "ap1") == StateRetiring })
}

// TestLeavingSourceStopsStreaming: a source leaving the AP set -- a static spoke removed from the
// configuration, or a schedd that stops matching the constraint -- retires and stops its streams.
func TestLeavingSourceStopsStreaming(t *testing.T) {
	for _, how := range []string{"static spoke removed", "constraint stops matching"} {
		t.Run(how, func(t *testing.T) {
			a := newTestSpoke(t)
			a.putJobs(1)
			disc := &fakeDiscovery{}
			snap := members("ap1")
			if how == "static spoke removed" {
				snap.Spokes["ap1"] = Spoke{Schedd: "ap1", Address: "static-addr", Static: true}
			}
			disc.set(snap)
			cat := openCatalog(t, t.TempDir())
			t.Cleanup(func() { _ = cat.Close() })
			hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: func(string) cedarsync.Dial { return a.dial },
				FlushInterval: 20 * time.Millisecond})
			defer hub.stop(t)
			waitFor(t, "streaming", func() bool { v, _ := sourceRowAttrBool(cat, "ap1", "JobsConnected"); return v })

			next := members()
			if how == "constraint stops matching" {
				next.Present["ap1"] = true // still advertising, no longer matching
			}
			disc.set(next)
			hub.hub.Rediscover()
			waitFor(t, "retiring", func() bool { return sourceState(cat, "ap1") == StateRetiring })
			waitFor(t, "streams stopped", func() bool {
				v, ok := sourceRowAttrBool(cat, "ap1", "JobsConnected")
				return ok && !v
			})
		})
	}
}

// TestUntrustedState: a matched schedd whose every claimant failed host validation, with no spoke
// validated before, is reported untrusted with the reason -- not absent.
func TestUntrustedState(t *testing.T) {
	disc := &fakeDiscovery{}
	snap := members("ap1")
	delete(snap.Spokes, "ap1")
	snap.Untrusted["ap1"] = "1 spoke(s) claim this schedd from another host"
	disc.set(snap)
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: unreachable})
	defer hub.stop(t)
	waitFor(t, "ap1 untrusted", func() bool { return sourceState(cat, "ap1") == StateUntrusted })
	row, _ := sourceRow(cat, "ap1")
	if r, _ := row.EvaluateAttrString("Reason"); r != snap.Untrusted["ap1"] {
		t.Errorf("Reason = %q", r)
	}
	if s := hub.hub.Summary(); s == nil || s.Untrusted != 1 {
		t.Errorf("summary = %+v", s)
	}
}
