package locate

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
)

// TestDaemonInPoolAgainstRealCollector runs the lookup over the wire against a real
// condor_collector: the constraint is evaluated by the collector, not by a local evaluator,
// and the ad goes in the way the daemon sends it (MyType HTCondorDB, hence UPDATE_AD_GENERIC).
// The unit tests can only prove that the constraint means what we think; this proves the
// collector agrees, which is where a wrong query type or an unindexed attribute would show up.
//
// Skips when condor_master is not installed (the harness decides).
func TestDaemonInPoolAgainstRealCollector(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the condor harness must run unprivileged")
	}
	h := htcondor.SetupCondorHarnessWithConfig(t, "")
	t.Setenv("CONDOR_CONFIG", h.GetConfigFile())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := h.GetCollectorAddr()
	cfg, err := h.GetConfig()
	if err != nil {
		t.Fatal(err)
	}

	// Two databases in one pool: a default-named one, and one an operator named. Between
	// them they cover every spelling PoolConstraint accepts.
	const addrDefault = "<10.0.0.1:9618?sock=db1>"
	const addrNamed = "<10.0.0.2:9618?sock=db2>"
	advertise(t, ctx, pool, "htcondordb@db1.example.com", "db1.example.com", addrDefault)
	advertise(t, ctx, pool, "archive", "db2.example.com", addrNamed)

	// The collector accepts the update and then processes it; give the query a few tries
	// rather than a fixed sleep.
	waitForAds(t, ctx, pool, 2)

	for _, tc := range []struct {
		name string
		want string
	}{
		{"db1.example.com", addrDefault},            // host alone finds htcondordb@host
		{"htcondordb@db1.example.com", addrDefault}, // the advertised name, verbatim
		{"db2.example.com", addrNamed},              // host alone finds an operator-named one
		{"archive", addrNamed},                      // its name alone
		{"archive@db2.example.com", addrNamed},      // name@host, both halves agreeing
	} {
		got, err := DaemonInPool(ctx, cfg, pool, tc.name)
		if err != nil {
			t.Errorf("-name %q: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("-name %q: got %q, want %q", tc.name, got, tc.want)
		}
	}

	// A name nothing advertises, and the ambiguity of naming nothing at all.
	if _, err := DaemonInPool(ctx, cfg, pool, "db3.example.com"); err == nil {
		t.Error("an unknown name should not resolve")
	}
	_, err = DaemonInPool(ctx, cfg, pool, "")
	if err == nil {
		t.Fatal("two databases in the pool and no name should be an error, not a silent pick")
	}
	for _, want := range []string{"archive", "htcondordb@db1.example.com"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the ambiguity error does not list %q: %v", want, err)
		}
	}
}

// advertise sends one HTCondorDB ad the way the daemon does: MyType drives the collector's
// choice of UPDATE_AD_GENERIC. No ack is requested -- the collector acknowledges only
// UPDATE_STARTD_AD -- so waitForAds is what makes the following query non-racy.
func advertise(t *testing.T, ctx context.Context, pool, name, machine, address string) {
	t.Helper()
	ad := classad.New()
	ad.InsertAttrString("MyType", AdType)
	ad.InsertAttrString("Name", name)
	ad.InsertAttrString("Machine", machine)
	ad.InsertAttrString("MyAddress", address)
	ad.InsertAttr("UpdateSequenceNumber", int64(1))
	if err := htcondor.NewCollector(pool).Advertise(ctx, ad, nil); err != nil {
		t.Fatalf("advertising %s: %v", name, err)
	}
}

// waitForAds polls until the collector reports at least n HTCondorDB ads.
func waitForAds(t *testing.T, ctx context.Context, pool string, n int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ads, _, err := htcondor.NewCollector(pool).QueryAdsWithOptions(ctx, AdType, "",
			&htcondor.QueryOptions{Limit: maxPoolCandidates})
		if err == nil && len(ads) >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("collector never reported %d %s ads (last error: %v)", n, AdType, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
