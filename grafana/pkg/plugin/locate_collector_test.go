package plugin

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
)

const dbAdType = "HTCondorDB"

// advertiseDB puts an htcondordb discovery ad into a real collector. seq must rise
// for an update to replace the previous ad rather than be dropped as stale.
func advertiseDB(t *testing.T, ctx context.Context, pool, name, machine, address string, seq int64) {
	t.Helper()
	ad := classad.New()
	ad.InsertAttrString("MyType", dbAdType)
	ad.InsertAttrString("Name", name)
	ad.InsertAttrString("Machine", machine)
	ad.InsertAttrString("MyAddress", address)
	ad.InsertAttr("UpdateSequenceNumber", seq)
	if err := htcondor.NewCollector(pool).Advertise(ctx, ad, nil); err != nil {
		t.Fatalf("advertising %s: %v", name, err)
	}
}

// waitForAddress polls until the datasource's pool lookup returns want, so the test
// does not race the collector's processing of an update.
func waitForAddress(t *testing.T, ctx context.Context, cc connConfig, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last string
	var lastErr error
	for time.Now().Before(deadline) {
		got, err := cc.resolveAddress(ctx, 10*time.Second)
		if err == nil && got == want {
			return
		}
		last, lastErr = got, err
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("pool lookup never returned %q (last: %q, err: %v)", want, last, lastErr)
}

// TestPoolLookupFollowsTheDaemonAcrossARestart is the whole point of the pool option.
// Behind shared port a daemon's sinful string carries its pid, so it changes on every
// restart. Configured by pool/name, the datasource must find the daemon again at its
// new address without anyone editing the datasource.
//
// It runs against a real condor_collector, so the constraint is evaluated by the
// collector rather than a local stand-in.
//
// Skips when condor_master is not installed (the harness decides).
func TestPoolLookupFollowsTheDaemonAcrossARestart(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the condor harness must run unprivileged")
	}
	h := htcondor.SetupCondorHarnessWithConfig(t, "")
	t.Setenv("CONDOR_CONFIG", h.GetConfigFile())

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool := h.GetCollectorAddr()

	const before = "<10.0.0.1:9618?sock=htcondordb_111_aaaa>"
	const after = "<10.0.0.1:9618?sock=htcondordb_222_bbbb>" // same host, new pid

	cc := connConfig{Pool: pool, Name: "htcondordb@db1.example.com"}

	advertiseDB(t, ctx, pool, "htcondordb@db1.example.com", "db1.example.com", before, 1)
	waitForAddress(t, ctx, cc, before)

	// The daemon restarts: same name, new sinful string.
	advertiseDB(t, ctx, pool, "htcondordb@db1.example.com", "db1.example.com", after, 2)
	waitForAddress(t, ctx, cc, after)
}

// TestPoolLookupRefusesToGuess: with several databases in a pool and no name, the
// lookup must error and list them rather than pick one. A wrong-database answer is
// indistinguishable from a right one, so silently choosing is worse than failing.
func TestPoolLookupRefusesToGuess(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the condor harness must run unprivileged")
	}
	h := htcondor.SetupCondorHarnessWithConfig(t, "")
	t.Setenv("CONDOR_CONFIG", h.GetConfigFile())

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool := h.GetCollectorAddr()

	advertiseDB(t, ctx, pool, "htcondordb@a.example.com", "a.example.com", "<10.0.0.1:9618?sock=a>", 1)
	advertiseDB(t, ctx, pool, "htcondordb@b.example.com", "b.example.com", "<10.0.0.2:9618?sock=b>", 1)

	// Named, it still resolves.
	named := connConfig{Pool: pool, Name: "a.example.com"}
	waitForAddress(t, ctx, named, "<10.0.0.1:9618?sock=a>")

	// Unnamed, it must refuse.
	unnamed := connConfig{Pool: pool}
	if addr, err := unnamed.resolveAddress(ctx, 10*time.Second); err == nil {
		t.Fatalf("ambiguous pool resolved to %q; it should have refused", addr)
	}
}
