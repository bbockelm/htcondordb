package federate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// resolveMap is a resolver over a fixed table.
func resolveMap(m map[string][]string) func(context.Context, string) ([]string, error) {
	return func(_ context.Context, host string) ([]string, error) {
		if a, ok := m[strings.ToLower(host)]; ok {
			return a, nil
		}
		return nil, errors.New("nx")
	}
}

// TestHostValidationIgnoresSelfAssertedAddresses: a spoke's claim is checked against its primary
// address only. Its alias= and addrs= parameters are whatever the spoke chose to write into its own
// ad, so a spoke on another host could name the AP there; and a private address shared with the
// schedd (Docker's 172.17.0.2 behind CCB/NAT, say) is not one host.
func TestHostValidationIgnoresSelfAssertedAddresses(t *testing.T) {
	resolve := resolveMap(map[string][]string{"ap1.example.org": {"10.0.0.1"}})
	cases := []struct {
		name   string
		schedd *classad.ClassAd
		spoke  *classad.ClassAd
	}{
		{"alias names the AP",
			scheddAd(t, "ap1.example.org", "<10.0.0.1:9618>"),
			spokeAd(t, "evil", "<10.6.6.6:9620?alias=ap1.example.org>", "ap1.example.org", true, true)},
		{"addrs lists the AP's address",
			scheddAd(t, "ap1.example.org", "<10.0.0.1:9618>"),
			spokeAd(t, "evil", "<10.6.6.6:9620?addrs=10.0.0.1-9620>", "ap1.example.org", true, true)},
		{"shared private address",
			scheddAd(t, "ap1.example.org", "<172.17.0.2:9618?CCBID=192.0.2.1:9618%231&PrivNet=ap1.example.org>"),
			spokeAd(t, "db-on-ap2", "<172.17.0.2:9620?CCBID=192.0.2.1:9618%233&PrivNet=ap2.example.org>", "ap1.example.org", true, true)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fc := &fakeCollector{schedds: []*classad.ClassAd{c.schedd}, dbs: []*classad.ClassAd{c.spoke}}
			snap, err := (&Discovery{Collector: fc, ScheddConstraint: `true`, Resolve: resolve}).Discover(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if sp, ok := snap.Spokes["ap1.example.org"]; ok {
				t.Fatalf("spoke on another host paired with ap1: %+v", sp)
			}
			if len(snap.Rejected) != 1 || snap.Rejected[0].Reason != RejectHostMismatch {
				t.Errorf("rejected = %+v, want one host_mismatch", snap.Rejected)
			}
		})
	}
}

// TestHostValidationAcceptsPrimaryHost: the spoke's primary host passes when it is the claimed host
// by name, or an address the hub resolves that name to.
func TestHostValidationAcceptsPrimaryHost(t *testing.T) {
	resolve := resolveMap(map[string][]string{"ap1.example.org": {"2001:db8::1", "10.0.0.1"}})
	for _, addr := range []string{"<10.0.0.1:9620>", "<[2001:db8:0::1]:9620>", "<AP1.example.org:9620>"} {
		fc := &fakeCollector{
			schedds: []*classad.ClassAd{scheddAd(t, "ap1.example.org", "<192.0.2.9:9618>")},
			dbs:     []*classad.ClassAd{spokeAd(t, "db1", addr, "ap1.example.org", true, true)},
		}
		snap, err := (&Discovery{Collector: fc, ScheddConstraint: `true`, Resolve: resolve}).Discover(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if snap.Spokes["ap1.example.org"].Address != addr {
			t.Errorf("spoke at %s not paired: spokes=%+v rejected=%+v", addr, snap.Spokes, snap.Rejected)
		}
	}
}
