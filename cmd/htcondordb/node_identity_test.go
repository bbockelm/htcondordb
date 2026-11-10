package main

import "testing"

// A node's raft identity is written into the cluster configuration when it self-joins, so
// it must not move when the advertised address gains a parameter. These are the addresses
// golang-htcondor#896 made the daemon advertise -- carrying the certificate alias, the
// IPv6 address and noUDP where it used to carry none of them -- and every one of them has
// to still name the node what it registered under before that change.
func TestNodeIdentityIgnoresAddressParameters(t *testing.T) {
	const (
		sock   = "htcondordb_7808_378f"
		lnAddr = "128.105.68.113:9619"
	)
	want := "128.105.68.113:9618?sock=" + sock

	for _, tc := range []struct {
		name      string
		advertise string
	}{
		{"the address as it was advertised before the fix", "128.105.68.113:9618?sock=" + sock},
		{"with the certificate alias", "128.105.68.113:9618?sock=" + sock + "&alias=ap2001.chtc.wisc.edu"},
		{
			"with everything the shared-port server publishes",
			"128.105.68.113:9618?addrs=128.105.68.113-9618+[2607:f388::1]-9618&alias=ap2001.chtc.wisc.edu&noUDP&sock=" + sock,
		},
		{"bracketed", "<128.105.68.113:9618?alias=ap2001.chtc.wisc.edu&sock=" + sock + ">"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := nodeIdentityFrom(tc.advertise, sock, lnAddr); got != want {
				t.Errorf("nodeIdentityFrom(%q) = %q, want %q", tc.advertise, got, want)
			}
		})
	}
}

func TestNodeIdentityWithoutSharedPort(t *testing.T) {
	const lnAddr = "128.105.68.113:9619"
	if got := nodeIdentityFrom("128.105.68.113:9619?alias=ap2001.chtc.wisc.edu", "", lnAddr); got != lnAddr {
		t.Errorf("nodeIdentityFrom() = %q, want the listen address %q", got, lnAddr)
	}
}

// An address that cannot be parsed must not produce a half-built identity; the listen
// address is the honest answer.
func TestNodeIdentityFallsBackOnAnUnparseableAddress(t *testing.T) {
	const lnAddr = "128.105.68.113:9619"
	if got := nodeIdentityFrom("not-an-address", "sock_1", lnAddr); got != lnAddr {
		t.Errorf("nodeIdentityFrom() = %q, want the listen address %q", got, lnAddr)
	}
}

func TestNodeIdentityIPv6(t *testing.T) {
	const sock = "htcondordb_1_2"
	got := nodeIdentityFrom("[2607:f388::1]:9618?alias=ap2001.chtc.wisc.edu&sock="+sock, sock, "x")
	if want := "[2607:f388::1]:9618?sock=" + sock; got != want {
		t.Errorf("nodeIdentityFrom() = %q, want %q", got, want)
	}
}
