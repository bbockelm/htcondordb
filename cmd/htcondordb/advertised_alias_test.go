package main

import (
	"strings"
	"testing"

	"github.com/bbockelm/cedar/addresses"
)

// TestWithAlias pins the alias the daemon attaches to its advertised address. Behind shared
// port the address host is the shared-port server's IP, and a host certificate carries DNS
// names and no IP SANs -- so without an alias every remote SSL authentication fails the
// certificate check (issue #292).
func TestWithAlias(t *testing.T) {
	const fqdn = "ap2001.chtc.wisc.edu"
	base := "FULL_HOSTNAME = " + fqdn + "\n"

	for _, tc := range []struct {
		name string
		cfg  string
		addr string
		want string
	}{
		{
			name: "shared-port sinful behind an IP gets the fqdn",
			cfg:  base,
			addr: "128.105.68.113:9618?sock=htcondordb_7808_378f",
			want: "128.105.68.113:9618?sock=htcondordb_7808_378f&alias=" + fqdn,
		},
		{
			name: "a bare listen address starts the query string",
			cfg:  base,
			addr: "128.105.68.113:9618",
			want: "128.105.68.113:9618?alias=" + fqdn,
		},
		{
			name: "an IPv6 literal address is aliased",
			cfg:  base,
			addr: "[2607:f388::1]:9618?sock=htcondordb_1_2",
			want: "[2607:f388::1]:9618?sock=htcondordb_1_2&alias=" + fqdn,
		},
		{
			name: "angle brackets are stripped, not doubled",
			cfg:  base,
			addr: "<128.105.68.113:9618?sock=db>",
			want: "128.105.68.113:9618?sock=db&alias=" + fqdn,
		},
		{
			// C++ attaches the alias whatever the host looks like (Sock::get_sinful), and
			// a cert served under a name that is not the address host is exactly the case
			// the alias exists for.
			name: "a hostname address is aliased too",
			cfg:  base,
			addr: "ap2001.chtc.wisc.edu:9618?sock=db",
			want: "ap2001.chtc.wisc.edu:9618?sock=db&alias=" + fqdn,
		},
		{
			name: "an explicit HOST_ALIAS wins over FULL_HOSTNAME",
			cfg:  base + "HOST_ALIAS = lb.chtc.wisc.edu\n",
			addr: "128.105.68.113:9618?sock=db",
			want: "128.105.68.113:9618?sock=db&alias=lb.chtc.wisc.edu",
		},
		{
			name: "an alias already on the address is kept",
			cfg:  base,
			addr: "128.105.68.113:9618?sock=db&alias=other.example",
			want: "128.105.68.113:9618?sock=db&alias=other.example",
		},
		{
			// Writing it back would produce an address no client could parse; the startup
			// log shows the advertised address, so a dropped alias is visible there.
			name: "a HOST_ALIAS that is not a hostname is dropped",
			cfg:  base + "HOST_ALIAS = who?me=yes\n",
			addr: "128.105.68.113:9618?sock=db",
			want: "128.105.68.113:9618?sock=db",
		},
		{
			name: "no name to offer leaves the address as it was",
			cfg:  "HOST_ALIAS =\n",
			addr: "128.105.68.113:9618?sock=db",
			want: "128.105.68.113:9618?sock=db",
		},
		{
			name: "an empty address stays empty",
			cfg:  base,
			addr: "",
			want: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := withAlias(tc.addr, importerCfg(t, tc.cfg)); got != tc.want {
				t.Errorf("withAlias(%q) = %q, want %q", tc.addr, got, tc.want)
			}
		})
	}

	t.Run("a nil config changes nothing", func(t *testing.T) {
		const addr = "128.105.68.113:9618?sock=db"
		if got := withAlias(addr, nil); got != addr {
			t.Errorf("withAlias(%q, nil) = %q", addr, got)
		}
	})
}

// TestWithAliasKeepsTheRestOfTheAddress checks that appending the alias leaves every other
// sinful parameter readable -- the address is edited textually, so a broken edit would show
// up as a lost sock=, addrs= or ccbid=.
func TestWithAliasKeepsTheRestOfTheAddress(t *testing.T) {
	const addr = "128.105.68.113:9618?addrs=128.105.68.113-9618+[2607:f388::1]-9618&noUDP&sock=htcondordb_7808_378f"
	got := withAlias(addr, importerCfg(t, "FULL_HOSTNAME = ap2001.chtc.wisc.edu\n"))

	info, err := addresses.ParseSinful(got)
	if err != nil {
		t.Fatalf("the alias made the address unparseable: %v (%q)", err, got)
	}
	if info.Alias != "ap2001.chtc.wisc.edu" {
		t.Errorf("alias = %q, want ap2001.chtc.wisc.edu", info.Alias)
	}
	if info.SharedPortID != "htcondordb_7808_378f" {
		t.Errorf("sock = %q, want htcondordb_7808_378f", info.SharedPortID)
	}
	if !info.NoUDP {
		t.Error("noUDP was lost")
	}
	if len(info.Addrs) != 2 {
		t.Errorf("addrs = %v, want 2 entries", info.Addrs)
	}
	if info.PrimaryAddr != "128.105.68.113:9618" {
		t.Errorf("primary = %q, want 128.105.68.113:9618", info.PrimaryAddr)
	}
	if !strings.HasPrefix(got, addr) {
		t.Errorf("the original address was rewritten rather than appended to: %q", got)
	}
}
