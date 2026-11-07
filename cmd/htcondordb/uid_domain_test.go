package main

import (
	"os"
	"strings"
	"testing"

	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/htcondordb/command"
)

func uidDomainSecurityConfig(t *testing.T, body string) (*config.Config, string) {
	t.Helper()
	cfg, err := config.NewFromReaderWithOptions(strings.NewReader(body), config.ConfigOptions{Subsystem: "HTCONDORDB"})
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	// Built exactly as run() builds it, so the test fails for the same reason the
	// daemon would.
	sec, err := htcondor.GetServerSecurityConfig(cfg, command.DBSession, "DAEMON")
	if err != nil {
		t.Fatalf("GetServerSecurityConfig: %v", err)
	}
	return cfg, sec.UIDDomain
}

// An FS peer has no domain of its own -- its identity is the owner of a marker file --
// so the security layer qualifies the bare name, and the resulting "user@domain" is the
// whole string the ALLOW_<PERM> tables are matched against. UID_DOMAIN therefore has to
// reach the security config. Left unset there, the security layer supplies this host's
// own name instead: the peer authenticates as bbockelm@ap2001 rather than
// bbockelm@chtc.wisc.edu, matches no rule written for the pool's UID domain, and loses
// every authorization level including READ. The qualifying happens before srv.FQUMapper
// runs, and the mapper leaves an already-qualified name alone, so it cannot repair this.
func TestSecurityConfigCarriesUIDDomain(t *testing.T) {
	_, dom := uidDomainSecurityConfig(t, "UID_DOMAIN = chtc.wisc.edu\n")

	if dom != "chtc.wisc.edu" {
		t.Fatalf("UIDDomain = %q, want %q", dom, "chtc.wisc.edu")
	}
	if host, _ := os.Hostname(); host != "" && dom == host {
		t.Fatalf("UIDDomain fell back to this host's name %q instead of UID_DOMAIN", host)
	}
}

// An unconfigured pool is not left to the security layer's fallback either. UID_DOMAIN
// carries HTCondor's own $(FULL_HOSTNAME) default, which is the fully-qualified name the
// C++ daemons qualify with -- not whatever short name this machine answers to.
func TestSecurityConfigUIDDomainDefaultsToFullHostname(t *testing.T) {
	cfg, dom := uidDomainSecurityConfig(t, "# no UID_DOMAIN\n")

	want, ok := cfg.Get("UID_DOMAIN")
	if !ok || want == "" {
		t.Fatalf("UID_DOMAIN resolved to %q (ok=%v), want HTCondor's $(FULL_HOSTNAME) default", want, ok)
	}
	if dom != want {
		t.Fatalf("UIDDomain = %q, want the resolved default %q", dom, want)
	}
}
