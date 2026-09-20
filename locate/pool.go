package locate

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
)

// AdType is the MyType of the discovery ad the daemon advertises to the collector. It is
// dbad.AdType, spelled again here so locating a daemon does not drag in the advertising side
// (dbad pulls the whole sync engine with it).
const AdType = "HTCondorDB"

// maxPoolCandidates bounds the collector query. It is not a result limit in the usual sense:
// one ad is the answer and more than one is an error, so this only has to be large enough for
// the ambiguity message to list a realistic pool's databases rather than a truncated few.
const maxPoolCandidates = 64

// poolProjection is all Daemon needs from each ad: the address to return, and the two
// attributes the ambiguity message names a candidate by.
var poolProjection = []string{"Name", "Machine", "MyAddress"}

// PoolConstraint returns the ClassAd constraint selecting the htcondordb daemon that name
// refers to, or "" for "whichever one is out there" when name is empty.
//
// It accepts the same spellings condor_status -name does, because the daemon's Name is not
// simply its host: unless HTCONDORDB_NAME is set, a daemon names itself "htcondordb@<host>".
// A name with an "@" is matched whole, and also as its two halves against Name and Machine,
// so "db1@host" finds a daemon that set HTCONDORDB_NAME = db1. A name without one is matched
// against both Name and Machine, so a plain host finds the default-named daemon there.
func PoolConstraint(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	if local, host, ok := strings.Cut(name, "@"); ok && local != "" && host != "" {
		return fmt.Sprintf("Name == %s || (Name == %s && Machine == %s)",
			classad.Quote(name), classad.Quote(local), classad.Quote(host))
	}
	return fmt.Sprintf("Name == %s || Machine == %s", classad.Quote(name), classad.Quote(name))
}

// DaemonInPool asks a collector where an htcondordb daemon is, the way the HTCondor
// command-line tools do: pool is the collector to ask (empty means COLLECTOR_HOST from the
// configuration) and name is the daemon to ask for (empty means the pool's only one). It
// returns the daemon's command address, ready for cedar's ConnectAndAuthenticate.
//
// This is the cross-pool counterpart to Daemon, which resolves a daemon the caller is
// configured for. Nothing here reads the HTCONDORDB_ADDRESS_FILE / HTCONDORDB_HOST knobs: a
// caller that names a pool means that pool, and silently preferring a local address file
// would answer a different question than the one asked.
//
// An empty name is deliberately not "pick one": with several databases advertising, it errors
// and lists them. Picking silently would route a query at whichever database sorted first,
// and a wrong-database answer looks exactly like a right one.
func DaemonInPool(ctx context.Context, cfg *config.Config, pool, name string) (string, error) {
	pool = strings.TrimSpace(pool)
	if pool == "" {
		if v, ok := cfg.Get("COLLECTOR_HOST"); ok {
			pool = strings.TrimSpace(v)
		}
	}
	if pool == "" {
		return "", fmt.Errorf("cannot locate the htcondordb daemon: no collector to ask (name one, or set COLLECTOR_HOST)")
	}

	ads, _, err := htcondor.NewCollector(pool).QueryAdsWithOptions(ctx, AdType, PoolConstraint(name),
		&htcondor.QueryOptions{Limit: maxPoolCandidates, Projection: poolProjection})
	if err != nil {
		return "", fmt.Errorf("querying the collector at %s for %s ads: %w", pool, AdType, err)
	}
	return pickDaemon(ads, pool, strings.TrimSpace(name))
}

// pickDaemon turns the collector's answer into one command address, or explains why it cannot.
func pickDaemon(ads []*classad.ClassAd, pool, name string) (string, error) {
	switch {
	case len(ads) == 0 && name != "":
		return "", fmt.Errorf("no htcondordb daemon named %q is advertising to the collector at %s", name, pool)
	case len(ads) == 0:
		return "", fmt.Errorf("no htcondordb daemon is advertising to the collector at %s", pool)
	case len(ads) > 1 && name != "":
		return "", fmt.Errorf("%d htcondordb daemons match %q at the collector %s: %s",
			len(ads), name, pool, strings.Join(candidateNames(ads), ", "))
	case len(ads) > 1:
		return "", fmt.Errorf("%d htcondordb daemons are advertising to the collector at %s; name one: %s",
			len(ads), pool, strings.Join(candidateNames(ads), ", "))
	}

	addr, ok := ads[0].EvaluateAttrString("MyAddress")
	if !ok || strings.TrimSpace(addr) == "" {
		// The daemon advertised but its ad carries no address -- a truncated or foreign ad
		// under our MyType. Say so rather than dialing "".
		return "", fmt.Errorf("the %s ad for %s at the collector %s has no MyAddress",
			AdType, describeCandidate(ads[0]), pool)
	}
	return strings.TrimSpace(addr), nil
}

// candidateNames lists the matching daemons for an error message, sorted so the message is
// the same on every run (collector order is not stable).
func candidateNames(ads []*classad.ClassAd) []string {
	names := make([]string, 0, len(ads))
	for _, ad := range ads {
		names = append(names, describeCandidate(ad))
	}
	sort.Strings(names)
	return names
}

// describeCandidate names one ad the way the operator would pass it back as a name: its Name,
// falling back to its Machine and then its address for an ad missing both.
func describeCandidate(ad *classad.ClassAd) string {
	for _, attr := range []string{"Name", "Machine", "MyAddress"} {
		if v, ok := ad.EvaluateAttrString(attr); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return "(unnamed)"
}
