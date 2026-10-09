package federate

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
)

// Spoke is a validated pairing: the htcondordb at Address mirrors the schedd named Schedd.
type Spoke struct {
	Schedd    string
	Address   string
	SpokeName string // the spoke's own daemon Name, for reporting
	// Static marks a spoke the admin configured (HTCONDORDB_FEDERATE_SPOKES). It skips host
	// validation: the admin asserted the pairing.
	Static bool
}

// Rejection is a spoke ad that was not paired, and why.
type Rejection struct {
	Schedd       string // the schedd the spoke claimed
	SpokeName    string
	SpokeAddress string
	Reason       string // RejectHostMismatch | RejectHATie
	Detail       string
}

// Rejection reasons (also the rejected_spokes_total label).
const (
	RejectHostMismatch = "host_mismatch"
	RejectHATie        = "ha_tie"
)

// Snapshot is one discovery pass's view of the AP set. The Known flags say which parts could be
// determined: a part that could not (the collector was unreachable) must not be read as empty --
// "absent from the answer" is not "absent from the pool" when there was no answer. Static spokes
// are configuration, not collector data: they are in Matched and Spokes (Static set) whatever the
// flags say.
type Snapshot struct {
	// Matched are the schedds in the AP set (constraint matches plus static spokes).
	Matched    map[string]bool
	MatchKnown bool
	// Present are the schedds advertising to the collector at all, matching or not. A member that
	// is present but no longer matches is leaving the set (retiring), not merely absent.
	Present      map[string]bool
	PresentKnown bool
	// Spokes are the accepted pairings, one per schedd.
	Spokes      map[string]Spoke
	SpokesKnown bool
	// Rejected lists spoke ads that were not paired.
	Rejected []Rejection
	// Untrusted are matched schedds every one of whose claimants failed host validation.
	Untrusted map[string]string
	// Declined are matched schedds with several valid claimants none of which could be preferred.
	Declined map[string]string
}

// Discoverer finds the spokes of the AP set.
type Discoverer interface {
	Discover(ctx context.Context) (Snapshot, error)
	// Constraint is the AP set as a ScheddAd constraint, for the hub's collector ad.
	Constraint() string
}

// RestoreValidator is implemented by a Discoverer that can re-check a persisted pairing without the
// collector. A hub restarting resumes streaming from a persisted spoke address only when it passes;
// otherwise it waits for discovery to pair the schedd again.
type RestoreValidator interface {
	// ValidateRestored reports whether the spoke at address may stream as schedd, and why not.
	ValidateRestored(ctx context.Context, schedd, address string) (reason string, ok bool)
}

// CollectorQuerier queries a collector. Behind an interface so discovery is testable with fakes.
type CollectorQuerier interface {
	Query(ctx context.Context, adType, constraint string, projection []string) ([]*classad.ClassAd, error)
}

// PoolCollector queries the pool collector at Address with the process's ambient HTCondor
// security configuration, like every other pool client here.
type PoolCollector struct{ Address string }

// Query returns every matching ad (no result cap), projected.
func (p PoolCollector) Query(ctx context.Context, adType, constraint string, projection []string) ([]*classad.ClassAd, error) {
	ads, _, err := htcondor.NewCollector(p.Address).QueryAdsWithOptions(ctx, adType, constraint,
		&htcondor.QueryOptions{Limit: -1, Projection: projection})
	return ads, err
}

// Discovery pairs the schedds matching Constraint with the HTCondorDB ads that claim to mirror
// them, plus the admin's static spokes. A zero Constraint means static spokes only.
type Discovery struct {
	Collector        CollectorQuerier // required when ScheddConstraint is set
	ScheddConstraint string
	Static           []Spoke
	// Resolve looks up a host name's addresses for host validation. Nil means net.DefaultResolver.
	Resolve func(ctx context.Context, host string) ([]string, error)
}

// Constraint returns the AP set as a ScheddAd constraint: the configured constraint, with each
// static spoke's schedd OR'd in by name.
func (d *Discovery) Constraint() string {
	var terms []string
	if c := strings.TrimSpace(d.ScheddConstraint); c != "" {
		terms = append(terms, "("+c+")")
	}
	for _, s := range d.Static {
		terms = append(terms, "Name == "+stringLit(s.Schedd))
	}
	return strings.Join(terms, " || ")
}

var (
	scheddProjection = []string{"Name", "MyAddress"}
	spokeProjection  = []string{"Name", "MyAddress", "MirroredScheddName", "Syncing", "JobQueueCaughtUp", "HistoryCaughtUp"}
)

// Discover runs one pass. It returns an error only when nothing at all could be learned; partial
// failures leave the corresponding Known flag false.
func (d *Discovery) Discover(ctx context.Context) (Snapshot, error) {
	snap := Snapshot{
		Matched: map[string]bool{}, Present: map[string]bool{}, Spokes: map[string]Spoke{},
		Untrusted: map[string]string{}, Declined: map[string]string{},
		MatchKnown: true, PresentKnown: true, SpokesKnown: true,
	}
	var firstErr error
	if strings.TrimSpace(d.ScheddConstraint) != "" {
		if d.Collector == nil {
			return snap, fmt.Errorf("federate: a schedd constraint needs a collector")
		}
		// Present first: a schedd that starts advertising between the two queries then reads as
		// matched (every matched schedd is present), never as present-but-not-matching -- which
		// is leaving the AP set, and starts retirement.
		present, perr := d.Collector.Query(ctx, "Schedd", "", []string{"Name"})
		if perr != nil {
			snap.PresentKnown = false
		} else {
			for _, ad := range present {
				if n, _ := ad.EvaluateAttrString("Name"); n != "" {
					snap.Present[n] = true
				}
			}
		}
		schedds, err := d.Collector.Query(ctx, "Schedd", d.ScheddConstraint, scheddProjection)
		if err != nil {
			snap.MatchKnown, snap.SpokesKnown, firstErr = false, false, fmt.Errorf("querying schedd ads: %w", err)
		}
		if err == nil {
			var spokes []*classad.ClassAd
			spokes, err = d.Collector.Query(ctx, "HTCondorDB", "MirroredScheddName =!= undefined", spokeProjection)
			if err != nil {
				snap.SpokesKnown = false
				firstErr = fmt.Errorf("querying HTCondorDB ads: %w", err)
			}
			infos := scheddInfos(schedds)
			for _, si := range infos {
				snap.Matched[si.name] = true
				snap.Present[si.name] = true
			}
			accepted, rejected, untrusted, declined := pair(ctx, infos, spokeInfos(spokes), d.resolver())
			snap.Spokes, snap.Rejected, snap.Untrusted, snap.Declined = accepted, rejected, untrusted, declined
		}
	}
	for _, s := range d.Static {
		s.Static = true
		snap.Matched[s.Schedd] = true
		snap.Present[s.Schedd] = true
		snap.Spokes[s.Schedd] = s
		delete(snap.Untrusted, s.Schedd)
		delete(snap.Declined, s.Schedd)
	}
	if !snap.MatchKnown && len(d.Static) == 0 {
		return snap, firstErr
	}
	return snap, nil
}

// ValidateRestored accepts a persisted pairing that is a configured static spoke, or that passes the
// same host validation a discovered spoke does. The persisted Static flag is not consulted: the
// configuration decides what is static.
func (d *Discovery) ValidateRestored(ctx context.Context, schedd, address string) (string, bool) {
	for _, s := range d.Static {
		if strings.EqualFold(s.Schedd, schedd) {
			if s.Address == address {
				return "", true
			}
			return fmt.Sprintf("the configured static spoke for this schedd is %s, not %s", s.Address, address), false
		}
	}
	if strings.TrimSpace(d.ScheddConstraint) == "" {
		return "not a configured static spoke, and there is no constraint to discover it by", false
	}
	return hostValid(ctx, spokeInfo{address: address}, scheddInfo{name: schedd}, d.resolver())
}

func (d *Discovery) resolver() func(context.Context, string) ([]string, error) {
	if d.Resolve != nil {
		return d.Resolve
	}
	return net.DefaultResolver.LookupHost
}

// scheddInfo / spokeInfo are the projected collector ads.
type scheddInfo struct{ name, address string }

type spokeInfo struct {
	name, address, mirrored string
	syncing, caughtUp       bool
}

func scheddInfos(ads []*classad.ClassAd) map[string]scheddInfo {
	out := map[string]scheddInfo{}
	for _, ad := range ads {
		n, _ := ad.EvaluateAttrString("Name")
		if n == "" {
			continue
		}
		a, _ := ad.EvaluateAttrString("MyAddress")
		out[foldName(n)] = scheddInfo{name: n, address: a} // names are case-insensitive
	}
	return out
}

func spokeInfos(ads []*classad.ClassAd) []spokeInfo {
	var out []spokeInfo
	for _, ad := range ads {
		var si spokeInfo
		si.name, _ = ad.EvaluateAttrString("Name")
		si.address, _ = ad.EvaluateAttrString("MyAddress")
		si.mirrored, _ = ad.EvaluateAttrString("MirroredScheddName")
		si.syncing, _ = ad.EvaluateAttrBool("Syncing")
		jq, jqOK := ad.EvaluateAttrBool("JobQueueCaughtUp")
		hist, histOK := ad.EvaluateAttrBool("HistoryCaughtUp")
		// Caught up means every source it reports is; a spoke reporting none is not.
		si.caughtUp = (jqOK || histOK) && (!jqOK || jq) && (!histOK || hist)
		if si.mirrored == "" || si.address == "" {
			continue
		}
		out = append(out, si)
	}
	return out
}

// pair validates each spoke's claim and picks one spoke per matched schedd.
//
// A claim is accepted only when the spoke's primary address is the claimed schedd's host (see
// hostValid). This is a guard against misconfiguration -- a spoke on one host claiming another's
// schedd would publish rows under that AP's name -- not authentication: every input comes from
// collector ads, which their advertisers write. Two valid claimants for one schedd (an HA pair) are
// resolved by preferring the one that is syncing and caught up; on a tie the schedd is declined
// rather than guessed.
func pair(ctx context.Context, schedds map[string]scheddInfo, spokes []spokeInfo, resolve func(context.Context, string) ([]string, error)) (accepted map[string]Spoke, rejected []Rejection, untrusted, declined map[string]string) {
	accepted, untrusted, declined = map[string]Spoke{}, map[string]string{}, map[string]string{}
	valid := map[string][]spokeInfo{}
	mismatched := map[string]int{}
	for _, sp := range spokes {
		sd, ok := schedds[foldName(sp.mirrored)]
		if !ok {
			continue // claims a schedd outside the AP set
		}
		if why, ok := hostValid(ctx, sp, sd, resolve); !ok {
			rejected = append(rejected, Rejection{Schedd: sd.name, SpokeName: sp.name, SpokeAddress: sp.address,
				Reason: RejectHostMismatch, Detail: why})
			mismatched[sd.name]++
			continue
		}
		valid[sd.name] = append(valid[sd.name], sp)
	}
	for schedd, n := range mismatched {
		if len(valid[schedd]) == 0 {
			untrusted[schedd] = fmt.Sprintf("%d spoke(s) claim this schedd from another host", n)
		}
	}
	for schedd, cands := range valid {
		pick := cands
		if len(cands) > 1 {
			pick = nil
			for _, c := range cands {
				if c.syncing && c.caughtUp {
					pick = append(pick, c)
				}
			}
		}
		if len(pick) != 1 {
			names := make([]string, 0, len(cands))
			for _, c := range cands {
				names = append(names, c.name+" "+c.address)
				rejected = append(rejected, Rejection{Schedd: schedd, SpokeName: c.name, SpokeAddress: c.address,
					Reason: RejectHATie, Detail: fmt.Sprintf("%d spokes claim this schedd and %d are syncing and caught up", len(cands), len(pick))})
			}
			sort.Strings(names)
			declined[schedd] = "several spokes claim this schedd and none can be preferred: " + strings.Join(names, ", ")
			continue
		}
		accepted[schedd] = Spoke{Schedd: schedd, Address: pick[0].address, SpokeName: pick[0].name}
	}
	return accepted, rejected, untrusted, declined
}

// hostValid checks a spoke's claim to mirror schedd sd: the host of the spoke's PRIMARY address
// (the host:port of its sinful string) must be the schedd's host -- the host part of its Name -- by
// name, or be an address the hub's resolver returns for that name. It returns a description of the
// mismatch when the claim fails.
//
// Nothing else counts. A sinful string's alias= and addrs= parameters are written by the spoke
// itself, so a spoke anywhere could name the AP there; and an address the spoke shares with the
// schedd's advertised one proves nothing when it is private (two APs behind different NATs or CCB
// can both be 172.17.0.2). A spoke that cannot pass -- one reached only through CCB or NAT, or whose
// primary address is not what its AP's name resolves to -- is paired statically
// (HTCONDORDB_FEDERATE_SPOKES).
func hostValid(ctx context.Context, sp spokeInfo, sd scheddInfo, resolve func(context.Context, string) ([]string, error)) (string, bool) {
	claimed := strings.ToLower(hostPart(sd.name))
	primary := primaryHost(sp.address)
	if primary == "" {
		return fmt.Sprintf("spoke address %q has no host", sp.address), false
	}
	if claimed != "" && primary == claimed {
		return "", true
	}
	ip := net.ParseIP(primary)
	if ip != nil && resolve != nil && claimed != "" {
		if addrs, err := resolve(ctx, claimed); err == nil {
			for _, a := range addrs {
				if other := net.ParseIP(a); other != nil && other.Equal(ip) {
					return "", true
				}
			}
		}
	}
	return fmt.Sprintf("the spoke's primary address host %q is not %q or an address it resolves to", primary, claimed), false
}

// hostPart is HTCondor's get_host_part: what follows the last '@', or the whole name.
func hostPart(name string) string {
	if i := strings.LastIndexByte(name, '@'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// primaryHost returns the host of a sinful string's primary address, lowercased: "<10.0.0.1:9618?...>"
// is "10.0.0.1", "<[2001:db8::1]:9618>" is "2001:db8::1". Its parameters are ignored.
func primaryHost(sinful string) string {
	s := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(sinful), "<"), ">")
	hostport, _, _ := strings.Cut(s, "?")
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return strings.ToLower(h)
	}
	return strings.ToLower(strings.Trim(hostport, "[]"))
}
