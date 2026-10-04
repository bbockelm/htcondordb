package federate

import (
	"context"
	"fmt"
	"net"
	"net/url"
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
// "absent from the answer" is not "absent from the pool" when there was no answer.
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
		schedds, err := d.Collector.Query(ctx, "Schedd", d.ScheddConstraint, scheddProjection)
		if err != nil {
			snap.MatchKnown, snap.SpokesKnown, firstErr = false, false, fmt.Errorf("querying schedd ads: %w", err)
		}
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
		if err == nil {
			var spokes []*classad.ClassAd
			spokes, err = d.Collector.Query(ctx, "HTCondorDB", "MirroredScheddName =!= undefined", spokeProjection)
			if err != nil {
				snap.SpokesKnown = false
				firstErr = fmt.Errorf("querying HTCondorDB ads: %w", err)
			}
			infos := scheddInfos(schedds)
			for n := range infos {
				snap.Matched[n] = true
				snap.Present[n] = true
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
		out[n] = scheddInfo{name: n, address: a}
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
// A claim is accepted only when the host in MirroredScheddName (the part after '@', or the whole
// name) is where the spoke itself runs -- its address's host or alias, or an address that host
// name resolves to -- or when the spoke shares a host with the schedd's own advertised address.
// Without that check a misconfigured or hostile spoke could publish rows under another AP's name.
// Two valid claimants for one schedd (an HA pair) are resolved by preferring the one that is
// syncing and caught up; on a tie the schedd is declined rather than guessed.
func pair(ctx context.Context, schedds map[string]scheddInfo, spokes []spokeInfo, resolve func(context.Context, string) ([]string, error)) (accepted map[string]Spoke, rejected []Rejection, untrusted, declined map[string]string) {
	accepted, untrusted, declined = map[string]Spoke{}, map[string]string{}, map[string]string{}
	valid := map[string][]spokeInfo{}
	mismatched := map[string]int{}
	for _, sp := range spokes {
		sd, ok := schedds[sp.mirrored]
		if !ok {
			continue // claims a schedd outside the AP set
		}
		if why, ok := hostValid(ctx, sp, sd, resolve); !ok {
			rejected = append(rejected, Rejection{Schedd: sp.mirrored, SpokeName: sp.name, SpokeAddress: sp.address,
				Reason: RejectHostMismatch, Detail: why})
			mismatched[sp.mirrored]++
			continue
		}
		valid[sp.mirrored] = append(valid[sp.mirrored], sp)
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

// hostValid checks a spoke's claim to mirror schedd sd. It returns a description of the mismatch
// when the claim fails.
func hostValid(ctx context.Context, sp spokeInfo, sd scheddInfo, resolve func(context.Context, string) ([]string, error)) (string, bool) {
	claimed := strings.ToLower(hostPart(sp.mirrored))
	spokeHosts := sinfulHosts(sp.address)
	if spokeHosts[claimed] {
		return "", true
	}
	for h := range sinfulHosts(sd.address) {
		if spokeHosts[h] {
			return "", true
		}
	}
	if resolve != nil && claimed != "" {
		if addrs, err := resolve(ctx, claimed); err == nil {
			for _, a := range addrs {
				if spokeHosts[strings.ToLower(a)] {
					return "", true
				}
			}
		}
	}
	hosts := make([]string, 0, len(spokeHosts))
	for h := range spokeHosts {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)
	return fmt.Sprintf("MirroredScheddName host %q is not the spoke's host (%s)", claimed, strings.Join(hosts, ", ")), false
}

// hostPart is HTCondor's get_host_part: what follows the last '@', or the whole name.
func hostPart(name string) string {
	if i := strings.LastIndexByte(name, '@'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// sinfulHosts returns the hosts a sinful string names, lowercased: its primary host, its alias
// parameter, and every address in its addrs parameter.
func sinfulHosts(sinful string) map[string]bool {
	out := map[string]bool{}
	s := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(sinful), "<"), ">")
	if s == "" {
		return out
	}
	hostport, query, _ := strings.Cut(s, "?")
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		out[strings.ToLower(h)] = true
	} else if hostport != "" {
		out[strings.ToLower(hostport)] = true
	}
	if q, err := url.ParseQuery(query); err == nil {
		if a := q.Get("alias"); a != "" {
			out[strings.ToLower(a)] = true
		}
		// '+' separates addrs entries; ParseQuery has already decoded it to a space.
		for _, a := range strings.FieldsFunc(q.Get("addrs"), func(r rune) bool { return r == '+' || r == ' ' }) {
			// "10.0.0.1-9618" or "[::1]-9618"
			if i := strings.LastIndexByte(a, '-'); i > 0 {
				out[strings.ToLower(strings.Trim(a[:i], "[]"))] = true
			}
		}
	}
	return out
}
