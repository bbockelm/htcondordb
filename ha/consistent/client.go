package consistent

import (
	"context"
	"fmt"

	"github.com/PelicanPlatform/classad/classad"
)

// Exchange performs one control request/response round trip against the daemon
// at addr. The CLI supplies a CEDAR implementation (dial addr on the DBControl
// command, send the request ad, read the response ad); tests supply an in-memory
// one. Keeping the transport behind this function keeps the redirect-following
// client testable without a live network.
type Exchange func(ctx context.Context, addr string, req *classad.ClassAd) (*classad.ClassAd, error)

// ControlClient submits control operations to a consistent-mode cluster,
// following leader redirects. A write addressed to a follower comes back with
// Redirect + LeaderAddress; the client retries against the leader, so callers get
// transparent leader routing.
type ControlClient struct {
	addr         string
	exchange     Exchange
	maxRedirects int
}

// NewControlClient targets the daemon at addr, using exchange for transport.
func NewControlClient(addr string, exchange Exchange) *ControlClient {
	return &ControlClient{addr: addr, exchange: exchange, maxRedirects: 5}
}

// Leader returns the cluster's current leader address and id.
func (cc *ControlClient) Leader(ctx context.Context) (addr, id string, err error) {
	resp, err := cc.exchange(ctx, cc.addr, BuildLeaderRequest())
	if err != nil {
		return "", "", err
	}
	if ok, _ := resp.EvaluateAttr(AttrResult).BoolValue(); !ok {
		return "", "", fmt.Errorf("consistent: no leader known")
	}
	return attrString(resp, AttrLeaderAddr), attrString(resp, AttrLeaderID), nil
}

// Apply submits a write batch, transparently retrying against the leader if the
// contacted node redirects. It returns nil on a quorum-committed success.
func (cc *ControlClient) Apply(ctx context.Context, b *Batch) error {
	req, err := BuildApplyRequest(b)
	if err != nil {
		return err
	}
	return cc.doWithRedirect(ctx, req)
}

// Register asks the cluster to admit a peer as a raft voter (leader-routed).
func (cc *ControlClient) Register(ctx context.Context, id, addr string) error {
	return cc.doWithRedirect(ctx, BuildRegisterRequest(id, addr))
}

// doWithRedirect runs req against the current target, following Redirect
// responses to the advertised leader up to maxRedirects times.
func (cc *ControlClient) doWithRedirect(ctx context.Context, req *classad.ClassAd) error {
	addr := cc.addr
	for i := 0; i <= cc.maxRedirects; i++ {
		resp, err := cc.exchange(ctx, addr, req)
		if err != nil {
			return err
		}
		if ok, _ := resp.EvaluateAttr(AttrResult).BoolValue(); ok {
			return nil
		}
		if redirect, _ := resp.EvaluateAttr(AttrRedirect).BoolValue(); redirect {
			next := attrString(resp, AttrLeaderAddr)
			if next == "" || next == addr {
				return fmt.Errorf("consistent: redirected but no leader address available")
			}
			addr = next
			continue
		}
		return fmt.Errorf("consistent: %s", attrString(resp, AttrErrorString))
	}
	return fmt.Errorf("consistent: too many leader redirects")
}
