package consistent

import (
	"context"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// TestControlClientRedirect verifies the client transparently follows a
// follower's redirect to the leader. A fake two-node cluster: "follower" answers
// every apply with a redirect to "leader"; "leader" accepts.
func TestControlClientRedirect(t *testing.T) {
	const leaderAddr = "leader:9618"
	var leaderCalls, followerCalls int

	exchange := func(_ context.Context, addr string, req *classad.ClassAd) (*classad.ClassAd, error) {
		resp := classad.New()
		switch addr {
		case leaderAddr:
			leaderCalls++
			resp.InsertAttrBool(AttrResult, true)
		default: // the follower
			followerCalls++
			resp.InsertAttrBool(AttrResult, false)
			resp.InsertAttrBool(AttrRedirect, true)
			resp.InsertAttrString(AttrLeaderAddr, leaderAddr)
		}
		return resp, nil
	}

	cc := NewControlClient("follower:9618", exchange)
	if err := cc.Apply(context.Background(), NewBatch().NewClassAd("1.0", "Owner = \"alice\"")); err != nil {
		t.Fatalf("Apply should succeed after redirect: %v", err)
	}
	if followerCalls != 1 || leaderCalls != 1 {
		t.Fatalf("calls: follower=%d leader=%d, want 1 and 1", followerCalls, leaderCalls)
	}
}

// TestControlClientError surfaces a non-redirect failure.
func TestControlClientError(t *testing.T) {
	exchange := func(_ context.Context, _ string, _ *classad.ClassAd) (*classad.ClassAd, error) {
		resp := classad.New()
		resp.InsertAttrBool(AttrResult, false)
		resp.InsertAttrString(AttrErrorString, "boom")
		return resp, nil
	}
	cc := NewControlClient("x:1", exchange)
	err := cc.Apply(context.Background(), NewBatch().DestroyClassAd("1.0"))
	if err == nil || err.Error() != "consistent: boom" {
		t.Fatalf("err = %v, want consistent: boom", err)
	}
}
