package consistent

import (
	"errors"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/hashicorp/raft"
)

// The consistent-mode control protocol is a single ClassAd request/response
// exchange (matching the daemon convention of ClassAd-in/ClassAd-out commands).
// It carries three operations, selected by the ControlOp attribute:
//
//   - "leader":   discover the current leader (for client redirect).
//   - "register": a DAEMON peer asks the leader to admit it as a raft voter
//     (the "first N hosts" bootstrap).
//   - "apply":    submit an encoded write Batch for quorum commit.
//
// Every response carries Result (bool). A write/register sent to a non-leader
// sets Redirect=true and LeaderAddress so the client retries against the leader.
const (
	AttrControlOp   = "ControlOp"
	AttrResult      = "Result"
	AttrErrorString = "ErrorString"
	AttrRedirect    = "Redirect"
	AttrLeaderAddr  = "LeaderAddress"
	AttrLeaderID    = "LeaderID"
	AttrPeerID      = "PeerID"
	AttrPeerAddress = "PeerAddress"
	AttrBatch       = "Batch" // JSON-encoded Batch, as a string

	OpLeader   = "leader"
	OpRegister = "register"
	OpApply    = "apply"
)

// BuildLeaderRequest builds a leader-discovery request.
func BuildLeaderRequest() *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString(AttrControlOp, OpLeader)
	return ad
}

// BuildRegisterRequest builds a peer-registration request.
func BuildRegisterRequest(id, addr string) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString(AttrControlOp, OpRegister)
	ad.InsertAttrString(AttrPeerID, id)
	ad.InsertAttrString(AttrPeerAddress, addr)
	return ad
}

// BuildApplyRequest builds a write-batch submission request.
func BuildApplyRequest(b *Batch) (*classad.ClassAd, error) {
	data, err := b.Encode()
	if err != nil {
		return nil, err
	}
	ad := classad.New()
	ad.InsertAttrString(AttrControlOp, OpApply)
	ad.InsertAttrString(AttrBatch, string(data))
	return ad, nil
}

// HandleControl processes one control request and returns the response ad. It
// never returns an error to the transport layer: failures are encoded in the
// response (Result=false, ErrorString, or Redirect).
func (c *Coordinator) HandleControl(req *classad.ClassAd) *classad.ClassAd {
	resp := classad.New()
	op := attrString(req, AttrControlOp)
	switch op {
	case OpLeader:
		addr, id := c.LeaderAddr()
		resp.InsertAttrBool(AttrResult, addr != "")
		resp.InsertAttrString(AttrLeaderAddr, addr)
		resp.InsertAttrString(AttrLeaderID, id)
	case OpRegister:
		c.handleRegister(req, resp)
	case OpApply:
		c.handleApply(req, resp)
	default:
		resp.InsertAttrBool(AttrResult, false)
		resp.InsertAttrString(AttrErrorString, "unknown ControlOp: "+op)
	}
	return resp
}

func (c *Coordinator) handleRegister(req, resp *classad.ClassAd) {
	id := attrString(req, AttrPeerID)
	addr := attrString(req, AttrPeerAddress)
	if id == "" || addr == "" {
		resp.InsertAttrBool(AttrResult, false)
		resp.InsertAttrString(AttrErrorString, "register requires PeerID and PeerAddress")
		return
	}
	err := c.RegisterPeer(id, addr)
	if c.encodeRedirect(err, resp) {
		return
	}
	if err != nil {
		resp.InsertAttrBool(AttrResult, false)
		resp.InsertAttrString(AttrErrorString, err.Error())
		return
	}
	resp.InsertAttrBool(AttrResult, true)
}

func (c *Coordinator) handleApply(req, resp *classad.ClassAd) {
	batchStr := attrString(req, AttrBatch)
	b, err := DecodeBatch([]byte(batchStr))
	if err != nil {
		resp.InsertAttrBool(AttrResult, false)
		resp.InsertAttrString(AttrErrorString, err.Error())
		return
	}
	err = c.Apply(b)
	if c.encodeRedirect(err, resp) {
		return
	}
	if err != nil {
		resp.InsertAttrBool(AttrResult, false)
		resp.InsertAttrString(AttrErrorString, err.Error())
		return
	}
	resp.InsertAttrBool(AttrResult, true)
}

// encodeRedirect, when err is ErrNotLeader, fills the response with a redirect to
// the current leader and returns true (the caller then returns).
func (c *Coordinator) encodeRedirect(err error, resp *classad.ClassAd) bool {
	if !errors.Is(err, raft.ErrNotLeader) {
		return false
	}
	addr, id := c.LeaderAddr()
	resp.InsertAttrBool(AttrResult, false)
	resp.InsertAttrBool(AttrRedirect, true)
	resp.InsertAttrString(AttrLeaderAddr, addr)
	resp.InsertAttrString(AttrLeaderID, id)
	return true
}

func attrString(ad *classad.ClassAd, name string) string {
	v := ad.EvaluateAttr(name)
	if v.IsString() {
		s, _ := v.StringValue()
		return s
	}
	return ""
}
