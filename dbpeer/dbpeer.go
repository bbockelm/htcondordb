// Package dbpeer reads what an htcondordb server advertised about its dbrpc
// protocol on a CEDAR connection.
//
// It exists for the nil, which is easy to get wrong in a way nothing catches:
// a cedar client reports no negotiation when no handshake ran, and reaching
// through that for the attributes is a nil dereference. capi's connection pool
// builds conns without a handshake, so the panic is reachable from a test --
// and, in principle, from any path that gets a client without authenticating.
//
// A connection that advertised nothing yields nil, which dbrpc reads as a peer
// that said nothing: SupportUnknown, and the caller keeps to the probe paths.
// That is the same answer an old server gives, which is the answer we want.
package dbpeer

import (
	cedarclient "github.com/bbockelm/cedar/client"
)

// Attrs is what the peer advertised, ready for dbrpc.WithPeerSessionAttrs.
func Attrs(cl *cedarclient.HTCondorClient) map[string]string {
	if cl == nil {
		return nil
	}
	neg := cl.GetSecurityNegotiation()
	if neg == nil {
		return nil
	}
	return neg.SessionAttrs
}
