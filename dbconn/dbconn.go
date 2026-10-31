// Package dbconn is the one place that knows how an htcondordb server and its
// clients tell each other what the dbrpc protocol between them can do.
//
// Before this, a client discovered a server's capabilities by using one and
// recognising the refusal. That left a fallback path behind every feature,
// reachable only against a server old enough to refuse it, so the paths that
// exist for mixed-version sites were the ones no test ran. A server now states
// its protocol version and feature list once, in the CEDAR post-authentication
// ad; a client reads them before it issues anything. See classad's dbrpc
// package for the Supports contract -- in particular that a silent peer is
// unknown, not a refusal.
//
// Both halves live here because they are one agreement. Split across the nine
// places that dial this daemon, a client that forgot to ask would look exactly
// like a server too old to answer, and nothing would report the difference.
package dbconn

import (
	"context"

	"github.com/PelicanPlatform/classad/dbrpc"
	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/security"
)

// Advertise makes a SERVER state what its dbrpc implementation supports. Call
// it on the security config of the listener serving command.DBSession, before
// the server is built.
func Advertise(sec *security.SecurityConfig) {
	if sec == nil {
		return
	}
	sec.SessionAttrs = dbrpc.ServerSessionAttrs()
}

// Discover makes a CLIENT capture what the server advertised. Call it on the
// security config before connecting; the attributes are read back by New or
// PeerOption once the handshake is done.
//
// A client that skips this is not broken -- it simply sees every peer as
// unknown and keeps to the probe paths, which is what an old client does.
func Discover(sec *security.SecurityConfig) {
	if sec == nil {
		return
	}
	sec.WantSessionAttrs = dbrpc.ClientSessionAttrNames()
}

// PeerOption carries what cl's peer advertised into a dbrpc client. Use it
// where the transport is not a plain CEDAR stream and New will not do -- the
// capi connection wraps its transport to watch for the peer dying.
//
// A connection that never authenticated, or whose config skipped Discover,
// yields no attributes, which reads as a peer that advertised nothing.
func PeerOption(cl *cedarclient.HTCondorClient) dbrpc.ClientOption {
	var attrs map[string]string
	if cl != nil {
		if neg := cl.GetSecurityNegotiation(); neg != nil {
			attrs = neg.SessionAttrs
		}
	}
	return dbrpc.WithPeerSessionAttrs(attrs)
}

// New builds the dbrpc client for an authenticated CEDAR connection, carrying
// over whatever the server advertised. ctx bounds the connection's I/O.
func New(ctx context.Context, cl *cedarclient.HTCondorClient) *dbrpc.Client {
	return dbrpc.NewClient(dbrpc.NewCedarConn(ctx, cl.GetStream()), PeerOption(cl))
}
