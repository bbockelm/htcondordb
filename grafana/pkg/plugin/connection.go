// Package plugin implements the htcondordb Grafana backend datasource: it opens an
// authenticated dbrpc session to an htcondordb server and runs the repl SQL engine
// on the user's queries, returning Grafana data frames.
package plugin

import (
	"context"
	"fmt"
	"time"

	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/command"
)

// connConfig is the resolved connection settings for one datasource instance.
type connConfig struct {
	// Address is the htcondordb server, as an HTCondor sinful string or host:port.
	Address string
	// Token, when non-empty, is an IDTOKEN offered for authentication (so the
	// session maps to a user and may be authorized beyond anonymous READ). Empty
	// means an anonymous, read-only connection.
	Token string
	// ConnectTimeout bounds dialing + the CEDAR handshake.
	ConnectTimeout time.Duration
}

// dbSession is one authenticated dbrpc client plus the cleanup that closes it and
// the underlying CEDAR connection.
type dbSession struct {
	client  *dbrpc.Client
	cleanup func()
}

// connect opens an authenticated DBSession to the configured htcondordb server.
// It mirrors the htcondordb-cli connect path: build a CLIENT security config for
// the DBSession command (optionally carrying an IDTOKEN), prefer authentication so
// the peer maps our identity, dial + authenticate over CEDAR, then wrap the stream
// in a dbrpc client. The returned session must be closed via its cleanup.
func connect(ctx context.Context, cc connConfig) (*dbSession, error) {
	// nil config -> golang-htcondor's compiled-in security defaults (no HTCondor
	// config files needed on the Grafana host); a supplied token is prepended as
	// TOKEN so it is actually offered.
	sec, err := htcondor.NewClientSecurityConfig(ctx, cc.Token, cc.Address, command.DBSession, "CLIENT", nil)
	if err != nil {
		return nil, fmt.Errorf("building client security config: %w", err)
	}
	sec.Command = command.DBSession
	// PREFERRED (not OPTIONAL) so a token-bearing client authenticates instead of
	// silently negotiating down to an anonymous session; it still connects
	// read-only when no method is mutually available.
	if sec.Authentication == security.SecurityOptional {
		sec.Authentication = security.SecurityPreferred
	}

	timeout := cc.ConnectTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	connCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cl, err := cedarclient.ConnectAndAuthenticate(connCtx, cc.Address, sec)
	if err != nil {
		return nil, fmt.Errorf("connecting to htcondordb at %s: %w", cc.Address, err)
	}
	// dbrpc rides the still-authenticated stream; NewCedarConn takes the long-lived
	// ctx (not the dial-timeout one) so the session is not torn down when connect
	// returns.
	dbc := dbrpc.NewClient(dbrpc.NewCedarConn(ctx, cl.GetStream()))
	return &dbSession{
		client:  dbc,
		cleanup: func() { _ = dbc.Close(); _ = cl.Close() },
	}, nil
}
