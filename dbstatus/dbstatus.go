// Package dbstatus reads an htcondordb daemon's sync health over its command port.
//
// The daemon publishes that health two ways. It advertises a ClassAd to the collector, which is
// how a pool-wide consumer (the REST API, the MCP server) finds a database and decides whether to
// trust it. But a client that reaches the daemon through its address file has no collector in the
// picture, and so no advertisement to read: it knows where the database is and nothing about
// whether the mirror behind it is current. A reader that must not serve stale answers needs that
// second fact before it queries.
//
// This asks the daemon directly. The reply is the same ClassAd the daemon advertises, so
// golang-htcondor's dbmirror package parses it unchanged -- Query feeds ParseAd, and the caller
// gets the same Decision it would have gotten from the collector.
package dbstatus

import (
	"context"
	"fmt"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/message"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/command"
)

// DefaultTimeout bounds one status exchange. A status read sits in front of a query the caller is
// about to make, so it is worth failing fast and falling back to the authoritative source rather
// than stalling the read behind an unresponsive database.
const DefaultTimeout = 10 * time.Second

// Query asks the daemon at addr for its sync-health ClassAd.
//
// addr is a CEDAR sinful, typically read from the daemon's address file. cfg supplies the client
// security policy. The command is READ-authorized.
func Query(ctx context.Context, cfg *config.Config, addr string) (*classad.ClassAd, error) {
	sec, err := htcondor.GetSecurityConfig(cfg, command.DBSyncStatus, "CLIENT")
	if err != nil {
		return nil, fmt.Errorf("security config for the htcondordb status command: %w", err)
	}
	sec.Command = command.DBSyncStatus

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultTimeout)
		defer cancel()
	}

	cl, err := cedarclient.ConnectAndAuthenticate(ctx, addr, sec)
	if err != nil {
		return nil, fmt.Errorf("connecting to htcondordb at %s: %w", addr, err)
	}
	defer func() { _ = cl.Close() }()

	s := cl.GetStream()
	out := message.NewMessageForStream(s)
	// The daemon ignores the request body today, but the exchange is request/response so that a
	// later selector can be added without a new command number.
	req := classad.New()
	req.InsertAttrString("Action", "status")
	if err := out.PutClassAd(ctx, req); err != nil {
		return nil, err
	}
	if err := out.FinishMessage(ctx); err != nil {
		return nil, err
	}

	respAd, err := message.NewMessageFromStream(s).GetClassAd(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading the htcondordb status reply: %w", err)
	}
	if ok, present := respAd.EvaluateAttrBool("Ok"); present && !ok {
		msg, _ := respAd.EvaluateAttrString("Error")
		if msg == "" {
			msg = "status request failed"
		}
		return nil, fmt.Errorf("%s", msg)
	}
	return respAd, nil
}
