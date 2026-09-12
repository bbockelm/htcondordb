package main

import (
	"context"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/message"
	cedarserver "github.com/bbockelm/cedar/server"

	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/dbad"
)

// statusReporter answers DBSyncStatus requests with the daemon's sync health.
//
// The response is assembled from the same two halves as the collector advertisement -- the generic
// base ad (identity, version, MonitorSelf*) and dbad's HTCondorDB attributes -- so the two cannot
// drift. A client that already parses the advertised ad parses this one unchanged, and an
// attribute added to the ad appears here for free rather than having to be added twice.
type statusReporter struct {
	publish func(*classad.ClassAd)
	augment func(*classad.ClassAd)
}

// handle builds the response ad. The request is accepted and ignored: there is exactly one thing
// to report and no options to select, but reading a request ad keeps the wire shape the same as
// the other ClassAd commands, which leaves room to add selectors later without a new command.
func (sr *statusReporter) handle(_ *classad.ClassAd) *classad.ClassAd {
	resp := classad.New()
	resp.InsertAttrString("MyType", dbad.AdType)
	if sr.publish != nil {
		sr.publish(resp)
	}
	if sr.augment != nil {
		sr.augment(resp)
	}
	resp.InsertAttrBool("Ok", true)
	return resp
}

// registerSyncStatus installs the DBSyncStatus handler on srv. READ-level: it reports health, not
// secrets, and the clients that need it are readers deciding whether this mirror is current enough
// to read from.
func registerSyncStatus(srv *cedarserver.Server, publish, augment func(*classad.ClassAd)) {
	sr := &statusReporter{publish: publish, augment: augment}
	srv.Handle(command.DBSyncStatus, func(hctx context.Context, c *cedarserver.Conn) error {
		reqAd, err := message.NewMessageFromStream(c.Stream).GetClassAd(hctx)
		if err != nil {
			return err
		}
		resp := message.NewMessageForStream(c.Stream)
		if err := resp.PutClassAd(hctx, sr.handle(reqAd)); err != nil {
			return err
		}
		return resp.FinishMessage(hctx) // flush the frame (EOM); PutClassAd only buffers
	}, "READ")
}
