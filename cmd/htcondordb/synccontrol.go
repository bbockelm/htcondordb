package main

import (
	"context"
	"fmt"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/message"
	cedarserver "github.com/bbockelm/cedar/server"

	"github.com/bbockelm/htcondordb/command"
)

// syncController answers DBSyncControl requests: administrative control of the daemon's sync
// sources. It resolves a target to either a schedd-sync tailer (jobs/history) or a managed
// change-data exporter and asks it to resync. It is registered in every mode (unlike the HA-only
// DBControl), so an operator can heal a mirror or re-export without a restart.
type syncController struct {
	sched *scheddSyncManager
	exp   *exporterManager
}

// handle runs one request ClassAd and returns the response ClassAd. Request attributes:
//
//	Action = "resync"   (default if absent)
//	Target = "jobs" | "history" | "epoch" | "<exporter-name>"
//
// Response: Ok (bool); on failure Error (string); on success Note (string).
func (sc *syncController) handle(reqAd *classad.ClassAd) *classad.ClassAd {
	resp := classad.New()
	action, _ := reqAd.EvaluateAttrString("Action")
	target, _ := reqAd.EvaluateAttrString("Target")
	if action == "" {
		action = "resync"
	}
	switch action {
	case "resync":
		if err := sc.resync(target); err != nil {
			return fail(resp, err.Error())
		}
		resp.InsertAttrBool("Ok", true)
		resp.InsertAttrString("Note", fmt.Sprintf("resync requested for %q", target))
	default:
		return fail(resp, fmt.Sprintf("unknown sync action %q (want resync)", action))
	}
	return resp
}

func fail(resp *classad.ClassAd, msg string) *classad.ClassAd {
	resp.InsertAttrBool("Ok", false)
	resp.InsertAttrString("Error", msg)
	return resp
}

// resync routes a target to its owning manager. "jobs"/"history"/"epoch" are the schedd-sync
// tailers; any other name is looked up among the managed exporters.
func (sc *syncController) resync(target string) error {
	switch target {
	case "":
		return fmt.Errorf("resync requires a target (jobs, history, epoch, or an exporter name)")
	case "jobs", "history", "epoch":
		if sc.sched == nil {
			return fmt.Errorf("schedd-sync is not configured on this daemon")
		}
		return sc.sched.Resync(target)
	default:
		if sc.exp == nil {
			return fmt.Errorf("the exporter manager is not running on this daemon")
		}
		return sc.exp.Resync(target)
	}
}

// registerSyncControl installs the DBSyncControl handler on srv. DAEMON-level: resyncing a source
// is an administrative operation.
func registerSyncControl(srv *cedarserver.Server, sched *scheddSyncManager, exp *exporterManager) {
	sc := &syncController{sched: sched, exp: exp}
	srv.Handle(command.DBSyncControl, func(hctx context.Context, c *cedarserver.Conn) error {
		req := message.NewMessageFromStream(c.Stream)
		reqAd, err := req.GetClassAd(hctx)
		if err != nil {
			return err
		}
		respAd := sc.handle(reqAd)
		resp := message.NewMessageForStream(c.Stream)
		if err := resp.PutClassAd(hctx, respAd); err != nil {
			return err
		}
		return resp.FinishMessage(hctx) // flush the frame (EOM); PutClassAd only buffers
	}, "DAEMON")
}
