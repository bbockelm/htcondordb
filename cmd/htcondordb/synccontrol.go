package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/bbockelm/cedar/message"
	cedarserver "github.com/bbockelm/cedar/server"
	"github.com/dustin/go-humanize"

	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/server"
)

// syncController answers DBSyncControl requests: administrative control of the daemon's sync
// sources and of the tables they own. It resolves a resync target to either a schedd-sync tailer
// (jobs/history/epoch) or a managed change-data exporter, and routes the data-removing admin
// actions on an OWNED table -- which the dbrpc session refuses on any connection
// (ServeOptions.TableWritable) -- through that table's owner. It is registered in every mode
// (unlike the HA-only DBControl), so an operator can heal a mirror or re-export without a restart.
type syncController struct {
	sched  *scheddSyncManager
	exp    *exporterManager
	ced    *cedarSyncManager
	imp    *importerManager
	owners *server.TableOwners
	cat    *db.Catalog
	fed    *federationManager
}

// ownerTruncateTimeout bounds how long a truncate waits for the owning tailer to take it. The
// CLI gives the whole exchange 30s.
const ownerTruncateTimeout = 20 * time.Second

// handle runs one request ClassAd and returns the response ClassAd. Request attributes:
//
//	Action = "resync"   (default if absent)  Target = "jobs" | "history" | "epoch" | "<exporter-name>"
//	Action = "truncate" | "rotate"           Target = <owned table>
//	Action = "retention.set"                 Target = <owned archive>, Args = "<maxSegments> <maxBytes> [attr ageSeconds]"
//	Action = "owner"                         Target = <table>   (who owns it, and what to run instead)
//
//	Action = "retire"   (federation hub)
//	Target = "<schedd-name>"
//
// Response: Ok (bool); on failure Error (string); on success Note (string).
func (sc *syncController) handle(ctx context.Context, reqAd *classad.ClassAd) *classad.ClassAd {
	resp := classad.New()
	action, _ := reqAd.EvaluateAttrString("Action")
	target, _ := reqAd.EvaluateAttrString("Target")
	args, _ := reqAd.EvaluateAttrString("Args")
	if action == "" {
		action = "resync"
	}
	var note string
	var err error
	switch action {
	case "resync":
		if err = sc.resync(target); err == nil {
			note = fmt.Sprintf("resync requested for %q", target)
		}
	case "retire":
		note, err = sc.retire(target)
	case "truncate":
		note, err = sc.truncate(ctx, target)
	case "rotate":
		note, err = sc.rotate(target)
	case "retention.set":
		note, err = sc.retentionSet(target, strings.Fields(args))
	case "owner":
		note, err = sc.ownerNote(target), nil
	default:
		err = fmt.Errorf("unknown sync action %q (want resync, truncate, rotate, retention.set, owner, or retire)", action)
	}
	if err != nil {
		return fail(resp, err.Error())
	}
	resp.InsertAttrBool("Ok", true)
	resp.InsertAttrString("Note", note)
	return resp
}

func fail(resp *classad.ClassAd, msg string) *classad.ClassAd {
	resp.InsertAttrBool("Ok", false)
	resp.InsertAttrString("Error", msg)
	return resp
}

// retire has a federation hub retire a source now: its rows leave the hub's mutable tables.
func (sc *syncController) retire(target string) (string, error) {
	if target == "" {
		return "", fmt.Errorf("retire requires a target (a federated schedd name)")
	}
	if sc.fed == nil {
		return "", fmt.Errorf("this daemon is not a federation hub")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := sc.fed.Retire(ctx, target); err != nil {
		return "", err
	}
	return fmt.Sprintf("retired %q: its jobs/syncstatus/federation_sources rows are deleted; "+
		"archive rows age out with retention. If it is still in the AP set it returns at the next discovery, replayed from scratch", target), nil
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

// ownedBy returns table's owners, or an error saying the table is not owned (in which case the
// ordinary admin action applies and this path has nothing to do).
func (sc *syncController) ownedBy(table, action string) ([]string, error) {
	if table == "" {
		return nil, fmt.Errorf("%s requires a table", action)
	}
	owners := sc.owners.Owners(table)
	if len(owners) == 0 {
		return nil, fmt.Errorf("table %q is not owned by a writer in this daemon; %s it directly", table, action)
	}
	return owners, nil
}

// truncate empties an owned table through its owner. Only schedd sync can rebuild what it owns
// (it re-reads the history file); a replica or an import target would be left permanently
// missing the truncated records, since neither source resends what its cursor has passed, so
// those are refused with the steps that do it safely.
func (sc *syncController) truncate(ctx context.Context, table string) (string, error) {
	owners, err := sc.ownedBy(table, "truncate")
	if err != nil {
		return "", err
	}
	if slices.Contains(owners, ownerScheddSync) && sc.sched != nil {
		tctx, cancel := context.WithTimeout(ctx, ownerTruncateTimeout)
		defer cancel()
		return sc.sched.Truncate(tctx, table)
	}
	return "", fmt.Errorf("%s", joinSentences(sc.describe(table, owners)+
		"; truncating it here would leave it permanently missing those records", sc.releaseHint(owners)))
}

// joinSentences joins the non-empty sentences, each ending in one period.
func joinSentences(parts ...string) string {
	var out []string
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			if !strings.HasSuffix(p, ".") {
				p += "."
			}
			out = append(out, p)
		}
	}
	return strings.Join(out, " ")
}

// rotate enforces an owned archive's retention now -- what the periodic archive maintenance does
// on its own schedule, so it never conflicts with the owner.
func (sc *syncController) rotate(table string) (string, error) {
	if _, err := sc.ownedBy(table, "rotate"); err != nil {
		return "", err
	}
	a, ok := sc.cat.ArchiveTable(table)
	if !ok {
		return "", fmt.Errorf("%q is not an archive table; rotate applies to archives", table)
	}
	n, err := a.Rotate(float64(time.Now().Unix()))
	if err != nil {
		return "", fmt.Errorf("rotate: %w", err)
	}
	return fmt.Sprintf("rotated: dropped %d segment(s)", n), nil
}

// retentionSet sets an owned archive's retention bounds (same arguments as the admin action). A
// schedd-sync archive's size cap (and job_metrics' age cap) is also driven by configuration,
// which is re-applied on the next start or reconfigure that changes it; the note says so.
func (sc *syncController) retentionSet(table string, args []string) (string, error) {
	owners, err := sc.ownedBy(table, "retention.set")
	if err != nil {
		return "", err
	}
	a, ok := sc.cat.ArchiveTable(table)
	if !ok {
		return "", fmt.Errorf("%q is not an archive table; retention applies to archives", table)
	}
	r, err := parseRetention(args)
	if err != nil {
		return "", err
	}
	if err := a.SetRetention(r); err != nil {
		return "", fmt.Errorf("retention.set: %w", err)
	}
	note := fmt.Sprintf("retention set on %s (maxSegments=%d maxBytes=%d", table, r.MaxSegments, r.MaxBytes)
	if r.MaxAgeAttr != "" {
		note += fmt.Sprintf(" maxAge=%gs on %s", r.MaxAge, r.MaxAgeAttr)
	}
	note += ")"
	if slices.Contains(owners, ownerScheddSync) {
		note += "; note: schedd sync re-applies its configured size cap (HTCONDORDB_ARCHIVE_MAX_BYTES and the per-table " +
			"*_MAX_BYTES knobs) when the daemon restarts or a reconfigure changes them -- set it there to keep it"
	}
	return note, nil
}

// parseRetention parses "<maxSegments> <maxBytes> [maxAgeAttr maxAgeSeconds]", the admin
// retention.set arguments. maxBytes takes a unit suffix (KB = 1000, KiB = 1024).
func parseRetention(args []string) (db.Retention, error) {
	var r db.Retention
	if len(args) != 2 && len(args) != 4 {
		return r, fmt.Errorf("retention.set needs <maxSegments> <maxBytes> [maxAgeAttr maxAgeSeconds]")
	}
	if _, err := fmt.Sscanf(args[0], "%d", &r.MaxSegments); err != nil || r.MaxSegments < 0 {
		return r, fmt.Errorf("maxSegments must be a non-negative integer, got %q", args[0])
	}
	b, err := humanize.ParseBytes(args[1])
	if err != nil {
		return r, fmt.Errorf("maxBytes: %w", err)
	}
	r.MaxBytes = int64(b)
	if len(args) == 4 {
		r.MaxAgeAttr = args[2]
		if _, err := fmt.Sscanf(args[3], "%g", &r.MaxAge); err != nil || r.MaxAge < 0 {
			return r, fmt.Errorf("maxAgeSeconds must be a non-negative number, got %q", args[3])
		}
	}
	return r, nil
}

// ownerNote answers "owner": who maintains table and what an operator runs instead of writing it.
func (sc *syncController) ownerNote(table string) string {
	owners := sc.owners.Owners(table)
	if len(owners) == 0 {
		return fmt.Sprintf("table %q is not owned by a writer in this daemon", table)
	}
	return joinSentences(sc.describe(table, owners)+"; clients may read and watch it but not modify it", sc.insteadHint(owners))
}

// describe says which writer(s) maintain table.
func (sc *syncController) describe(table string, owners []string) string {
	parts := make([]string, 0, len(owners))
	for _, o := range owners {
		switch o {
		case ownerScheddSync:
			parts = append(parts, "schedd sync (mirrored from the schedd's job_queue.log / history files)")
		case ownerReplication:
			parts = append(parts, fmt.Sprintf("replication (HTCONDORDB_REPLICATE_SOURCES: %s)", orUnknown(sc.ced.SourcesFor(table))))
		case ownerHistoryImport:
			parts = append(parts, fmt.Sprintf("history import (HTCONDORDB_HISTORY_IMPORT jobs: %s)", orUnknown(sc.imp.JobsFor(table))))
		case ownerFederation:
			parts = append(parts, "the federation hub (replicated from its spokes; HTCONDORDB_FEDERATE_SCHEDD_CONSTRAINT / HTCONDORDB_FEDERATE_SPOKES)")
		default:
			parts = append(parts, o)
		}
	}
	return fmt.Sprintf("table %q is maintained by %s", table, strings.Join(parts, " and "))
}

// insteadHint names the operator actions that do work on an owned table.
func (sc *syncController) insteadHint(owners []string) string {
	if slices.Contains(owners, ownerScheddSync) {
		return "Use `.resync <jobs|history|epoch>` to rebuild it from the schedd's files, or `.truncate`/`.rotate`/`.retention` " +
			"(routed through schedd sync); to write it yourself, use another table."
	}
	return joinSentences("`.rotate` and `.retention` still work (routed through the daemon)", sc.releaseHint(owners))
}

// releaseHint says how to take a table back from a replication/import owner.
func (sc *syncController) releaseHint(owners []string) string {
	var steps []string
	if slices.Contains(owners, ownerReplication) {
		steps = append(steps, "remove its source from HTCONDORDB_REPLICATE_SOURCES (and delete <db dir>/cedarsync/<source>.cursor to replicate it again from the start)")
	}
	if slices.Contains(owners, ownerHistoryImport) {
		steps = append(steps, "remove its job from HTCONDORDB_HISTORY_IMPORT (and delete $(LOG)/history-import-<job>.cursors.json to import it again from the start)")
	}
	var hint string
	if len(steps) > 0 {
		hint = "To modify it, first " + strings.Join(steps, " and ") + ", then condor_reconfig; the table is then an ordinary table."
	}
	if slices.Contains(owners, ownerFederation) {
		// A hub's table is rebuilt only by its spokes, and a resuming spoke never resends what the
		// hub's cursor has passed. The per-AP removal is retirement.
		hint = joinSentences(hint, "To remove one access point's rows, use `.retire <schedd>` (its archive rows age out with retention); "+
			"to stop federating, remove HTCONDORDB_FEDERATE_SCHEDD_CONSTRAINT and HTCONDORDB_FEDERATE_SPOKES and condor_reconfig")
	}
	return hint
}

func orUnknown(ss []string) string {
	if len(ss) == 0 {
		return "?"
	}
	return strings.Join(ss, ", ")
}

// registerSyncControl installs the DBSyncControl handler on srv. DAEMON-level: resyncing a source
// or emptying an owned table is an administrative operation.
func registerSyncControl(srv *cedarserver.Server, sc *syncController) {
	srv.Handle(command.DBSyncControl, func(hctx context.Context, c *cedarserver.Conn) error {
		req := message.NewMessageFromStream(c.Stream)
		reqAd, err := req.GetClassAd(hctx)
		if err != nil {
			return err
		}
		respAd := sc.handle(hctx, reqAd)
		resp := message.NewMessageForStream(c.Stream)
		if err := resp.PutClassAd(hctx, respAd); err != nil {
			return err
		}
		return resp.FinishMessage(hctx) // flush the frame (EOM); PutClassAd only buffers
	}, "DAEMON")
}
