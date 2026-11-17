package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/message"
	cedarserver "github.com/bbockelm/cedar/server"

	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/server"
)

// backupController answers DBBackupControl requests: take a snapshot now, or verify snapshots
// already written. It holds the configured snapshot directory so an operator can ask for a backup
// without naming a path, and so a verify with no argument has a directory to walk.
type backupController struct {
	svc *server.Service
	dir string // HTCONDORDB_SNAPSHOT_DIR; may be empty

	// one at a time: a snapshot reads every table, and two concurrent ones would double the I/O
	// for no benefit. A second caller is refused rather than queued, so an operator gets an answer
	// instead of a hang.
	mu     sync.Mutex
	inFlig bool
}

// handle runs one request ClassAd and returns the response ClassAd. Request attributes:
//
//	Action = "backup" | "verify"   (default "backup")
//	Path   = explicit file (backup) or file to verify (verify); optional
//
// Response: Ok (bool); on failure Error (string). A backup adds File, Bytes, DurationSeconds,
// SHA256, Tables and SkippedArchives. A verify adds Checked, Good, Bad, Unverifiable and Detail.
func (bc *backupController) handle(reqAd *classad.ClassAd) *classad.ClassAd {
	resp := classad.New()
	action, _ := reqAd.EvaluateAttrString("Action")
	path, _ := reqAd.EvaluateAttrString("Path")
	if action == "" {
		action = "backup"
	}
	switch action {
	case "backup":
		return bc.backup(resp, strings.TrimSpace(path))
	case "verify":
		return bc.verify(resp, strings.TrimSpace(path))
	default:
		return fail(resp, fmt.Sprintf("unknown backup action %q (want backup or verify)", action))
	}
}

// backup writes a snapshot now. With no path it writes a timestamped file into the configured
// snapshot directory, the same naming the periodic snapshots use, so an on-demand backup sorts
// and prunes alongside them.
func (bc *backupController) backup(resp *classad.ClassAd, path string) *classad.ClassAd {
	if path == "" {
		if bc.dir == "" {
			return fail(resp, "no snapshot directory is configured (HTCONDORDB_SNAPSHOT_DIR); pass an explicit Path")
		}
		path = uniqueSnapshotPath(bc.dir, time.Now())
	}
	if !filepath.IsAbs(path) {
		return fail(resp, fmt.Sprintf("Path must be absolute, got %q", path))
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fail(resp, err.Error())
	}

	bc.mu.Lock()
	if bc.inFlig {
		bc.mu.Unlock()
		return fail(resp, "a backup is already running on this daemon")
	}
	bc.inFlig = true
	bc.mu.Unlock()
	defer func() {
		bc.mu.Lock()
		bc.inFlig = false
		bc.mu.Unlock()
	}()

	started := time.Now()
	if err := bc.svc.SnapshotToFile(path); err != nil {
		return fail(resp, err.Error())
	}
	resp.InsertAttrBool("Ok", true)
	resp.InsertAttrString("File", path)
	resp.InsertAttrFloat("DurationSeconds", time.Since(started).Seconds())
	if m, err := server.ReadManifest(path); err == nil {
		resp.InsertAttr("Bytes", m.Bytes)
		resp.InsertAttrString("SHA256", m.SHA256)
		resp.InsertAttrString("Tables", strings.Join(m.Tables, ","))
		resp.InsertAttrString("SkippedArchives", strings.Join(m.SkippedArchives, ","))
	}
	return resp
}

// uniqueSnapshotPath builds a timestamped snapshot path that does not already exist.
//
// The snapshot name has one-second granularity, which is ample for the periodic timer but not for
// a person: two on-demand backups in the same second would otherwise land on the same name and the
// second would silently replace the first. An operator who asks for a backup gets a backup, so a
// collision takes a -1, -2 suffix rather than overwriting. The suffix keeps the prefix and
// extension, so retention still sees and prunes these.
func uniqueSnapshotPath(dir string, now time.Time) string {
	base := server.SnapshotName(now)
	path := filepath.Join(dir, base)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return path
	}
	stem := strings.TrimSuffix(base, ".cadb")
	for n := 1; n < 1000; n++ {
		cand := filepath.Join(dir, fmt.Sprintf("%s-%d.cadb", stem, n))
		if _, err := os.Stat(cand); os.IsNotExist(err) {
			return cand
		}
	}
	return path // a thousand backups in one second: let it overwrite rather than spin
}

// verify checks one snapshot, or every snapshot in the configured directory when no path is given.
// Detail carries one line per file so the caller can print what is wrong without a second round
// trip.
func (bc *backupController) verify(resp *classad.ClassAd, path string) *classad.ClassAd {
	var results []server.VerifyResult
	switch {
	case path != "":
		if !filepath.IsAbs(path) {
			return fail(resp, fmt.Sprintf("Path must be absolute, got %q", path))
		}
		res, err := server.VerifySnapshot(path)
		if err != nil {
			return fail(resp, err.Error())
		}
		results = []server.VerifyResult{res}
	case bc.dir != "":
		var err error
		if results, err = server.VerifyDir(bc.dir); err != nil {
			return fail(resp, err.Error())
		}
	default:
		return fail(resp, "no snapshot directory is configured (HTCONDORDB_SNAPSHOT_DIR); pass an explicit Path")
	}

	var good, bad, unverifiable int
	var detail strings.Builder
	for _, r := range results {
		switch {
		case r.Ok:
			good++
			fmt.Fprintf(&detail, "ok           %s (%d bytes)\n", filepath.Base(r.File), r.Bytes)
		case !r.HasManifest:
			unverifiable++
			fmt.Fprintf(&detail, "unverifiable %s: %s\n", filepath.Base(r.File), r.Problem)
		default:
			bad++
			fmt.Fprintf(&detail, "DAMAGED      %s: %s\n", filepath.Base(r.File), r.Problem)
		}
	}
	// Ok reports whether anything is known to be damaged. An unverifiable snapshot is not a
	// failure -- it is one written before manifests existed -- so it does not make Ok false, but
	// it is counted separately so the caller can say so.
	resp.InsertAttrBool("Ok", bad == 0)
	resp.InsertAttr("Checked", int64(len(results)))
	resp.InsertAttr("Good", int64(good))
	resp.InsertAttr("Bad", int64(bad))
	resp.InsertAttr("Unverifiable", int64(unverifiable))
	resp.InsertAttrString("Detail", detail.String())
	return resp
}

// registerBackupControl installs the DBBackupControl handler on srv. DAEMON-level: a snapshot
// reads every mutable table, private attributes included.
func registerBackupControl(srv *cedarserver.Server, svc *server.Service, snapshotDir string) {
	bc := &backupController{svc: svc, dir: snapshotDir}
	srv.Handle(command.DBBackupControl, func(hctx context.Context, c *cedarserver.Conn) error {
		req := message.NewMessageFromStream(c.Stream)
		reqAd, err := req.GetClassAd(hctx)
		if err != nil {
			return err
		}
		respAd := bc.handle(reqAd)
		resp := message.NewMessageForStream(c.Stream)
		if err := resp.PutClassAd(hctx, respAd); err != nil {
			return err
		}
		return resp.FinishMessage(hctx) // flush the frame (EOM); PutClassAd only buffers
	}, "DAEMON")
}
