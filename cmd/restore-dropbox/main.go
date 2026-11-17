// Command restore-dropbox reads an archive dropbox back into an htcondordb archive table.
//
// The dropbox exporter writes an archive's records out as self-contained ClassAd text, one record
// per file, so that no record depends on a shared compression dictionary or on anything else
// outside itself. That makes a dropbox the archive copy that survives damage to the store's
// internal structure -- and, until this command, the one nothing could read back.
//
//	restore-dropbox -dir <dropbox> -table <archive> [-addr HOST:PORT] [-dry-run] [-skip-dedup]
//
// Restoring is re-runnable: records are deduped on GlobalJobId, so the exporter's at-least-once
// duplicates and a half-finished earlier run are both harmless.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/dbpeer"
	"github.com/bbockelm/htcondordb/dropboxrestore"
	"github.com/bbockelm/htcondordb/historyimport"
	"github.com/bbockelm/htcondordb/locate"
)

func main() {
	dir := flag.String("dir", "", "dropbox directory to restore from (required)")
	table := flag.String("table", "", "destination archive table (required)")
	addr := flag.String("addr", "", "htcondordb address HOST:PORT (default: discover)")
	dryRun := flag.Bool("dry-run", false, "read and report without writing anything")
	verify := flag.Bool("verify", false, "check the dropbox against its manifests and report; implies -dry-run")
	skipDedup := flag.Bool("skip-dedup", false, "append without checking for existing records (faster, correct only into an empty table)")
	quiet := flag.Bool("quiet", false, "suppress progress output")
	flag.Parse()

	if *dir == "" || *table == "" {
		fmt.Fprintln(os.Stderr, "restore-dropbox: -dir and -table are required")
		flag.Usage()
		os.Exit(2)
	}
	if *verify {
		*dryRun = true
	}
	if err := run(*dir, *table, *addr, *dryRun, *skipDedup, *quiet); err != nil {
		fmt.Fprintln(os.Stderr, "restore-dropbox:", err)
		os.Exit(1)
	}
}

func run(dir, table, addrFlag string, dryRun, skipDedup, quiet bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A dry run never touches the database, so it must not require one to be reachable: an
	// operator checking what a dropbox holds should be able to do it from anywhere the files are.
	opt := dropboxrestore.Options{Dir: dir, Table: table, DryRun: dryRun, SkipDedup: skipDedup}
	var w dropboxrestore.Writer
	if !dryRun {
		cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "TOOL"})
		if err != nil {
			return err
		}
		dbc, closeFn, err := dial(ctx, cfg, addrFlag)
		if err != nil {
			return err
		}
		defer closeFn()
		w = &historyimport.DBWriter{Client: dbc}
	}

	if !quiet {
		last := time.Now()
		opt.Progress = func(r dropboxrestore.Result) {
			if time.Since(last) < 2*time.Second {
				return
			}
			last = time.Now()
			fmt.Fprintf(os.Stderr, "\r%d batches, %d records, %d appended, %d already present",
				r.Batches, r.Records, r.Appended, r.Duplicates)
		}
	}

	res, err := dropboxrestore.Restore(ctx, w, opt)
	if !quiet {
		fmt.Fprintln(os.Stderr)
	}
	// Report what was found even when the restore stopped early: the loss reports and sequence
	// gaps are the part the operator must see, and they are known before any write happens.
	res.Report(os.Stdout)
	if dryRun {
		fmt.Fprintln(os.Stdout, "(dry run -- nothing was written)")
	}
	// A verify that found damage must fail the process, so it is usable from cron and from a
	// monitoring check rather than only by eye.
	if err == nil && (len(res.BadBatches) > 0 || len(res.ShortReads) > 0) {
		return fmt.Errorf("%d damaged batch(es), %d short read(s)", len(res.BadBatches), len(res.ShortReads))
	}
	return err
}

// dial connects to the htcondordb daemon, resuming an inherited session when present.
func dial(ctx context.Context, cfg *config.Config, addrFlag string) (*dbrpc.Client, func(), error) {
	addr := addrFlag
	if addr == "" {
		var err error
		if addr, err = locate.Daemon(cfg); err != nil {
			return nil, nil, fmt.Errorf("%w -- or pass -addr", err)
		}
	}
	sec, err := htcondor.GetSecurityConfig(cfg, command.DBSession, "CLIENT")
	if err != nil {
		return nil, nil, fmt.Errorf("building client security config: %w", err)
	}
	sec.Command = command.DBSession
	if sec.Authentication == security.SecurityOptional {
		sec.Authentication = security.SecurityPreferred
	}
	if sid := security.GetParentSessionID(); sid != "" {
		sec.SessionID = sid
		sec.SessionCache = security.GetSessionCache()
	}
	connCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	sec.WantSessionAttrs = dbrpc.ClientSessionAttrNames()
	cl, err := cedarclient.ConnectAndAuthenticate(connCtx, addr, sec)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to htcondordb at %s: %w", addr, err)
	}
	dbc := dbrpc.NewClient(dbrpc.NewCedarConn(ctx, cl.GetStream()),
		dbrpc.WithPeerSessionAttrs(dbpeer.Attrs(cl)))
	return dbc, func() { _ = dbc.Close(); _ = cl.Close() }, nil
}
