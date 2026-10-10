package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/htcondordb/dbad"
	"github.com/bbockelm/htcondordb/server"
	"github.com/bbockelm/htcondordb/syncstatus"
)

// TestScheddDaemonName pins the schedd's naming rule (get_daemon_name.cpp), because a hub pairs a
// spoke with a schedd by this exact string: one character off and the spoke is never paired.
func TestScheddDaemonName(t *testing.T) {
	const fqdn, short = "ap1.example.org", "ap1"
	cases := []struct {
		name       string
		configured string
		asCondor   bool
		user       string
		want       string
	}{
		{"with @ used verbatim", "sched2@ap9.example.org", true, "condor", "sched2@ap9.example.org"},
		{"with @ verbatim even for a user", "x@y", false, "alice", "x@y"},
		{"bare name gets @FQDN", "sched2", true, "condor", "sched2@ap1.example.org"},
		{"own FQDN is just the host", "AP1.example.org", true, "condor", fqdn},
		{"own short name is just the host", "ap1", true, "condor", fqdn},
		{"unset, condor user", "", true, "condor", fqdn},
		{"unset, personal condor", "", false, "alice", "alice@ap1.example.org"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, rule := scheddDaemonName(c.configured, fqdn, short, c.asCondor, c.user)
			if got != c.want {
				t.Fatalf("scheddDaemonName(%q) = %q (%s), want %q", c.configured, got, rule, c.want)
			}
			if rule == "" {
				t.Fatal("no rule reported; the startup log must say why this name was chosen")
			}
		})
	}
}

func TestResolveMirroredSchedd(t *testing.T) {
	dir := t.TempDir()
	addrFile := filepath.Join(dir, ".schedd_address")
	if err := os.WriteFile(addrFile, []byte("<10.0.0.1:9618?addrs=10.0.0.1-9618>\n$CondorVersion: 25.0 $\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Run("override wins", func(t *testing.T) {
		cfg := mkSyncCfg(t, "FULL_HOSTNAME = ap1.example.org\nSCHEDD_NAME = foo\nHTCONDORDB_MIRRORED_SCHEDD_NAME = pinned@ap7\nSCHEDD_ADDRESS_FILE = "+addrFile+"\n")
		m := resolveMirroredSchedd(cfg)
		if m.name != "pinned@ap7" || m.rule != "HTCONDORDB_MIRRORED_SCHEDD_NAME" {
			t.Fatalf("got %+v", m)
		}
		if got := m.address(); got != "<10.0.0.1:9618?addrs=10.0.0.1-9618>" {
			t.Fatalf("address = %q, want the first line of the address file", got)
		}
	})
	t.Run("schedd-scoped name beats the global one", func(t *testing.T) {
		cfg := mkSyncCfg(t, "FULL_HOSTNAME = ap1.example.org\nSCHEDD_NAME = global\nSCHEDD.SCHEDD_NAME = scoped\n")
		if m := resolveMirroredSchedd(cfg); m.name != "scoped@ap1.example.org" {
			t.Fatalf("got %+v", m)
		}
	})
	t.Run("SCHEDD_NAME with @", func(t *testing.T) {
		cfg := mkSyncCfg(t, "FULL_HOSTNAME = ap1.example.org\nSCHEDD_NAME = s1@ap1.example.org\n")
		if m := resolveMirroredSchedd(cfg); m.name != "s1@ap1.example.org" {
			t.Fatalf("got %+v", m)
		}
	})
	t.Run("SCHEDD_NAME without @", func(t *testing.T) {
		cfg := mkSyncCfg(t, "FULL_HOSTNAME = ap1.example.org\nSCHEDD_NAME = s1\n")
		if m := resolveMirroredSchedd(cfg); m.name != "s1@ap1.example.org" {
			t.Fatalf("got %+v", m)
		}
	})
	t.Run("unreadable address file is omitted, not guessed", func(t *testing.T) {
		cfg := mkSyncCfg(t, "FULL_HOSTNAME = ap1.example.org\nSCHEDD_ADDRESS_FILE = "+filepath.Join(dir, "missing")+"\n")
		if got := resolveMirroredSchedd(cfg).address(); got != "" {
			t.Fatalf("address = %q, want empty", got)
		}
	})
}

// TestMirroredAdAttrsOnlyWithScheddSync: the collector ad names a mirrored schedd exactly when
// schedd-sync runs. A daemon mirroring nothing that still claimed a schedd would be paired with it
// by a hub, and serve an empty queue as that AP's.
func TestMirroredAdAttrsOnlyWithScheddSync(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	svc, err := server.New(server.Config{Dir: t.TempDir(), Authorize: func(_, _, _ string) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = svc.Close() }()
	jobLog := filepath.Join(t.TempDir(), "job_queue.log")
	if err := os.WriteFile(jobLog, []byte("101 1.0 Job Machine\n103 1.0 ProcId 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := &scheddSyncManager{parent: context.Background(), svc: svc, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	defer m.Stop()
	augment := dbad.Augment(svc.Catalog(), m.Sources, nil, nil, "127.0.0.1:1", dbad.WithMirrored(m.Mirrored))
	adAttr := func() (string, bool) {
		ad := classad.New()
		augment(ad)
		return ad.EvaluateAttrString("MirroredScheddName")
	}

	if _, ok := adAttr(); ok {
		t.Fatal("MirroredScheddName advertised with schedd-sync off")
	}
	on := "HTCONDORDB_SYNC_SCHEDD = true\nJOB_QUEUE_LOG = " + jobLog + "\nFULL_HOSTNAME = ap1.example.org\n" +
		"HTCONDORDB_MIRRORED_SCHEDD_NAME = ap1.example.org\nHTCONDORDB_SYNCSTATUS_INTERVAL = 1\n"
	if err := m.apply(mkSyncCfg(t, on)); err != nil {
		t.Fatal(err)
	}
	if v, ok := adAttr(); !ok || v != "ap1.example.org" {
		t.Fatalf("MirroredScheddName = %q (present %v), want ap1.example.org", v, ok)
	}
	// The heartbeat row exists and names the same schedd.
	tbl, ok := svc.Catalog().Table(syncstatus.Table)
	if !ok {
		t.Fatal("no syncstatus table with schedd-sync on")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if row, ok := tbl.LookupClassAd(syncstatus.Key); ok {
			if v, _ := row.EvaluateAttrString(syncstatus.AttrMirroredScheddName); v != "ap1.example.org" {
				t.Fatalf("heartbeat row names %q", v)
			}
			if _, ok := row.EvaluateAttrBool("JobQueueCaughtUp"); !ok {
				t.Fatalf("heartbeat row lacks JobQueueCaughtUp: %s", row)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no heartbeat row written")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := m.apply(mkSyncCfg(t, "")); err != nil {
		t.Fatal(err)
	}
	if _, ok := adAttr(); ok {
		t.Fatal("MirroredScheddName still advertised after schedd-sync was turned off")
	}
}
