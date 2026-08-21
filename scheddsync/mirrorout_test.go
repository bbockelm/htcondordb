package scheddsync

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// mirrorConfig builds a MirrorOutConfig writing to out from a namespace set.
func (n ns) mirrorConfig(out string) MirrorOutConfig {
	return MirrorOutConfig{
		OutPath: out,
		Jobs:    n.jobs, Users: n.users, Jobsets: n.jobsets, Clusters: n.clusters,
		Header: n.header, ClusterPrivate: n.clusterprivate, LogMeta: n.logmeta,
	}
}

// TestMirrorOutRoundTrip proves the follower path: tables -> MirrorOut file -> tables. An
// original log is mirrored into a first table set, MirrorOut regenerates a job_queue.log from
// it, and re-ingesting that file into a second table set reproduces every namespace exactly.
func TestMirrorOutRoundTrip(t *testing.T) {
	dir := t.TempDir()
	orig := filepath.Join(dir, "job_queue.log")
	writeFile(t, orig, `107 7 CreationTimestamp 1700000000
101 0.0 Header (unknown)
103 0.0 NextClusterNum 2
101 01.-1 Job Machine
103 01.-1 Owner "alice"
103 01.-1 SharedAttr 42
101 1.-2 ClusterPvt (unknown)
103 1.-2 Secret "x"
101 1.0 Job Machine
103 1.0 ProcId 0
103 1.0 ClusterId 1
103 1.0 SharedAttr 99
`)
	n1 := tables(t)
	syncLog(t, orig, n1)

	out := filepath.Join(dir, "mirror", "job_queue.log")
	m := NewMirrorOut(n1.mirrorConfig(out))
	if err := m.Regenerate(); err != nil {
		t.Fatalf("regenerate: %v", err)
	}

	// The write is atomic: no leftover temp file, and the destination exists.
	if _, err := os.Stat(out + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("temp file %s.tmp was left behind", out)
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatalf("mirror output missing: %v", err)
	}

	n2 := tables(t)
	syncLog(t, out, n2)

	assertTablesEqual(t, "jobs", n1.jobs, n2.jobs)
	assertTablesEqual(t, "users", n1.users, n2.users)
	assertTablesEqual(t, "jobsets", n1.jobsets, n2.jobsets)
	assertTablesEqual(t, "clusters", n1.clusters, n2.clusters)
	assertTablesEqual(t, "header", n1.header, n2.header)
	assertTablesEqual(t, "clusterprivate", n1.clusterprivate, n2.clusterprivate)
	assertTablesEqual(t, "logmeta", n1.logmeta, n2.logmeta)

	// Status reflects the successful cycle.
	if st := m.Status(); st.Source != out || !st.CaughtUp || st.FileSize <= 0 {
		t.Errorf("status after regenerate = %+v, want Source=%q CaughtUp size>0", st, out)
	}
}

// TestMirrorOutRunRegeneratesLiveUpdates checks the Run loop keeps the output current: after a
// new job appears in the tables, a later cycle includes it, and cancelling Run stops it.
func TestMirrorOutRunRegeneratesLiveUpdates(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "job_queue.log")
	writeFile(t, src, "107 1 CreationTimestamp 0\n101 1.0 Job Machine\n103 1.0 ProcId 0\n103 1.0 ClusterId 1\n")
	n := tables(t)
	syncLog(t, src, n)

	out := filepath.Join(dir, "job_queue.mirror.log")
	m := NewMirrorOut(n.mirrorConfig(out))
	m.interval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = m.Run(ctx) }()

	// First cycle (immediate) must produce the one job.
	waitFor(t, 2*time.Second, func() bool {
		out2 := tables(t)
		if !fileExists(out) {
			return false
		}
		syncLog(t, out, out2)
		return out2.jobs.Len() == 1
	})

	// Add a second job to the source and re-mirror it into the tables; a later cycle must pick it up.
	appendFile(t, src, "101 2.0 Job Machine\n103 2.0 ProcId 0\n103 2.0 ClusterId 2\n")
	syncLog(t, src, n)
	waitFor(t, 2*time.Second, func() bool {
		out2 := tables(t)
		syncLog(t, out, out2)
		return out2.jobs.Len() == 2
	})

	cancel()
	<-runDone
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
