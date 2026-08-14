package scheddsync

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// ns holds the five in-memory namespace tables JobSync routes a job_queue.log into.
type ns struct{ jobs, users, jobsets, clusters, header *db.DB }

// tables opens a fresh set of in-memory namespace tables.
func tables(t *testing.T) ns {
	t.Helper()
	open := func() *db.DB {
		d, err := db.Open("")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { d.Close() })
		return d
	}
	return ns{open(), open(), open(), open(), open()}
}

// syncLog replays logPath into the given tables with a one-shot JobSync poll.
func syncLog(t *testing.T, logPath string, n ns) {
	t.Helper()
	s := NewJobSync(n.jobs, JobSyncConfig{
		Filename: logPath, Users: n.users, Jobsets: n.jobsets, Clusters: n.clusters, Header: n.header,
	})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll %s: %v", logPath, err)
	}
}

// writer builds a QueueLogWriter over a namespace set.
func (n ns) writer() *QueueLogWriter {
	return &QueueLogWriter{Jobs: n.jobs, Users: n.users, Jobsets: n.jobsets, Clusters: n.clusters, Header: n.header}
}

// assertTablesEqual checks two tables hold the same keys with Equal ads.
func assertTablesEqual(t *testing.T, name string, want, got *db.DB) {
	t.Helper()
	wantKeys := want.Keys()
	if len(wantKeys) != len(got.Keys()) {
		t.Fatalf("%s: key count %d -> %d after round-trip", name, len(wantKeys), len(got.Keys()))
	}
	for _, k := range wantKeys {
		a, ok := want.LookupClassAd(k)
		if !ok {
			t.Fatalf("%s: source missing key %s", name, k)
		}
		b, ok := got.LookupClassAd(k)
		if !ok {
			t.Fatalf("%s: round-tripped table missing key %s", name, k)
		}
		if !a.Equal(b) {
			t.Fatalf("%s key %s diverged after round-trip:\n before: %s\n after:  %s", name, k, a.String(), b.String())
		}
	}
}

// TestQueueLogRoundTrip proves job_queue.log -> htcondordb -> job_queue.log preserves the
// mirrored queue state: an original log is ingested into the four tables, the tables are
// reserialized back to a fresh log, that log is ingested into a second set of tables, and
// the two sets must be identical. The fixture exercises the tricky cases: a proc that
// OVERRIDES a chained cluster attribute (SharedAttr), cluster/jobset/user namespaces, a
// dropped header ad, and an expression-valued attribute (Requirements).
func TestQueueLogRoundTrip(t *testing.T) {
	dir := t.TempDir()
	orig := filepath.Join(dir, "job_queue.log")
	writeFile(t, orig, `105
101 0.0 Header (unknown)
103 0.0 NextClusterNum 3
101 0.1 Owner (unknown)
103 0.1 User "alice"
103 0.1 NumJobs 2
101 01.-1 Job Machine
103 01.-1 Owner "alice"
103 01.-1 Requirements (OpSys == "LINUX") && (Arch == "X86_64")
103 01.-1 SharedAttr 42
101 1.0 Job Machine
103 1.0 ProcId 0
103 1.0 ClusterId 1
103 1.0 JobStatus 2
103 1.0 SharedAttr 99
101 1.1 Job Machine
103 1.1 ProcId 1
103 1.1 ClusterId 1
103 1.1 JobStatus 1
101 1.-100 JobSet (unknown)
103 1.-100 JobSetName "myset"
106
`)

	// Forward: original log -> tables (DB1).
	n1 := tables(t)
	syncLog(t, orig, n1)

	// Sanity on the ingest itself: the proc override survived chaining, the header ad landed
	// in the header table (not jobs), and its queue counter was captured.
	if n1.jobs.Len() != 2 {
		t.Fatalf("jobs Len = %d, want 2", n1.jobs.Len())
	}
	if ad, ok := n1.jobs.LookupClassAd("1.0"); !ok {
		t.Fatal("job 1.0 missing after ingest")
	} else if v, _ := ad.EvaluateAttrInt("SharedAttr"); v != 99 {
		t.Fatalf("1.0 SharedAttr = %d, want 99 (proc override of cluster's 42)", v)
	} else if v, _ := ad.EvaluateAttrString("Owner"); v != "alice" {
		t.Fatalf("1.0 Owner = %q, want alice (chained from cluster)", v)
	}
	if _, ok := n1.jobs.LookupClassAd("0.0"); ok {
		t.Error("header ad 0.0 leaked into the jobs table")
	}
	if ad, ok := n1.header.LookupClassAd("0.0"); !ok {
		t.Fatal("header ad 0.0 missing from the header table")
	} else if v, _ := ad.EvaluateAttrInt("NextClusterNum"); v != 3 {
		t.Fatalf("header NextClusterNum = %d, want 3", v)
	}

	// Reverse: tables -> reconstructed log.
	recon := filepath.Join(dir, "job_queue.reconstructed.log")
	if err := n1.writer().WriteFile(recon); err != nil {
		t.Fatalf("reconstruct log: %v", err)
	}

	// Forward again: reconstructed log -> tables (DB2).
	n2 := tables(t)
	syncLog(t, recon, n2)

	// The round-trip must preserve every namespace exactly -- including the header.
	assertTablesEqual(t, "jobs", n1.jobs, n2.jobs)
	assertTablesEqual(t, "users", n1.users, n2.users)
	assertTablesEqual(t, "jobsets", n1.jobsets, n2.jobsets)
	assertTablesEqual(t, "clusters", n1.clusters, n2.clusters)
	assertTablesEqual(t, "header", n1.header, n2.header)
}

// TestQueueLogRoundTripStable proves the reconstruction is a fixed point: reserializing the
// second-generation tables yields byte-for-byte the same log as the first reconstruction. A
// stable serialization is what lets the log be diffed or checksummed as a backup artifact.
func TestQueueLogRoundTripStable(t *testing.T) {
	dir := t.TempDir()
	orig := filepath.Join(dir, "job_queue.log")
	writeFile(t, orig, `105
101 01.-1 Job Machine
103 01.-1 Owner "bob"
103 01.-1 SharedAttr 7
101 1.0 Job Machine
103 1.0 ProcId 0
103 1.0 ClusterId 1
101 2.0 Job Machine
103 2.0 ProcId 0
103 2.0 ClusterId 2
106
`)
	n1 := tables(t)
	syncLog(t, orig, n1)

	var buf1 bytes.Buffer
	if _, err := n1.writer().WriteTo(&buf1); err != nil {
		t.Fatalf("first reconstruction: %v", err)
	}

	recon := filepath.Join(dir, "recon.log")
	writeFile(t, recon, buf1.String())
	n2 := tables(t)
	syncLog(t, recon, n2)

	var buf2 bytes.Buffer
	if _, err := n2.writer().WriteTo(&buf2); err != nil {
		t.Fatalf("second reconstruction: %v", err)
	}

	if buf1.String() != buf2.String() {
		t.Fatalf("reconstruction not a fixed point:\n--- gen1 ---\n%s\n--- gen2 ---\n%s", buf1.String(), buf2.String())
	}
}
