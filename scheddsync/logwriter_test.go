package scheddsync

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// tables opens the four in-memory namespace tables JobSync routes into.
func tables(t *testing.T) (jobs, users, jobsets, clusters *db.DB) {
	t.Helper()
	open := func() *db.DB {
		d, err := db.Open("")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { d.Close() })
		return d
	}
	return open(), open(), open(), open()
}

// syncLog replays logPath into the given tables with a one-shot JobSync poll.
func syncLog(t *testing.T, logPath string, jobs, users, jobsets, clusters *db.DB) {
	t.Helper()
	s := NewJobSync(jobs, JobSyncConfig{
		Filename: logPath, Users: users, Jobsets: jobsets, Clusters: clusters,
	})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll %s: %v", logPath, err)
	}
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
	jobs1, users1, jobsets1, clusters1 := tables(t)
	syncLog(t, orig, jobs1, users1, jobsets1, clusters1)

	// Sanity on the ingest itself: the proc override survived chaining, and the dropped
	// header did not land in any table.
	if jobs1.Len() != 2 {
		t.Fatalf("jobs Len = %d, want 2", jobs1.Len())
	}
	if ad, ok := jobs1.LookupClassAd("1.0"); !ok {
		t.Fatal("job 1.0 missing after ingest")
	} else if v, _ := ad.EvaluateAttrInt("SharedAttr"); v != 99 {
		t.Fatalf("1.0 SharedAttr = %d, want 99 (proc override of cluster's 42)", v)
	} else if v, _ := ad.EvaluateAttrString("Owner"); v != "alice" {
		t.Fatalf("1.0 Owner = %q, want alice (chained from cluster)", v)
	}
	if _, ok := jobs1.LookupClassAd("0.0"); ok {
		t.Error("header ad 0.0 leaked into the jobs table")
	}

	// Reverse: tables -> reconstructed log.
	recon := filepath.Join(dir, "job_queue.reconstructed.log")
	w := &QueueLogWriter{Jobs: jobs1, Users: users1, Jobsets: jobsets1, Clusters: clusters1}
	if err := w.WriteFile(recon); err != nil {
		t.Fatalf("reconstruct log: %v", err)
	}

	// Forward again: reconstructed log -> tables (DB2).
	jobs2, users2, jobsets2, clusters2 := tables(t)
	syncLog(t, recon, jobs2, users2, jobsets2, clusters2)

	// The round-trip must preserve every namespace exactly.
	assertTablesEqual(t, "jobs", jobs1, jobs2)
	assertTablesEqual(t, "users", users1, users2)
	assertTablesEqual(t, "jobsets", jobsets1, jobsets2)
	assertTablesEqual(t, "clusters", clusters1, clusters2)
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
	jobs1, users1, jobsets1, clusters1 := tables(t)
	syncLog(t, orig, jobs1, users1, jobsets1, clusters1)

	var buf1 bytes.Buffer
	w1 := &QueueLogWriter{Jobs: jobs1, Users: users1, Jobsets: jobsets1, Clusters: clusters1}
	if _, err := w1.WriteTo(&buf1); err != nil {
		t.Fatalf("first reconstruction: %v", err)
	}

	recon := filepath.Join(dir, "recon.log")
	writeFile(t, recon, buf1.String())
	jobs2, users2, jobsets2, clusters2 := tables(t)
	syncLog(t, recon, jobs2, users2, jobsets2, clusters2)

	var buf2 bytes.Buffer
	w2 := &QueueLogWriter{Jobs: jobs2, Users: users2, Jobsets: jobsets2, Clusters: clusters2}
	if _, err := w2.WriteTo(&buf2); err != nil {
		t.Fatalf("second reconstruction: %v", err)
	}

	if buf1.String() != buf2.String() {
		t.Fatalf("reconstruction not a fixed point:\n--- gen1 ---\n%s\n--- gen2 ---\n%s", buf1.String(), buf2.String())
	}
}
