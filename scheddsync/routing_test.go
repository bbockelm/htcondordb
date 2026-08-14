package scheddsync

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// routingLog exercises every job_queue.log namespace in one small log: the schedd header (0.0),
// a user/owner record (0.1), a cluster ad (01.-1 carrying Owner "alice"), a jobset ad (1.-100),
// a cluster-private ad (1.-2), an OCU ad (1.-99, still dropped), and a real proc ad (1.0). The
// cluster ad's Owner is set BEFORE the proc is created so the proc chains it out of the clusters
// table (the reverse-order path).
const routingLog = `101 0.0 Job Machine
103 0.0 NextClusterNum 2
101 0.1 Owner (unknown)
103 0.1 Name "alice"
101 01.-1 Job Machine
103 01.-1 Owner "alice"
101 1.-100 JobSet (unknown)
103 1.-100 JobSetName "myset"
101 1.-2 ClusterPvt (unknown)
103 1.-2 Secret "s3cr3t"
101 1.-99 OCU (unknown)
101 1.0 Job Machine
103 1.0 ProcId 0
103 1.0 ClusterId 1
`

func TestRoutingPoll(t *testing.T)      { assertRouting(t, false) }
func TestRoutingReconcile(t *testing.T) { assertRouting(t, true) }

// assertRouting replays routingLog through either the incremental (Poll -> applyEntry) path or the
// reconcile-reload path and verifies each namespace landed in exactly the right table, that the
// header and cluster-private ads route to their own tables, that the OCU ad is dropped everywhere,
// and that the proc chained its cluster's Owner.
func assertRouting(t *testing.T, reconcile bool) {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, routingLog)

	jobs := openMem(t)
	users := openMem(t)
	jobsets := openMem(t)
	clusters := openMem(t)
	header := openMem(t)
	clusterprivate := openMem(t)

	s := NewJobSync(jobs, JobSyncConfig{
		Filename: logPath, Users: users, Jobsets: jobsets, Clusters: clusters, Header: header,
		ClusterPrivate: clusterprivate,
	})
	var err error
	if reconcile {
		err = s.reconcileReload(context.Background())
	} else {
		err = s.Poll(context.Background())
	}
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	wantKeys(t, "jobs", jobs, "1.0")
	wantKeys(t, "users", users, "0.1")
	wantKeys(t, "jobsets", jobsets, "1.-100")
	wantKeys(t, "clusters", clusters, "01.-1")
	wantKeys(t, "header", header, "0.0")
	wantKeys(t, "clusterprivate", clusterprivate, "1.-2")

	// The OCU ad (1.-99) is dropped from every table; no ad belongs in a table other than its own.
	for _, tbl := range []struct {
		name string
		db   *db.DB
	}{{"jobs", jobs}, {"users", users}, {"jobsets", jobsets}, {"clusters", clusters}, {"header", header}, {"clusterprivate", clusterprivate}} {
		if _, ok := tbl.db.LookupClassAd("1.-99"); ok {
			t.Errorf("OCU ad 1.-99 should be dropped but is present in %s table", tbl.name)
		}
	}

	// The proc row inherits the cluster ad's Owner ("alice") even though it was written only on
	// the cluster ad, before the proc existed.
	proc, ok := jobs.LookupClassAd("1.0")
	if !ok {
		t.Fatal("proc 1.0 missing from jobs table")
	}
	if v, _ := proc.EvaluateAttrString("Owner"); v != "alice" {
		t.Errorf("1.0 Owner = %q, want %q (chained from the cluster ad)", v, "alice")
	}
}

func openMem(t *testing.T) *db.DB {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

// wantKeys asserts table holds exactly the given keys.
func wantKeys(t *testing.T, name string, table *db.DB, keys ...string) {
	t.Helper()
	got := table.Keys()
	sort.Strings(got)
	want := append([]string(nil), keys...)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Errorf("%s keys = %v, want %v", name, got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s keys = %v, want %v", name, got, want)
			return
		}
	}
}
