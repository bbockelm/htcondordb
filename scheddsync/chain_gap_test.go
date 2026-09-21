package scheddsync

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// A proc row inherits its cluster ad's attributes two ways: chainFromParent at the proc's
// OpNewClassAd, and a fan-out to s.children when a later OpSetAttribute edits the cluster. On a
// live OSPool mirror ~2,800 proc rows were missing exactly their cluster's attributes (ClusterId,
// JobStatus, Owner, QDate) while the cluster ad held them and sibling procs had them -- and 1,470
// of those were ProcId 0, the first proc of a cluster.
//
// s.children is IN-MEMORY, rebuilt only as OpNewClassAd entries are applied. A proc registered
// before a restart is not registered after one, so a cluster attribute logged after the restart
// has nothing to fan out to. These tests pin each ordering separately so the failing one is named
// rather than inferred.

type chainNodes struct{ jobs, users, jobsets, clusters, header, clusterprivate *db.DB }

func newChainNodes(t *testing.T) chainNodes {
	t.Helper()
	open := func() *db.DB {
		d, err := db.OpenConfig(db.Config{Dir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = d.Close() })
		return d
	}
	return chainNodes{open(), open(), open(), open(), open(), open()}
}

func (n chainNodes) sync(t *testing.T, logPath string, store PositionStore) *JobSync {
	t.Helper()
	return NewJobSync(n.jobs, JobSyncConfig{
		Filename: logPath, Users: n.users, Jobsets: n.jobsets,
		Clusters: n.clusters, Header: n.header, ClusterPrivate: n.clusterprivate,
		Store: store,
	})
}

// procHas reports whether the proc row carries the attribute.
func procHas(t *testing.T, n chainNodes, key, attr string) bool {
	t.Helper()
	ad, ok := n.jobs.LookupClassAd(key)
	if !ok {
		t.Fatalf("proc %s missing entirely", key)
	}
	_, has := ad.Lookup(attr)
	return has
}

// TestChainClusterAttrsAcrossOrderings covers the two in-process orderings, which both work, and
// the restart case, which is the one under suspicion.
func TestChainClusterAttrsAcrossOrderings(t *testing.T) {
	t.Run("cluster attrs before the proc", func(t *testing.T) {
		n := newChainNodes(t)
		dir := t.TempDir()
		logPath := filepath.Join(dir, "job_queue.log")
		writeFile(t, logPath, "107 1 CreationTimestamp 1000\n"+
			"105 \n101 01.-1 Job Machine\n103 01.-1 Owner \"alice\"\n103 01.-1 JobStatus 1\n106 \n"+
			"105 \n101 1.0 Job Machine\n103 1.0 ProcId 0\n106 \n")
		s := n.sync(t, logPath, nil)
		if err := s.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !procHas(t, n, "1.0", "Owner") {
			t.Error("proc did not inherit Owner when the cluster was complete first")
		}
	})

	t.Run("proc before the cluster attrs, same process", func(t *testing.T) {
		n := newChainNodes(t)
		dir := t.TempDir()
		logPath := filepath.Join(dir, "job_queue.log")
		// proc 0 first: chainFromParent finds an empty (or absent) cluster, so the fan-out on the
		// later SetAttribute is what has to deliver the attributes.
		writeFile(t, logPath, "107 1 CreationTimestamp 1000\n"+
			"105 \n101 1.0 Job Machine\n103 1.0 ProcId 0\n106 \n"+
			"105 \n101 01.-1 Job Machine\n103 01.-1 Owner \"alice\"\n103 01.-1 JobStatus 1\n106 \n")
		s := n.sync(t, logPath, nil)
		if err := s.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !procHas(t, n, "1.0", "Owner") {
			t.Error("proc did not inherit Owner via the fan-out when it preceded the cluster")
		}
	})

	t.Run("RESTART between the proc and the cluster attrs", func(t *testing.T) {
		n := newChainNodes(t)
		dir := t.TempDir()
		logPath := filepath.Join(dir, "job_queue.log")
		// Pass 1: proc 0 exists, cluster ad exists but carries nothing yet.
		writeFile(t, logPath, "107 1 CreationTimestamp 1000\n"+
			"105 \n101 01.-1 Job Machine\n106 \n"+
			"105 \n101 1.0 Job Machine\n103 1.0 ProcId 0\n106 \n")
		// A position store, so the second JobSync RESUMES where the first stopped instead of
		// replaying from offset 0. Without it restore() is a no-op ("persistence disabled: replay
		// from the start each run"), the whole log is re-read, the proc's OpNewClassAd is applied
		// again and s.children is rebuilt -- which is why the first version of this subtest passed
		// while proving nothing.
		store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}
		s1 := n.sync(t, logPath, store)
		if err := s1.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if procHas(t, n, "1.0", "Owner") {
			t.Fatal("proc already has Owner before the cluster set it; the fixture is wrong")
		}

		// The daemon restarts: a NEW JobSync over the SAME tables and log, resuming where it left
		// off. s.children starts empty, so proc 1.0 is no longer registered as a child.
		s2 := n.sync(t, logPath, store)

		// Pass 2: the cluster gets its attributes.
		appendFile(t, logPath, "105 \n103 01.-1 Owner \"alice\"\n103 01.-1 JobStatus 1\n106 \n")
		if err := s2.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !procHas(t, n, "1.0", "Owner") {
			t.Error("REPRODUCED: after a restart the cluster's attributes never reached the proc " +
				"-- s.children is in-memory, so the proc is no longer a registered child")
		}
	})
}
