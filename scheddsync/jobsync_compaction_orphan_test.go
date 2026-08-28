package scheddsync

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// TestJobSyncCompactionStaleOffsetOrphans reproduces the identity-less orphan rows seen on a busy
// AP (~74k rows with no ClusterId/ProcId/JobStatus, only runtime attributes like ImageSize /
// ResidentSetSize).
//
// Root cause: the syncer resumes at a saved BYTE OFFSET but binds it to the wrong file. It reopens
// job_queue.log by path each poll and detects rotation by inode -- but the inode it trusts is
// sampled by a stat(path) AFTER the read (readAndApply's defer), not an fstat of the fd it read. So
// when the schedd compacts (writes job_queue.log.tmp, all live jobs re-expressed as NewClassAd +
// SetAttributes, then renames it over the path with a bumped op-107 sequence number) in the read ->
// post-stat window, the OLD file's offset gets stamped with the NEW file's inode and persisted.
//
// This test recreates that exact durable artifact: a persisted position pairing a stale offset
// (valid for the pre-compaction file) with the compacted file's identity, where the offset lands
// past a job's NewClassAd. On restart, restore() sees a matching inode and size >= offset and
// resumes in place -- skipping the compacted NewClassAd prefix and applying the trailing
// SetAttributes to keys whose NewClassAd was never read. HTCondor's own C++ tailer avoids this by
// comparing the op-107 sequence number every poll (a differing seq => full re-read from offset 0),
// never trusting a byte offset across a rotation.
//
// The correct behavior: no orphan rows. Job 2.0 must carry its core attributes (it was fully
// present in the compacted log), and job 1.0 (entirely before the stale offset) must not be lost.
func TestJobSyncCompactionStaleOffsetOrphans(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}

	// The compacted log: op-107 header (sequence 2), then the full current state -- every live job
	// re-expressed as NewClassAd + core SetAttributes (ClusterId/ProcId/JobStatus written first, the
	// way a bulk state dump orders them) followed by runtime updates.
	const compacted = "107 2 CreationTimestamp 1000\n" +
		"105\n" +
		"101 1.0 Job Machine\n" +
		"103 1.0 ClusterId 1\n" +
		"103 1.0 ProcId 0\n" +
		"103 1.0 JobStatus 2\n" +
		"106\n" +
		"105\n" +
		"101 2.0 Job Machine\n" +
		"103 2.0 ClusterId 2\n" +
		"103 2.0 ProcId 0\n" +
		"103 2.0 JobStatus 2\n" +
		"103 2.0 ImageSize 500000\n" +
		"103 2.0 ResidentSetSize 400000\n" +
		"106\n"
	writeFile(t, logPath, compacted)

	// The stale offset a mid-read compaction TOCTOU would persist: a byte offset (valid for the
	// pre-compaction file) that, in the compacted file, lands PAST job 2.0's NewClassAd and its core
	// attributes -- right before its runtime SetAttributes. Resuming here reads only those runtime
	// Sets, applied to key "2.0" whose NewClassAd (and ClusterId/ProcId/JobStatus) sit before the
	// offset and are never read.
	staleOffset := int64(strings.Index(compacted, "103 2.0 ImageSize"))
	if staleOffset <= 0 {
		t.Fatal("could not locate the stale-offset marker in the compacted log")
	}

	cur, err := statIdentity(logPath)
	if err != nil {
		t.Fatal(err)
	}
	// The persisted position pairs the stale offset with the sequence number of the PRE-compaction
	// file (seq 1); the compacted file on disk is seq 2. That mismatch is the signal a correct
	// resume must act on -- the inode may match (reuse) and the size may be >= the offset, but the
	// seq proves the offset belongs to a different file.
	blob, err := jobPosition{File: cur, Offset: staleOffset, Seq: 1}.encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(blob); err != nil {
		t.Fatal(err)
	}

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	// Restart into the compacted log with the stale-offset checkpoint.
	s := restartJobSync(target, logPath, store)
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The bug's signature: an identity-less orphan under "2.0" (only runtime attributes), and job
	// 1.0 lost entirely. A correct rotation-aware resume re-reads from the head and yields both jobs
	// complete.
	ad2, ok := target.LookupClassAd("2.0")
	if !ok {
		t.Fatal("job 2.0 missing entirely")
	}
	if _, hasStatus := ad2.EvaluateAttrInt("JobStatus"); !hasStatus {
		t.Errorf("job 2.0 is an identity-less orphan: JobStatus is undefined (its NewClassAd and core "+
			"attributes were skipped by resuming at a stale offset into the compacted log)")
	}
	if _, hasCluster := ad2.EvaluateAttrInt("ClusterId"); !hasCluster {
		t.Errorf("job 2.0 is an identity-less orphan: ClusterId is undefined")
	}
	if _, ok := target.LookupClassAd("1.0"); !ok {
		t.Errorf("job 1.0 was lost: its records sit before the stale offset and were never re-read")
	}
}

// TestJobSyncInPlaceRewriteSeqBump covers the live rotation case the inode check structurally
// cannot see: the schedd rewrites the log IN PLACE (same inode) with a bumped op-107 sequence
// number and a larger size. Same inode + grown size => both the inode and the size-prober checks
// pass, so only the sequence-number check reveals the rotation. Its absence is observable as a
// ghost: a job that completed (dropped from the compacted state) is only removed by a full
// reconcile, never by resuming and appending -- so without the seq check the completed job lingers.
func TestJobSyncInPlaceRewriteSeqBump(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")

	// v1: sequence 1, two jobs. Consumed fully by the first poll.
	writeFile(t, logPath, "107 1 CreationTimestamp 1000\n"+
		"105\n101 1.0 Job Machine\n103 1.0 ClusterId 1\n103 1.0 JobStatus 2\n106\n"+
		"105\n101 2.0 Job Machine\n103 2.0 ClusterId 2\n103 2.0 JobStatus 2\n106\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	s := NewJobSync(target, JobSyncConfig{Filename: logPath})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if target.Len() != 2 {
		t.Fatalf("after first sync Len = %d, want 2", target.Len())
	}
	beforeInode, err := statIdentity(logPath)
	if err != nil {
		t.Fatal(err)
	}

	// Compaction rewritten IN PLACE (os.WriteFile truncates, keeping the inode): sequence 2. Job 2.0
	// completed and is gone; 1.0 remains with extra runtime attributes, and 3.0 is new -- so the new
	// file is LARGER than v1 despite dropping a job. Same inode + larger size defeats both the inode
	// and size checks; only the bumped sequence number reveals the rotation.
	writeFile(t, logPath, "107 2 CreationTimestamp 1000\n"+
		"105\n101 1.0 Job Machine\n103 1.0 ClusterId 1\n103 1.0 JobStatus 2\n"+
		"103 1.0 ImageSize 500000\n103 1.0 ResidentSetSize 400000\n106\n"+
		"105\n101 3.0 Job Machine\n103 3.0 ClusterId 3\n103 3.0 JobStatus 2\n"+
		"103 3.0 ImageSize 600000\n103 3.0 ResidentSetSize 500000\n106\n")
	afterInode, err := statIdentity(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !sameFileIdentity(beforeInode, afterInode) {
		t.Skip("in-place rewrite changed the inode on this platform; the inode check would catch it, " +
			"so this test cannot isolate the sequence-number check")
	}
	if afterInode.Size <= beforeInode.Size {
		t.Fatalf("rewrite did not grow the file (%d -> %d); the size prober would catch a shrink, so "+
			"this test would not isolate the sequence-number check", beforeInode.Size, afterInode.Size)
	}

	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 2.0 completed: it must be gone. Without the sequence-number check the syncer would resume-and-
	// append (inode + size both look like a plain append) and never learn 2.0 was dropped -- a ghost.
	if _, ghost := target.LookupClassAd("2.0"); ghost {
		t.Error("job 2.0 is a ghost after an in-place seq-bumped rewrite: the sequence-number check " +
			"did not fire, so the syncer resumed instead of reconciling and never dropped the completed job")
	}
	if _, ok := target.LookupClassAd("1.0"); !ok {
		t.Error("job 1.0 (surviving) was lost after the in-place rewrite")
	}
	if _, ok := target.LookupClassAd("3.0"); !ok {
		t.Error("job 3.0 (new) was not picked up after the in-place rewrite")
	}
}
