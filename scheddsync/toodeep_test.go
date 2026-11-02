package scheddsync

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/wire"
)

// deepExpr is a value that parses but nests deeper than the store can hold: a chain of one binary
// operator parses left-nested, so N terms reach depth N-1.
func deepExpr() string {
	return strings.Repeat("x || ", wire.MaxDepth+10) + "x"
}

// A job_queue.log SetAttribute whose value is too deep must cost that one attribute, not the job,
// and must be counted apart from an unparseable value (it parses fine).
func TestTooDeepSetAttributeSkippedIncremental(t *testing.T) {
	d := persistentDB(t)
	logPath := filepath.Join(t.TempDir(), "job_queue.log")
	writeFile(t, logPath, "107 1 CreationTimestamp 1000\n"+
		"105 \n101 1.0 Job Machine\n103 1.0 ClusterId 1\n103 1.0 Deep "+deepExpr()+"\n103 1.0 JobStatus 2\n106 \n")
	s := NewJobSync(d, JobSyncConfig{Filename: logPath})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	ad, ok := d.LookupClassAd("1.0")
	if !ok {
		t.Fatal("job 1.0 is missing: one too-deep attribute dropped the whole job")
	}
	if v, ok := ad.EvaluateAttrInt("JobStatus"); !ok || v != 2 {
		t.Errorf("JobStatus = %v (ok=%v), want 2: the attributes after the refused one were lost", v, ok)
	}
	if _, has := ad.Lookup("Deep"); has {
		t.Error("the too-deep attribute was stored")
	}
	if got := s.Status().TooDeep; got != 1 {
		t.Errorf("Status().TooDeep = %d, want 1", got)
	}
	if got := s.Status().Unapplied; got != 0 {
		t.Errorf("Status().Unapplied = %d, want 0", got)
	}
}

// A reconcile replays the log into whole-ad writes, and the store refuses a whole ad carrying one
// too-deep value outright -- keeping the stale row and losing every other update in the run. The
// reconcile must strip the value and land the rest.
func TestTooDeepReconcileKeepsRestOfJob(t *testing.T) {
	d := persistentDB(t)
	logPath := filepath.Join(t.TempDir(), "job_queue.log")
	writeFile(t, logPath, "107 1 CreationTimestamp 1000\n"+
		"105 \n101 1.0 Job Machine\n103 1.0 ClusterId 1\n103 1.0 JobStatus 1\n106 \n")
	s := NewJobSync(d, JobSyncConfig{Filename: logPath})
	ctx := context.Background()
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	// Two values in one run, so the strip has to find more than the first offender.
	appendFile(t, logPath, "105 \n103 1.0 Deep "+deepExpr()+"\n103 1.0 JobStatus 2\n103 1.0 Deeper "+deepExpr()+"\n106 \n")
	s.Resync()
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	ad, ok := d.LookupClassAd("1.0")
	if !ok {
		t.Fatal("job 1.0 is missing after the reconcile")
	}
	if v, ok := ad.EvaluateAttrInt("JobStatus"); !ok || v != 2 {
		t.Errorf("JobStatus = %v (ok=%v), want 2: the reconcile's write of the row was refused whole", v, ok)
	}
	for _, name := range []string{"Deep", "Deeper"} {
		if _, has := ad.Lookup(name); has {
			t.Errorf("the too-deep attribute %s was stored", name)
		}
	}
	if got := s.Status().TooDeep; got != 2 {
		t.Errorf("Status().TooDeep = %d, want 2", got)
	}
	if got := s.Status().Unapplied; got != 0 {
		t.Errorf("Status().Unapplied = %d, want 0: a write was refused instead of stripped", got)
	}
}

func TestStripTooDeep(t *testing.T) {
	ad := classad.New()
	ad.InsertAttr("A", int64(1))
	for _, name := range []string{"Deep1", "Deep2"} {
		e, err := classad.ParseExpr(deepExpr())
		if err != nil {
			t.Fatal(err)
		}
		ad.InsertExpr(name, e)
	}
	// Exactly at the limit fits and must be kept.
	atLimit, err := classad.ParseExpr(strings.Repeat("x || ", wire.MaxDepth) + "x")
	if err != nil {
		t.Fatal(err)
	}
	ad.InsertExpr("AtLimit", atLimit)

	got := stripTooDeep(ad)
	if len(got) != 2 {
		t.Fatalf("stripTooDeep = %v, want Deep1 and Deep2", got)
	}
	for _, name := range []string{"Deep1", "Deep2"} {
		if _, has := ad.Lookup(name); has {
			t.Errorf("%s still present", name)
		}
	}
	for _, name := range []string{"A", "AtLimit"} {
		if _, has := ad.Lookup(name); !has {
			t.Errorf("%s was stripped but fits", name)
		}
	}
	if err := wire.CheckDepth(ad.AST()); err != nil {
		t.Errorf("stripped ad still fails CheckDepth: %v", err)
	}
}

// A history record holding a too-deep value is skipped and counted; the records around it land.
func TestTooDeepHistoryRecordSkipped(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	histPath := filepath.Join(t.TempDir(), "history")
	deep := "Owner = \"u\"\nClusterId = 2\nProcId = 0\nJobStatus = 4\nDeep = " + deepExpr() +
		"\n*** Offset = 0 ClusterId = 2 ProcId = 0\n"
	writeFile(t, histPath, histRecord(1, 0, 4)+deep+histRecord(3, 0, 4))
	s := NewHistorySync(arch, HistorySyncConfig{Filename: histPath})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 2 {
		t.Fatalf("Count = %d, want 2 (records 1 and 3)", arch.Count())
	}
	if got := s.Status().TooDeep; got != 1 {
		t.Errorf("Status().TooDeep = %d, want 1", got)
	}
}
