package scheddsync

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
)

// usageOnly is a shadow update that carries no starter clock, so its sample's SampleTime is the
// ingest clock.
func usageOnly(memMB int) string {
	return "105\n103 1.0 MemoryUsage " + strconv.Itoa(memMB) + "\n106\n"
}

// TestJobMetricsSamplePosition: every sample carries the log position of the commit it came from
// (LogSeq, LogOffset), and that position is what tells two samples apart when everything else a
// reader might call identity -- job, run, SampleTime, trigger -- is the same: ingest-clock samples
// of one job read in one pass share a SampleTime. A federation hub dedups replayed samples by it.
func TestJobMetricsSamplePosition(t *testing.T) {
	pinClock(t, base+50)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, "107 3 CreationTimestamp 1700000000\n"+submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	if err := s.Poll(context.Background()); err != nil { // the first pass learns the generation
		t.Fatalf("poll: %v", err)
	}
	appendFile(t, logPath, spawned(1)+usageOnly(100)+usageOnly(200))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	got := samples(t, arch)
	if len(got) != 3 {
		t.Fatalf("%d samples, want 3 (spawn + two updates)", len(got))
	}
	a, b := got[1], got[2]
	for _, attr := range []string{SampleTimeAttr, RunInstanceAttr} {
		if num(t, a, attr) != num(t, b, attr) {
			t.Fatalf("precondition: the two updates differ in %s", attr)
		}
	}
	if trig(t, a) != trig(t, b) {
		t.Fatalf("precondition: the two updates differ in trigger (%q, %q)", trig(t, a), trig(t, b))
	}
	var prev float64
	for i, ad := range got {
		if seq := num(t, ad, SampleLogSeqAttr); seq != 3 {
			t.Errorf("sample %d LogSeq = %v, want 3", i, seq)
		}
		off := num(t, ad, SampleLogOffsetAttr)
		if off <= prev {
			t.Errorf("sample %d LogOffset = %v, not past the previous sample's %v", i, off, prev)
		}
		prev = off
	}
}

// TestJobMetricsUnchainedSampleHasJobID: a proc row that never got its cluster ad chained has no
// ClusterId; its sample still names the job by ClusterId and ProcId, taken from the key -- what a
// reader selects a job by, and half of a federation hub's structured job id. User and GlobalJobId
// cannot be derived and stay absent (a row an owner-scoped reader cannot see).
func TestJobMetricsUnchainedSampleHasJobID(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, "105\n101 7.3 Job Machine\n103 7.3 Owner \"alice\"\n103 7.3 JobStatus 1\n106\n")
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	appendFile(t, logPath, "105\n103 7.3 JobStatus 2\n103 7.3 NumShadowStarts 1\n106\n")
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	got := samples(t, arch)
	if len(got) == 0 {
		t.Fatal("no sample")
	}
	for i, ad := range got {
		if c, p := num(t, ad, "ClusterId"), num(t, ad, "ProcId"); c != 7 || p != 3 {
			t.Errorf("sample %d job id = %v.%v, want 7.3", i, c, p)
		}
		if _, ok := ad.EvaluateAttrString("User"); ok {
			t.Errorf("sample %d has a User the job never had", i)
		}
	}
}

// TestJobMetricsSampleCarriesUser: User -- what an owner-scoped reader selects by -- is copied
// from the job onto every sample, with GlobalJobId, ClusterId and ProcId.
func TestJobMetricsSampleCarriesUser(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted+"105\n103 1.0 User \"alice@example.org\"\n103 1.0 GlobalJobId \"ap1#1.0#1700000000\"\n106\n")
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	appendFile(t, logPath, spawned(1))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	got := samples(t, arch)
	if len(got) == 0 {
		t.Fatal("no sample")
	}
	for i, ad := range got {
		if u, _ := ad.EvaluateAttrString("User"); u != "alice@example.org" {
			t.Errorf("sample %d User = %q", i, u)
		}
		if g, _ := ad.EvaluateAttrString("GlobalJobId"); g != "ap1#1.0#1700000000" {
			t.Errorf("sample %d GlobalJobId = %q", i, g)
		}
		if c, p := num(t, ad, "ClusterId"), num(t, ad, "ProcId"); c != 1 || p != 0 {
			t.Errorf("sample %d job id = %v.%v, want 1.0", i, c, p)
		}
	}
}
