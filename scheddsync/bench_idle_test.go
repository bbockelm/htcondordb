package scheddsync

// Idle-poll benchmarks. A deployed daemon runs three tailers (jobs, history, epoch), each
// on its own ticker, and a schedd that is quiet still gets polled at the full rate -- so
// what one no-op poll costs is what the mirror charges an idle pool, forever. These use a
// synthetic log: the idle path never looks at content beyond the header, so a real log
// adds nothing, and keeping them self-contained means they run in CI.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// idleLog writes a small but structurally complete job_queue.log: a 107 sequence header
// (which the rotation check re-reads on every poll) followed by one committed transaction.
func idleLogT(tb testing.TB, dir string) string {
	tb.Helper()
	path := filepath.Join(dir, "job_queue.log")
	body := "107 42 CreationTimestamp 1654634544\n"
	for i := 0; i < 200; i++ {
		body += fmt.Sprintf("105\n101 %d.0 Job Machine\n103 %d.0 Owner \"alice\"\n103 %d.0 JobStatus 1\n106\n", i, i, i)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		tb.Fatal(err)
	}
	return path
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// idleTables opens the jobs table plus every sibling namespace the router needs.
func idleTablesT(tb testing.TB) (*db.DB, JobSyncConfig) {
	tb.Helper()
	open := func() *db.DB {
		d, err := db.Open("")
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() { _ = d.Close() })
		return d
	}
	return open(), JobSyncConfig{
		Users: open(), Jobsets: open(), Clusters: open(),
		Header: open(), ClusterPrivate: open(), LogMeta: open(),
		Logger: quietLogger(),
	}
}

// BenchmarkIdlePollJob measures one JobSync.Poll against a log that has not changed --
// the ProbeNoChange path, which is every poll on a quiet schedd.
func BenchmarkIdlePollJob(b *testing.B) {
	dir := b.TempDir()
	path := idleLogT(b, dir)
	jobs, cfg := idleTablesT(b)
	cfg.Filename = path
	s := NewJobSync(jobs, cfg)
	ctx := context.Background()
	if err := s.Poll(ctx); err != nil { // consume the log; every later poll is a no-op
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Poll(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIdlePollHistory is the same measurement for the history tailer, which holds its
// file open across polls instead of reopening it.
func BenchmarkIdlePollHistory(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "history")
	if err := os.WriteFile(path, []byte("Owner = \"alice\"\nJobStatus = 4\nClusterId = 1\nProcId = 0\n*** ClusterId=1 ProcId=0 Owner=\"alice\" CompletionDate=1654634544\n"), 0o600); err != nil {
		b.Fatal(err)
	}
	cat, err := db.OpenCatalog(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = cat.Close() })
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		b.Fatal(err)
	}
	s := NewHistorySync(arch, HistorySyncConfig{Filename: path, Logger: quietLogger()})
	ctx := context.Background()
	if err := s.Poll(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Poll(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// TestIdleCPU measures what a deployed daemon's tailers cost while the schedd is quiet, by
// running all three the way the manager does -- each on its own ticker -- and reading the
// process's own CPU time across the interval. A per-poll benchmark cannot answer this: it
// says what one poll costs but not how many run, and the ticker rate is the other half.
//
// Reports CPU as a fraction of one core, which is the number an operator compares against
// "the mirror should be idle". Skipped by default (it spends wall-clock sitting still); set
// SCHEDDSYNC_IDLE_SECONDS to run it.
func TestIdleCPU(t *testing.T) {
	secs := 0
	if v := os.Getenv("SCHEDDSYNC_IDLE_SECONDS"); v != "" {
		if _, err := fmt.Sscan(v, &secs); err != nil {
			t.Fatalf("SCHEDDSYNC_IDLE_SECONDS: %v", err)
		}
	}
	if secs <= 0 {
		t.Skip("set SCHEDDSYNC_IDLE_SECONDS to run the idle CPU measurement")
	}

	dir := t.TempDir()
	jobPath := idleLogT(t, dir)
	histPath := filepath.Join(dir, "history")
	epochPath := filepath.Join(dir, "epoch")
	for _, p := range []string{histPath, epochPath} {
		if err := os.WriteFile(p, []byte("Owner = \"alice\"\nClusterId = 1\nProcId = 0\nCompletionDate = 1654634544\n*** ClusterId=1 ProcId=0\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cat, err := db.OpenCatalog(filepath.Join(dir, "cat"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cat.Close() }()
	mkArchive := func(name string) *db.ArchiveTable {
		a, aerr := cat.CreateArchiveTable(name, db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
		if aerr != nil {
			t.Fatal(aerr)
		}
		return a
	}
	jobs, cfg := idleTablesT(t)
	cfg.Filename = jobPath

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	start := func(run func(context.Context) error) {
		wg.Add(1)
		go func() { defer wg.Done(); _ = run(ctx) }()
	}
	js := NewJobSync(jobs, cfg)
	hs := NewHistorySync(mkArchive("history"), HistorySyncConfig{Filename: histPath, Logger: quietLogger()})
	es := NewJobEpochSync(mkArchive("epoch_history"), HistorySyncConfig{Filename: epochPath, Logger: quietLogger()})

	// SCHEDDSYNC_IDLE_TAILERS=0 measures the same window with NO tailer running: the Go
	// runtime's own floor in this process. Without that control a small tailer cost is
	// indistinguishable from a large one sitting on top of a large floor.
	if os.Getenv("SCHEDDSYNC_IDLE_TAILERS") != "0" {
		// Let the first polls consume the files, so the measured window is pure idle.
		start(js.Run)
		start(hs.Run)
		start(es.Run)
	}
	time.Sleep(time.Second)

	// Each source reports cumulative time inside Poll, so the window's CPU can be attributed
	// to polling rather than guessed at: if these account for the process's CPU, the poll
	// rate is the lever; if they do not, something outside the poll loop is.
	pollBefore := js.Status().PollSeconds
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	before := cpuTime(t)
	time.Sleep(time.Duration(secs) * time.Second)
	used := cpuTime(t) - before
	runtime.ReadMemStats(&m1)
	cancel()
	wg.Wait()

	frac := used.Seconds() / float64(secs)
	t.Logf("idle: 3 tailers at %v, %d s window -> %v CPU = %.3f%% of one core (%.1f core-seconds/day)",
		DefaultPollInterval, secs, used.Round(time.Millisecond), frac*100, frac*86400)
	t.Logf("idle jobs tailer: %.1f ms of the window spent inside Poll",
		(js.Status().PollSeconds-pollBefore)*1000)
	rate := float64(m1.TotalAlloc-m0.TotalAlloc) / float64(secs)
	t.Logf("idle alloc: %.0f KiB/s in %d objects (%d GCs) -> %.1f GiB/day of garbage",
		rate/1024, m1.Mallocs-m0.Mallocs, m1.NumGC-m0.NumGC, rate*86400/(1<<30))
}

// cpuTime returns the process's total CPU time (user + system) so far.
func cpuTime(t *testing.T) time.Duration {
	t.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Fatalf("getrusage: %v", err)
	}
	tv := func(v syscall.Timeval) time.Duration {
		return time.Duration(v.Sec)*time.Second + time.Duration(v.Usec)*time.Microsecond
	}
	return tv(ru.Utime) + tv(ru.Stime)
}
