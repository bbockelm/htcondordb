package scheddsync

// Ingest benchmarks against a real condor_schedd job_queue.log. A real log is required --
// synthetic ads do not reproduce the shape that matters (wide job ads, long chained
// transactions, a realistic mix of Set/Delete/New) -- so the log is supplied out of band:
//
//	SCHEDDSYNC_BENCH_LOG=/path/to/job_queue.log go test ./scheddsync -run xxx -bench Ingest
//
// Without it every benchmark here skips, so a checkout with no log still runs clean.
//
// The two halves of a real log are entirely different workloads and must be measured
// apart (see splitSnapshot). BenchmarkBulkIngest is the cold full replay -- the restart
// and reconcile path. BenchmarkIncrementalTail is the steady state: the mirror is warmed
// from the compacted queue snapshot and the live transaction stream that follows it is
// then delivered in poll-sized appends, which is what the running daemon does all day and
// where its CPU actually goes.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

const benchLogEnv = "SCHEDDSYNC_BENCH_LOG"

// benchLog returns the contents of the benchmark log, skipping if none was supplied.
// SCHEDDSYNC_BENCH_BYTES caps how much of it is read (rounded down to a transaction
// boundary), so a run can be scaled down without a separate file.
func benchLog(tb testing.TB) []byte {
	tb.Helper()
	path := os.Getenv(benchLogEnv)
	if path == "" {
		tb.Skipf("set %s to a job_queue.log to run this benchmark", benchLogEnv)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatalf("reading %s: %v", path, err)
	}
	if capStr := os.Getenv("SCHEDDSYNC_BENCH_BYTES"); capStr != "" {
		var n int64
		if _, err := fmt.Sscan(capStr, &n); err != nil {
			tb.Fatalf("SCHEDDSYNC_BENCH_BYTES: %v", err)
		}
		if n > 0 && n < int64(len(data)) {
			data = data[:txnBoundary(data, n)]
		}
	}
	return data
}

// txnBoundary returns the offset of the first transaction boundary (the byte after an
// EndTransaction line) at or after want. Cutting a log anywhere else would leave a
// half-applied transaction, which the tailer handles but which makes two runs differ.
func txnBoundary(data []byte, want int64) int64 {
	for off := int(want); off < len(data); {
		nl := bytes.IndexByte(data[off:], '\n')
		if nl < 0 {
			return int64(len(data))
		}
		line := data[off : off+nl]
		off += nl + 1
		if bytes.Equal(bytes.TrimRight(line, " \t"), []byte("106")) {
			return int64(off)
		}
	}
	return int64(len(data))
}

// splitSnapshot divides a job_queue.log into its compacted queue snapshot and the live
// transaction stream that follows it. A schedd's compacted log is a flat run of
// NewClassAd/SetAttribute records with NO transaction framing -- every 105/106 pair sits
// AFTER it -- so the two halves are entirely different workloads: a cold bulk load of the
// whole queue, then the steady trickle a tailer actually lives on. Benchmarking the tail
// means warming from the first and feeding the second.
func splitSnapshot(data []byte) (snapshot, txns []byte) {
	for off := 0; off < len(data); {
		nl := bytes.IndexByte(data[off:], '\n')
		if nl < 0 {
			break
		}
		line := bytes.TrimRight(data[off:off+nl], " \t")
		if bytes.Equal(line, []byte("105")) {
			return data[:off], data[off:]
		}
		off += nl + 1
	}
	return data, nil
}

// benchTables opens the jobs table plus every sibling namespace table. persistent selects
// on-disk collections (what a deployed daemon runs) over in-memory ones.
func benchTables(tb testing.TB, dir string, persistent bool) (*db.DB, JobSyncConfig) {
	tb.Helper()
	open := func(name string) *db.DB {
		path := ""
		if persistent {
			path = filepath.Join(dir, name)
		}
		d, err := db.Open(path)
		if err != nil {
			tb.Fatalf("opening %s: %v", name, err)
		}
		tb.Cleanup(func() { d.Close() })
		return d
	}
	jobs := open("jobs")
	return jobs, JobSyncConfig{
		Users:          open("users"),
		Jobsets:        open("jobsets"),
		Clusters:       open("clusters"),
		Header:         open("header"),
		ClusterPrivate: open("clusterprivate"),
		LogMeta:        open("logmeta"),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func BenchmarkBulkIngest(b *testing.B) {
	data := benchLog(b)
	for _, persistent := range []bool{false, true} {
		name := "memory"
		if persistent {
			name = "persistent"
		}
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := b.TempDir()
				logPath := filepath.Join(dir, "job_queue.log")
				if err := os.WriteFile(logPath, data, 0o600); err != nil {
					b.Fatal(err)
				}
				jobs, cfg := benchTables(b, dir, persistent)
				cfg.Filename = logPath
				s := NewJobSync(jobs, cfg)
				b.StartTimer()

				start := time.Now()
				if err := s.Poll(context.Background()); err != nil {
					b.Fatalf("poll: %v", err)
				}
				elapsed := time.Since(start)

				b.StopTimer()
				b.ReportMetric(float64(len(data))/elapsed.Seconds()/(1<<20), "MB/s")
				b.ReportMetric(float64(jobs.Len()), "jobs")
				b.StartTimer()
			}
		})
	}
}

// BenchmarkIncrementalTail is the steady-state shape: a mirror already holding the queue,
// fed the log's remaining transactions in poll-sized appends. This -- not the cold replay
// -- is what a daemon attached to a busy schedd spends its life doing.
func BenchmarkIncrementalTail(b *testing.B) {
	data := benchLog(b)
	head, tail := splitSnapshot(data)
	if len(tail) == 0 {
		b.Skip("log has no transaction stream to tail")
	}
	chunkBytes := int64(256 << 10)
	if v := os.Getenv("SCHEDDSYNC_BENCH_CHUNK"); v != "" {
		if _, err := fmt.Sscan(v, &chunkBytes); err != nil {
			b.Fatalf("SCHEDDSYNC_BENCH_CHUNK: %v", err)
		}
	}

	for _, persistent := range []bool{false, true} {
		name := "memory"
		if persistent {
			name = "persistent"
		}
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := b.TempDir()
				logPath := filepath.Join(dir, "job_queue.log")
				if err := os.WriteFile(logPath, head, 0o600); err != nil {
					b.Fatal(err)
				}
				jobs, cfg := benchTables(b, dir, persistent)
				cfg.Filename = logPath
				s := NewJobSync(jobs, cfg)
				if err := s.Poll(context.Background()); err != nil {
					b.Fatalf("warm poll: %v", err)
				}
				f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				start := time.Now()
				polls := 0
				for off := int64(0); off < int64(len(tail)); {
					end := txnBoundary(tail, min(off+chunkBytes, int64(len(tail))))
					if _, err := f.Write(tail[off:end]); err != nil {
						b.Fatal(err)
					}
					if err := s.Poll(context.Background()); err != nil {
						b.Fatalf("tail poll: %v", err)
					}
					polls++
					off = end
				}
				elapsed := time.Since(start)

				b.StopTimer()
				f.Close()
				b.ReportMetric(float64(len(tail))/elapsed.Seconds()/(1<<20), "tailMB/s")
				b.ReportMetric(float64(polls), "polls")
				b.StartTimer()
			}
		})
	}
}

// TestProfileTail profiles ONLY the incremental tail. The benchmark above cannot: a Go
// -cpuprofile covers the whole process, so the cold warm-up replay (a different, much
// larger workload) drowns out the steady-state signal. This warms first, starts the
// profile, then tails.
//
//	SCHEDDSYNC_BENCH_LOG=... SCHEDDSYNC_TAIL_PROF=/tmp/tail.prof \
//	  go test ./scheddsync -run TestProfileTail -timeout 60m
func TestProfileTail(t *testing.T) {
	out := os.Getenv("SCHEDDSYNC_TAIL_PROF")
	if out == "" {
		t.Skip("set SCHEDDSYNC_TAIL_PROF to an output path to profile the tail")
	}
	data := benchLog(t)
	head, tail := splitSnapshot(data)
	if len(tail) == 0 {
		t.Skip("log has no transaction stream to tail")
	}
	// The tail alone can be tens of minutes of schedd activity; cap it (at a transaction
	// boundary) to keep a profiling run short while the warm-up -- and so the mirror state
	// the tail runs against -- stays identical between runs.
	if v := os.Getenv("SCHEDDSYNC_TAIL_BYTES"); v != "" {
		var n int64
		if _, err := fmt.Sscan(v, &n); err != nil {
			t.Fatalf("SCHEDDSYNC_TAIL_BYTES: %v", err)
		}
		if n > 0 && n < int64(len(tail)) {
			tail = tail[:txnBoundary(tail, n)]
		}
	}

	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	if err := os.WriteFile(logPath, head, 0o600); err != nil {
		t.Fatal(err)
	}
	jobs, cfg := benchTables(t, dir, os.Getenv("SCHEDDSYNC_BENCH_PERSISTENT") != "")
	cfg.Filename = logPath
	s := NewJobSync(jobs, cfg)
	warm := time.Now()
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("warm poll: %v", err)
	}
	t.Logf("warm: %d bytes -> %d jobs in %v", len(head), jobs.Len(), time.Since(warm))

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	pf, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	if err := pprof.StartCPUProfile(pf); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	polls := 0
	for off := int64(0); off < int64(len(tail)); {
		end := txnBoundary(tail, min(off+(256<<10), int64(len(tail))))
		if _, err := f.Write(tail[off:end]); err != nil {
			t.Fatal(err)
		}
		if err := s.Poll(context.Background()); err != nil {
			t.Fatalf("tail poll: %v", err)
		}
		polls++
		off = end
	}
	pprof.StopCPUProfile()
	el := time.Since(start)
	runtime.ReadMemStats(&after)
	t.Logf("tail: %d bytes in %d polls, %v (%.2f MB/s)",
		len(tail), polls, el, float64(len(tail))/el.Seconds()/(1<<20))
	// Allocation volume is the other half of the cost: the bytes the tail churns through
	// bound how hard it drives the GC, which a CPU profile attributes to the collector
	// rather than to the sync.
	t.Logf("tail alloc: %.1f GiB in %d objects (%d GCs), %.0f bytes allocated per log byte",
		float64(after.TotalAlloc-before.TotalAlloc)/(1<<30),
		after.Mallocs-before.Mallocs, after.NumGC-before.NumGC,
		float64(after.TotalAlloc-before.TotalAlloc)/float64(len(tail)))
}
