package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// A job ad the size the schedd really stores: ~45 attributes, the ones condor_q
// and the match-making actually carry.
func benchAd(cluster, proc, status, universe int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ClusterId = %d\nProcId = %d\nJobStatus = %d\nJobUniverse = %d\n",
		cluster, proc, status, universe)
	fmt.Fprintf(&b, "Owner = \"user%03d\"\nUser = \"user%03d@example.edu\"\n", cluster%200, cluster%200)
	fmt.Fprintf(&b, "QDate = %d\nEnteredCurrentStatus = %d\nJobStartDate = %d\n",
		1700000000+cluster, 1700000100+cluster, 1700000200+cluster)
	fmt.Fprintf(&b, "RemoteWallClockTime = %d.0\nRemoteUserCpu = %d.0\nRemoteSysCpu = %d.0\n",
		cluster%10000, cluster%9000, cluster%100)
	fmt.Fprintf(&b, "ImageSize = %d\nResidentSetSize = %d\nDiskUsage = %d\nMemoryUsage = %d\n",
		100000+cluster%50000, 90000+cluster%40000, 200000+cluster%80000, 90+cluster%400)
	fmt.Fprintf(&b, "RequestCpus = %d\nRequestMemory = %d\nRequestDisk = %d\n",
		1+cluster%8, 1024*(1+cluster%8), 100000)
	fmt.Fprintf(&b, "Cmd = \"/home/user%03d/run_%d.sh\"\nIwd = \"/home/user%03d/work/%d\"\n",
		cluster%200, cluster, cluster%200, cluster)
	fmt.Fprintf(&b, "Args = \"--input data_%d.root --seed %d --verbose\"\n", cluster, proc)
	fmt.Fprintf(&b, "Out = \"job_%d_%d.out\"\nErr = \"job_%d_%d.err\"\nUserLog = \"/home/u/log_%d\"\n",
		cluster, proc, cluster, proc, cluster)
	b.WriteString("Requirements = (TARGET.Arch == \"X86_64\") && (TARGET.OpSys == \"LINUX\") && (TARGET.Disk >= RequestDisk) && (TARGET.Memory >= RequestMemory)\n")
	b.WriteString("Rank = 0.0\nNiceUser = false\nWantCheckpoint = false\nWantRemoteIO = true\n")
	b.WriteString("ShouldTransferFiles = \"YES\"\nWhenToTransferOutput = \"ON_EXIT\"\n")
	fmt.Fprintf(&b, "TransferInput = \"data_%d.root,config.json,setup.sh\"\n", cluster)
	fmt.Fprintf(&b, "JobPrio = %d\nJobLeaseDuration = 2400\nMaxHosts = 1\nMinHosts = 1\n", cluster%20-10)
	fmt.Fprintf(&b, "NumJobStarts = %d\nNumShadowStarts = %d\nNumRestarts = 0\nJobRunCount = %d\n",
		cluster%5, cluster%5, cluster%5)
	fmt.Fprintf(&b, "LastMatchTime = %d\nLastJobLeaseRenewal = %d\n", 1700000300+cluster, 1700000400+cluster)
	fmt.Fprintf(&b, "RemoteHost = \"slot%d@node%04d.example.edu\"\n", 1+cluster%32, cluster%2000)
	fmt.Fprintf(&b, "GlobalJobId = \"ap.example.edu#%d.%d#%d\"\n", cluster, proc, 1700000000+cluster)
	b.WriteString("CondorVersion = \"$CondorVersion: 26.2.0 Oct 25 2026 BuildID: 1 $\"\n")
	b.WriteString("AccountingGroup = \"group_physics.user\"\nConcurrencyLimits = \"\"\n")
	fmt.Fprintf(&b, "ExitStatus = 0\nExitCode = %d\nExitBySignal = false\nCumulativeSlotTime = %d.0\n",
		cluster%3, cluster%10000)
	return b.String()
}

func benchDB(tb testing.TB, n int) (*dbrpc.Client, func()) {
	tb.Helper()
	dir, err := os.MkdirTemp("", "qbench")
	if err != nil {
		tb.Fatal(err)
	}
	d, err := db.Open(dir)
	if err != nil {
		tb.Fatal(err)
	}
	start := time.Now()
	for i := 0; i < n; i++ {
		// A realistic spread: mostly idle/running, a few held/completed; universe
		// almost all vanilla with some docker and a little grid.
		status := []int{1, 1, 1, 2, 2, 2, 2, 5, 4, 3}[i%10]
		universe := []int{5, 5, 5, 5, 5, 5, 5, 12, 12, 9}[i%10]
		key := fmt.Sprintf("job_%d.0", i)
		if err := d.UpdateOld(key, benchAd(i, 0, status, universe)); err != nil {
			tb.Fatal(err)
		}
	}
	tb.Logf("loaded %d ads in %v", n, time.Since(start))
	// What the daemon's maintenance pass does: build the per-segment columnar
	// accelerator. Without it neither GROUP BY shape has a fast path, and the
	// measurement says nothing about a real mirror.
	s2 := time.Now()
	if !d.EnableSchemaScan(4000, 8) {
		tb.Logf("WARNING: columnar schema scan did not enable")
	} else {
		tb.Logf("built columnar schema in %v", time.Since(s2))
	}

	srv := dbrpc.NewServer(d)
	a, b := net.Pipe()
	go func() { _ = srv.ServeConn(dbrpc.NewStreamConn(a)) }()
	cl := dbrpc.NewClient(dbrpc.NewStreamConn(b))
	return cl, func() { _ = cl.Close(); _ = d.Close(); _ = os.RemoveAll(dir) }
}

// TestGroupByShape measures what a two-column GROUP BY actually costs on this
// classad, against the one-column form and against counting the rows as they
// stream (what the schedd path does).
func TestGroupByShape(t *testing.T) {
	// A measurement, not an assertion: it loads 100k ads and times three query
	// shapes, which is minutes under -race. CI runs the suite without -short, so
	// it is gated on being asked for by name rather than on -short.
	if os.Getenv("HTCONDORDB_QUEUE_BENCH") == "" {
		t.Skip("set HTCONDORDB_QUEUE_BENCH=1 to run the queue measurement")
	}
	n := 100000
	cl, cleanup := benchDB(t, n)
	defer cleanup()
	ctx := context.Background()
	countStar := []dbrpc.AggSpec{{Func: dbrpc.AggCount, Arg: "*"}}

	// Warm the caches so the first query does not pay for everyone.
	_, _ = cl.AggregateTable(ctx, dbrpc.DefaultTable, "true", []string{"JobStatus"}, countStar)

	for _, tc := range []struct {
		name  string
		group []string
	}{
		{"one column (JobStatus)", []string{"JobStatus"}},
		{"one column (JobUniverse)", []string{"JobUniverse"}},
		{"two columns (JobUniverse, JobStatus)", []string{"JobUniverse", "JobStatus"}},
	} {
		best := time.Hour
		var rows []dbrpc.AggRow
		for i := 0; i < 3; i++ {
			s := time.Now()
			var err error
			rows, err = cl.AggregateTable(ctx, dbrpc.DefaultTable, "true", tc.group, countStar)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if d := time.Since(s); d < best {
				best = d
			}
		}
		t.Logf("%-38s %8.1f ms  (%d groups)", tc.name, float64(best.Microseconds())/1000, len(rows))
	}

	// The shipping path: stream every ad and tally as the rows go by.
	for _, summary := range []bool{true, false} {
		b := &adRecordBuilder{emit: !summary}
		attrs, suppress := withCountingAttrs(nil)
		b.suppress = suppress
		_ = attrs
		best := time.Hour
		var gotAds int
		for i := 0; i < 3; i++ {
			s := time.Now()
			r := &queueRequest{client: &queueClient{notifyFD: -1}, chunks: make(chan queueChunk, 4)}
			r.wg.Add(1)
			go r.stream(ctx, cl, dbrpc.DefaultTable, "true", nil, 0, b, true)
			go func() { r.wg.Wait(); close(r.chunks); r.closed.Store(true) }()
			gotAds = 0
			for ch := range r.chunks {
				gotAds += ch.ads
			}
			if d := time.Since(s); d < best {
				best = d
			}
		}
		label := "stream + tally (summary only, no ads)"
		if !summary {
			label = "stream + tally (full ads to wire)"
		}
		t.Logf("%-38s %8.1f ms  (%d ads)", label, float64(best.Microseconds())/1000, gotAds)
	}
}
