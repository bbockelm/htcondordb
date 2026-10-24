package scheddsync

// History ingest benchmark: a fresh HistorySync drains a history file of completed jobs into
// an on-disk archive, the first-run / backlog path.
//
//	go test ./scheddsync -run '^$' -bench HistoryIngest
//	SCHEDDSYNC_HISTORY_TEMPLATE=/path/to/history SCHEDDSYNC_HISTORY_RECORDS=50000 \
//	    go test ./scheddsync -run '^$' -bench HistoryIngest
//
// SCHEDDSYNC_HISTORY_TEMPLATE names a history (or epoch history) file whose records are
// repeated, with fresh ClusterId/ProcId/GlobalJobId, until SCHEDDSYNC_HISTORY_RECORDS records
// are written (default 10000). Without a template a built-in history-shaped record is used.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

const builtinHistoryRecord = `Owner = "alice"
User = "alice@ap.example.org"
AccountingGroup = "group_opportunistic.proj.alice"
JobStatus = 4
ExitCode = 0
ExitBySignal = false
CompletionDate = 1766943808
EnteredCurrentStatus = 1766943808
QDate = 1766132442
JobStartDate = 1766941298
RemoteWallClockTime = 2510.0
RemoteUserCpu = 9264.0
RemoteSysCpu = 44.0
CpusUsage = 3.974506893067818E+00
RequestCpus = 4
RequestMemory = 4096
RequestDisk = 20971520
MemoryUsage = ((ResidentSetSize + 1023) / 1024)
ResidentSetSize = 2000000
ImageSize = 10000000
DiskUsage = 7500000
BytesSent = 330832
BytesRecvd = 7884
Cmd = "/home/alice/project/sanitized/70920985b42e92fa"
Args = "/home/alice/project/in/b81a8db0303f7150"
Err = "logs/err"
Out = "logs/out"
LastRemoteHost = "slot1_3@glidein_1234_5678@node17.example.edu"
Requirements = (TARGET.Arch == "X86_64") && (TARGET.OpSys == "LINUX") && (TARGET.Disk >= RequestDisk) && (TARGET.Memory >= RequestMemory)
`

// historyTemplateRecords returns the attribute text of each template record, with the
// ClusterId/ProcId/GlobalJobId lines removed (the generator writes its own).
func historyTemplateRecords(tb testing.TB) [][]byte {
	tb.Helper()
	data := []byte(builtinHistoryRecord + "*** \n")
	if path := os.Getenv("SCHEDDSYNC_HISTORY_TEMPLATE"); path != "" {
		var err error
		if data, err = os.ReadFile(path); err != nil { //nolint:gosec // benchmark input named by the operator
			tb.Fatal(err)
		}
	}
	var recs [][]byte
	var rec []byte
	for _, line := range bytes.SplitAfter(data, []byte("\n")) {
		switch {
		case bytes.HasPrefix(line, []byte("***")):
			if len(rec) > 0 {
				recs = append(recs, rec)
			}
			rec = nil
		case bytes.HasPrefix(line, []byte("ClusterId ")), bytes.HasPrefix(line, []byte("ProcId ")),
			bytes.HasPrefix(line, []byte("GlobalJobId ")):
		default:
			rec = append(rec, line...)
		}
	}
	if len(recs) == 0 {
		tb.Fatal("history template has no records")
	}
	return recs
}

// writeBenchHistory writes n completed-job records built from the template records.
func writeBenchHistory(tb testing.TB, path string, n int) int64 {
	tb.Helper()
	recs := historyTemplateRecords(tb)
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		cluster, proc := 1000+i/100, i%100
		fmt.Fprintf(&buf, "ClusterId = %d\nProcId = %d\nGlobalJobId = \"ap.example.org#%d.%d#1766132442\"\n", cluster, proc, cluster, proc)
		buf.Write(recs[i%len(recs)])
		fmt.Fprintf(&buf, "*** Offset = %d ClusterId = %d ProcId = %d Owner = \"alice\" CompletionDate = 1766943808\n", buf.Len(), cluster, proc)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		tb.Fatal(err)
	}
	return int64(buf.Len())
}

// BenchmarkHistoryIngest measures one full drain of a history file into a fresh archive per
// iteration and reports records/s.
func BenchmarkHistoryIngest(b *testing.B) {
	n := 10000
	if s := os.Getenv("SCHEDDSYNC_HISTORY_RECORDS"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v <= 0 {
			b.Fatalf("SCHEDDSYNC_HISTORY_RECORDS: %q", s)
		}
		n = v
	}
	dir := b.TempDir()
	path := filepath.Join(dir, "history")
	size := writeBenchHistory(b, path, n)
	b.SetBytes(size)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cat, err := db.OpenCatalog(filepath.Join(dir, "cat"+strconv.Itoa(i)))
		if err != nil {
			b.Fatal(err)
		}
		arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
			ValueAttrs: []string{"ClusterId"},
			ZoneAttrs:  []string{"CompletionDate", EnteredHistoryAttr},
		})
		if err != nil {
			b.Fatal(err)
		}
		s := NewHistorySync(arch, HistorySyncConfig{
			Filename: path, Logger: quietLogger(),
			Store: &FileStore{Path: filepath.Join(dir, "pos"+strconv.Itoa(i), "history.pos")},
		})
		b.StartTimer()
		if err := s.Poll(ctx); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if got := arch.Count(); got != n {
			b.Fatalf("archived %d records, want %d", got, n)
		}
		s.close()
		_ = cat.Close()
		_ = os.RemoveAll(filepath.Join(dir, "cat"+strconv.Itoa(i)))
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "records/s")
}
