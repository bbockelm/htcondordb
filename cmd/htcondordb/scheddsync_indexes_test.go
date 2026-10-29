package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/htcondordb/server"
)

// TestIndexAdditions pins the reconciliation's rules: add what is missing under the wanted kind,
// never redefine an index that exists, and re-add an auto-created one under its current kind so
// it is recorded as configured.
func TestIndexAdditions(t *testing.T) {
	addCat, addVal := indexAdditions(
		[]string{"Owner", "AcctGroup"},                        // have categorical
		[]string{"ClusterId", "QDate"},                        // have value
		[]string{"QDate", "AcctGroup"},                        // auto-created
		[]string{"Owner", "JobStatus"},                        // want categorical (JobStatus wanted categorical, absent)
		[]string{"clusterid", "QDate", "ProcId", "AcctGroup"}, // want value
	)
	if want := []string{"JobStatus", "AcctGroup"}; !slices.Equal(addCat, want) {
		t.Errorf("addCat = %v, want %v (AcctGroup re-added as the categorical index it is)", addCat, want)
	}
	if want := []string{"QDate", "ProcId"}; !slices.Equal(addVal, want) {
		t.Errorf("addVal = %v, want %v", addVal, want)
	}
}

// TestReconcilePromotesAutoIndex: an index the auto-tuner created is exactly the case the
// built-in set exists for, and after reconciliation it must be recorded as configured -- the
// trimmer drops only auto indexes -- while an auto index the built-in set does not name is left
// to the tuner. Provenance lives in archiveconfig.json, so that is where it is checked.
func TestReconcilePromotesAutoIndex(t *testing.T) {
	dir := t.TempDir()
	cat, err := db.OpenCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cat.CreateArchiveTable("history", db.ArchiveConfig{
		CategoricalAttrs: []string{"Owner"}, ValueAttrs: []string{"ClusterId", "CompletionDate", "RequestMemory"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := cat.Close(); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "archives", "history", "archiveconfig.json")
	setAuto(t, cfgPath, []string{"CompletionDate", "RequestMemory"})

	cat, err = db.OpenCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cat.Close() }()
	hist, ok := cat.ArchiveTable("history")
	if !ok {
		t.Fatal("archive not recovered")
	}
	if got := hist.AutoIndexNames(); !slices.Contains(got, "CompletionDate") {
		t.Fatalf("setup: auto indexes = %v, want CompletionDate among them", got)
	}
	for i := 0; i < 20; i++ {
		if err := hist.AppendOld(fmt.Sprintf("ClusterId = %d\nProcId = 0\nOwner = \"alice\"\nCompletionDate = %d", i, 1000+i)); err != nil {
			t.Fatal(err)
		}
	}

	m := &scheddSyncManager{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
	histCat, histVal := s.historyIndexSet()
	var wg sync.WaitGroup
	m.reconcileArchiveIndexes(context.Background(), hist, "history", histCat, histVal, &wg)
	wg.Wait()

	if got := hist.AutoIndexNames(); !slices.Equal(got, []string{"RequestMemory"}) {
		t.Errorf("auto indexes after reconcile = %v, want [RequestMemory]: CompletionDate is built in "+
			"and must be recorded as configured, RequestMemory is the tuner's and stays its", got)
	}
	if got := readAuto(t, cfgPath); slices.Contains(got, "CompletionDate") {
		t.Errorf("archiveconfig.json still records CompletionDate as auto (%v); a restart would restore that", got)
	}
	_, val := hist.IndexedAttrs()
	for _, a := range histVal {
		if !slices.Contains(val, a) {
			t.Errorf("value index %s missing after reconcile; value = %v", a, val)
		}
	}
	if !slices.Contains(val, "RequestMemory") {
		t.Errorf("the tuner's RequestMemory index was dropped; reconciliation is add-only")
	}
}

func setAuto(t *testing.T, path string, auto []string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	cfg["AutoAttrs"] = auto
	out, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out, 0o644); err != nil {
		t.Fatal(err)
	}
}

func readAuto(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct{ AutoAttrs []string }
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg.AutoAttrs
}

// TestReconcileReportsIndexCoverageGap: an index added to an existing archive reaches only the
// backfill horizon, and the archive lists it as indexed regardless. The reconciliation has to say
// so -- that gap, invisible anywhere but `.explain`, is what left most of a production history
// scanned in full for indexes everyone believed it had.
func TestReconcileReportsIndexCoverageGap(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cat.Close() }()
	// A one-byte horizon stands in for the default 1 GiB against an archive far larger than it.
	hist, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		SegmentSize: 1 << 12, CategoricalAttrs: []string{"Owner"}, ValueAttrs: []string{"ClusterId"},
		IndexBackfillBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i++ {
		if err := hist.AppendOld(fmt.Sprintf("ClusterId = %d\nProcId = %d\nOwner = \"u%d\"\nJobStatus = 4\nQDate = %d\nCompletionDate = %d",
			i, i%7, i%5, 1000+i, 2000+i)); err != nil {
			t.Fatal(err)
		}
	}
	hist.Reindex()

	var buf bytes.Buffer
	m := &scheddSyncManager{logger: slog.New(slog.NewTextHandler(&buf, nil))}
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
	histCat, histVal := s.historyIndexSet()
	var wg sync.WaitGroup
	m.reconcileArchiveIndexes(context.Background(), hist, "history", histCat, histVal, &wg)
	wg.Wait()

	out := buf.String()
	if !strings.Contains(out, "archive indexes do not cover every segment") {
		t.Fatalf("no coverage warning after adding indexes past the horizon:\n%s", out)
	}
	for _, a := range []string{"ProcId=", "QDate=", "CompletionDate="} {
		if !strings.Contains(out, a) {
			t.Errorf("coverage warning does not name %s:\n%s", strings.TrimSuffix(a, "="), out)
		}
	}
	// The indexes the archive was created with cover everything and must not be named.
	if strings.Contains(out, "ClusterId=") || strings.Contains(out, "Owner=") {
		t.Errorf("coverage warning names an index that covers every segment:\n%s", out)
	}
}

// TestReconcileQuietWhenCovered: an archive created with the built-in set is fully covered, and
// the coverage report must say nothing -- a warning on every healthy start would be ignored.
func TestReconcileQuietWhenCovered(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cat.Close() }()
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
	histCat, histVal := s.historyIndexSet()
	hist, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		SegmentSize: 1 << 12, CategoricalAttrs: histCat, ValueAttrs: histVal, IndexBackfillBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i++ {
		if err := hist.AppendOld(fmt.Sprintf("ClusterId = %d\nProcId = %d\nOwner = \"u%d\"\nJobStatus = 4\nQDate = %d\nCompletionDate = %d",
			i, i%7, i%5, 1000+i, 2000+i)); err != nil {
			t.Fatal(err)
		}
	}
	hist.Reindex()
	var buf bytes.Buffer
	m := &scheddSyncManager{logger: slog.New(slog.NewTextHandler(&buf, nil))}
	var wg sync.WaitGroup
	m.reconcileArchiveIndexes(context.Background(), hist, "history", histCat, histVal, &wg)
	wg.Wait()
	if strings.Contains(buf.String(), "do not cover") {
		t.Errorf("coverage warning on an archive created with its indexes:\n%s", buf.String())
	}
}

// TestScheddSyncCreatesTablesWithBuiltInIndexes drives the launch path: every table schedd-sync
// creates carries its built-in index set from the start, the history archive requires
// CompletionDate in its columnar schema while the other tables do not, and a configured addition
// lands beside the built-in set rather than replacing it. Through the manager rather than the
// helpers, because a set that is defined but never passed to the table is the failure to catch.
func TestScheddSyncCreatesTablesWithBuiltInIndexes(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	dir := t.TempDir()
	svc, err := server.New(server.Config{Dir: dir, Authorize: func(_, _, _ string) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = svc.Close() }()

	logDir := t.TempDir()
	jobLog := filepath.Join(logDir, "job_queue.log")
	if err := os.WriteFile(jobLog, []byte("101 1.0 Job Machine\n103 1.0 ProcId 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	histFile := filepath.Join(logDir, "history")
	epochFile := filepath.Join(logDir, "epoch_history")
	for _, f := range []string{histFile, epochFile} {
		if err := os.WriteFile(f, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &scheddSyncManager{parent: ctx, svc: svc, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHTCONDORDB_DIR = "+dir+
		"\nHTCONDORDB_JOB_QUEUE_LOG = "+jobLog+"\nHTCONDORDB_HISTORY = "+histFile+
		"\nHTCONDORDB_JOB_EPOCH_HISTORY = "+epochFile+"\nHTCONDORDB_JOB_METRICS = true\n"+
		"HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS = AccountingGroup\n")); err != nil {
		t.Fatal(err)
	}
	m.Stop() // joins the index builds

	type indexed interface{ IndexedAttrs() ([]string, []string) }
	check := func(table string, tab indexed, set tableIndexSet, extraCat ...string) {
		t.Helper()
		gotCat, gotVal := tab.IndexedAttrs()
		for _, a := range append(slices.Clone(set.categorical), extraCat...) {
			if !slices.ContainsFunc(gotCat, func(b string) bool { return strings.EqualFold(a, b) }) {
				t.Errorf("%s: categorical index %s missing; categorical = %v", table, a, gotCat)
			}
		}
		for _, a := range set.value {
			if !slices.ContainsFunc(gotVal, func(b string) bool { return strings.EqualFold(a, b) }) {
				t.Errorf("%s: value index %s missing; value = %v", table, a, gotVal)
			}
		}
	}
	c := svc.Catalog()
	for name, set := range map[string]tableIndexSet{
		"history": historyIndexes, "epoch_history": epochHistoryIndexes, "job_metrics": jobMetricsIndexes,
	} {
		arch, ok := c.ArchiveTable(name)
		if !ok {
			t.Fatalf("%s archive was not created", name)
		}
		var extra []string
		if name != "job_metrics" {
			extra = []string{"AccountingGroup"}
		}
		check(name, arch, set, extra...)
	}
	jobs, ok := c.Table("jobs")
	if !ok {
		t.Fatal("jobs table was not created")
	}
	check("jobs", jobs, jobsIndexes)

	hist, _ := c.ArchiveTable("history")
	if got := hist.SchemaRequireAttrs(); !slices.Contains(got, "completiondate") {
		t.Errorf("history required schema attributes = %v, want completiondate among them", got)
	}
	ep, _ := c.ArchiveTable("epoch_history")
	if got := ep.SchemaRequireAttrs(); slices.Contains(got, "completiondate") {
		t.Errorf("epoch_history requires completiondate (%v); epoch records do not carry it", got)
	}
	if got := ep.SchemaRequireAttrs(); !slices.Contains(got, "vacatereasoncode") {
		t.Errorf("epoch_history required schema attributes = %v, want vacatereasoncode among them", got)
	}
	if got := hist.SchemaRequireAttrs(); slices.Contains(got, "vacatereasoncode") {
		t.Errorf("history requires vacatereasoncode (%v); only the epoch archive is filtered on it", got)
	}
}

// TestHistorySchemaRequireDefaults: history requires CompletionDate on top of the shared list when
// the knob is unset, and a configured list replaces both (the knob's documented meaning; a site
// must be able to say exactly which per-record slots it pays for).
func TestHistorySchemaRequireDefaults(t *testing.T) {
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
	hist := splitAttrList(s.historySchemaRequireAttrs)
	if want := append(splitAttrList(s.schemaRequireAttrs), "CompletionDate"); !slices.Equal(hist, want) {
		t.Errorf("history required = %v, want %v", hist, want)
	}
	if slices.Contains(splitAttrList(s.schemaRequireAttrs), "CompletionDate") {
		t.Error("CompletionDate is in the shared required list; jobs and epoch_history would pay a slot for it")
	}
	s, _ = resolveScheddSyncSettings(mkSyncCfg(t, syncOn+"HTCONDORDB_SCHEMA_REQUIRE_ATTRS = ResidentSetSize\n"))
	if got := splitAttrList(s.historySchemaRequireAttrs); !slices.Equal(got, []string{"ResidentSetSize"}) {
		t.Errorf("configured history required = %v, want [ResidentSetSize]", got)
	}
	s, _ = resolveScheddSyncSettings(mkSyncCfg(t, syncOn+"HTCONDORDB_SCHEMA_REQUIRE_ATTRS = none\n"))
	if s.historySchemaRequireAttrs != "" {
		t.Errorf("`none` left history requiring %q", s.historySchemaRequireAttrs)
	}
}

// TestEpochSchemaRequireDefaults: epoch_history requires VacateReasonCode on top of the shared
// list when the knob is unset, and a configured list replaces both.
func TestEpochSchemaRequireDefaults(t *testing.T) {
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
	ep := splitAttrList(s.epochSchemaRequireAttrs)
	if want := append(splitAttrList(s.schemaRequireAttrs), "VacateReasonCode"); !slices.Equal(ep, want) {
		t.Errorf("epoch_history required = %v, want %v", ep, want)
	}
	if slices.Contains(splitAttrList(s.schemaRequireAttrs), "VacateReasonCode") {
		t.Error("VacateReasonCode is in the shared required list; jobs would pay a slot for it")
	}
	s, _ = resolveScheddSyncSettings(mkSyncCfg(t, syncOn+"HTCONDORDB_SCHEMA_REQUIRE_ATTRS = ResidentSetSize\n"))
	if got := splitAttrList(s.epochSchemaRequireAttrs); !slices.Equal(got, []string{"ResidentSetSize"}) {
		t.Errorf("configured epoch_history required = %v, want [ResidentSetSize]", got)
	}
	s, _ = resolveScheddSyncSettings(mkSyncCfg(t, syncOn+"HTCONDORDB_SCHEMA_REQUIRE_ATTRS = none\n"))
	if s.epochSchemaRequireAttrs != "" {
		t.Errorf("`none` left epoch_history requiring %q", s.epochSchemaRequireAttrs)
	}
}
