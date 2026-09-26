package main

import (
	"slices"
	"testing"
	"time"

	"github.com/bbockelm/htcondordb/scheddsync"
)

// TestJobMetricsDefaults pins what an admin gets by doing nothing: sampling OFF (it adds a record
// per running job per shadow update, which is a volume decision to make deliberately) and the
// library segment size, which the measurement picked -- see TestJobMetricsSegmentSizeAB.
func TestJobMetricsDefaults(t *testing.T) {
	s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
	if s.metricsEnabled {
		t.Error("job metrics should be off unless asked for")
	}
	if s.metricsSegSize != 0 {
		t.Errorf("metricsSegSize = %d, want 0 (library default)", s.metricsSegSize)
	}
	if s.metricsCatAttrs != "Owner" {
		t.Errorf("metricsCatAttrs = %q, want Owner", s.metricsCatAttrs)
	}
	if s.metricsAttrs != "" {
		t.Errorf("metricsAttrs = %q, want empty", s.metricsAttrs)
	}
	if s.metricsMinInterval != 0 {
		t.Errorf("metricsMinInterval = %v, want 0 (no throttle)", s.metricsMinInterval)
	}
}

// TestJobMetricsOverrides checks every knob is actually read, including the two that inherit from
// the shared archive default.
func TestJobMetricsOverrides(t *testing.T) {
	s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
		"HTCONDORDB_ARCHIVE_MAX_BYTES = 10 GB\n"+
		"HTCONDORDB_JOB_METRICS = true\n"+
		"HTCONDORDB_JOB_METRICS_ATTRS = ProjectName, AccountingGroup\n"+
		"HTCONDORDB_JOB_METRICS_CATEGORICAL_ATTRS = Owner ProjectName\n"+
		"HTCONDORDB_JOB_METRICS_MIN_INTERVAL = 120\n"+
		"HTCONDORDB_JOB_METRICS_SEGMENT_SIZE = 4194304\n"+
		"HTCONDORDB_JOB_METRICS_MAX_AGE = 2592000\n"))
	if !s.metricsEnabled {
		t.Fatal("metricsEnabled = false")
	}
	if got := splitAttrList(s.metricsAttrs); !slices.Equal(got, []string{"ProjectName", "AccountingGroup"}) {
		t.Errorf("metricsAttrs = %v", got)
	}
	if got := splitAttrList(s.metricsCatAttrs); !slices.Equal(got, []string{"Owner", "ProjectName"}) {
		t.Errorf("metricsCatAttrs = %v", got)
	}
	if s.metricsMinInterval != 120*time.Second {
		t.Errorf("metricsMinInterval = %v, want 2m", s.metricsMinInterval)
	}
	if s.metricsSegSize != 4194304 {
		t.Errorf("metricsSegSize = %d", s.metricsSegSize)
	}
	if s.metricsMaxAge != 2592000 {
		t.Errorf("metricsMaxAge = %v, want 2592000", s.metricsMaxAge)
	}
	// Unset per-table size cap inherits the shared archive default, like history and epoch do.
	if s.metricsMaxBytes != 10*1000*1000*1000 {
		t.Errorf("metricsMaxBytes = %d, want the inherited 10 GB", s.metricsMaxBytes)
	}
}

// TestJobMetricsMaxBytesOverridesDefault: an explicit 0 must uncap this table while the shared
// default still caps the others -- the same "explicit wins, even when it is zero" rule the
// history and epoch caps follow.
func TestJobMetricsMaxBytesOverridesDefault(t *testing.T) {
	s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
		"HTCONDORDB_ARCHIVE_MAX_BYTES = 10 GB\n"+
		"HTCONDORDB_JOB_METRICS = true\n"+
		"HTCONDORDB_JOB_METRICS_MAX_BYTES = 0\n"))
	if s.metricsMaxBytes != 0 {
		t.Errorf("metricsMaxBytes = %d, want 0 (explicitly uncapped)", s.metricsMaxBytes)
	}
	if s.historyMaxBytes != 10*1000*1000*1000 {
		t.Errorf("historyMaxBytes = %d; the shared default must still cap the other tables", s.historyMaxBytes)
	}
}

// TestJobMetricsSegmentSizeNegative: a nonsense value falls back to the library default rather
// than being passed through to the archive, which would refuse to open.
func TestJobMetricsSegmentSizeNegative(t *testing.T) {
	s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
		"HTCONDORDB_JOB_METRICS = true\nHTCONDORDB_JOB_METRICS_SEGMENT_SIZE = -1\n"))
	if s.metricsSegSize != 0 {
		t.Errorf("metricsSegSize = %d, want 0 (library default)", s.metricsSegSize)
	}
}

// TestJobMetricsRetentionAttrIsZoned: age retention measures against a zone-mapped attribute (the
// archive keeps a per-segment maximum of it), so the attribute the retention names and the one the
// table is indexed on cannot drift apart.
func TestJobMetricsRetentionAttrIsZoned(t *testing.T) {
	if !slices.Contains(scheddsync.JobMetricsZoneAttrs, scheddsync.SampleTimeAttr) {
		t.Fatalf("%s must be in JobMetricsZoneAttrs %v: age retention measures against it",
			scheddsync.SampleTimeAttr, scheddsync.JobMetricsZoneAttrs)
	}
}

// TestJobMetricsGroupSchemasKnob: grouping defaults ON (matching the library) and the knob is an
// opt-OUT. That asymmetry is the whole reason it needs a test -- configBool defaults to false, so
// a knob whose absence must mean "on" is easy to wire up backwards, and getting it wrong would
// silently disable the columnar path for GPU and container attributes on every deployment.
func TestJobMetricsGroupSchemasKnob(t *testing.T) {
	on := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+"HTCONDORDB_JOB_METRICS = true\n"))
	if !on.metricsGroupSchemas {
		t.Error("group schemas should default to on")
	}
	if got := groupSchemaCount(on.metricsGroupSchemas); got != 0 {
		t.Errorf("groupSchemaCount(on) = %d, want 0 (library default)", got)
	}
	off := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
		"HTCONDORDB_JOB_METRICS = true\nHTCONDORDB_JOB_METRICS_GROUP_SCHEMAS = false\n"))
	if off.metricsGroupSchemas {
		t.Error("HTCONDORDB_JOB_METRICS_GROUP_SCHEMAS = false must turn them off")
	}
	if got := groupSchemaCount(off.metricsGroupSchemas); got != -1 {
		t.Errorf("groupSchemaCount(off) = %d, want -1 (build none)", got)
	}
	// Explicitly true is on, which is the case a set-ness check gets wrong if it ignores the value.
	explicit := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
		"HTCONDORDB_JOB_METRICS = true\nHTCONDORDB_JOB_METRICS_GROUP_SCHEMAS = true\n"))
	if !explicit.metricsGroupSchemas {
		t.Error("an explicit true must be on")
	}
}
