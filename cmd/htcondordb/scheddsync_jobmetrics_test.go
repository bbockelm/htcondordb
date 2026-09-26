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
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
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
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
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
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
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
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
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
	on, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+"HTCONDORDB_JOB_METRICS = true\n"))
	if !on.metricsGroupSchemas {
		t.Error("group schemas should default to on")
	}
	if got := groupSchemaCount(on.metricsGroupSchemas); got != 0 {
		t.Errorf("groupSchemaCount(on) = %d, want 0 (library default)", got)
	}
	off, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
		"HTCONDORDB_JOB_METRICS = true\nHTCONDORDB_JOB_METRICS_GROUP_SCHEMAS = false\n"))
	if off.metricsGroupSchemas {
		t.Error("HTCONDORDB_JOB_METRICS_GROUP_SCHEMAS = false must turn them off")
	}
	if got := groupSchemaCount(off.metricsGroupSchemas); got != -1 {
		t.Errorf("groupSchemaCount(off) = %d, want -1 (build none)", got)
	}
	// Explicitly true is on, which is the case a set-ness check gets wrong if it ignores the value.
	explicit, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
		"HTCONDORDB_JOB_METRICS = true\nHTCONDORDB_JOB_METRICS_GROUP_SCHEMAS = true\n"))
	if !explicit.metricsGroupSchemas {
		t.Error("an explicit true must be on")
	}
}

// mustResolve resolves settings and fails the test if any knob was unparseable -- so a test that
// does not care about the bad-knob list still cannot silently depend on one.
func mustResolve(t *testing.T, conf string) scheddSyncSettings {
	t.Helper()
	s, bad := resolveScheddSyncSettings(mkSyncCfg(t, conf))
	if len(bad) > 0 {
		t.Fatalf("unexpected unparseable knobs: %v", bad)
	}
	return s
}

// TestJobMetricsDurationKnobs pins the parsing of the two knobs where a silently-truncated value
// is destructive rather than merely wrong.
//
// HTCONDORDB_JOB_METRICS_MAX_AGE = 30d parsed as 30 SECONDS under fmt.Sscanf("%d"), which stops at
// the first non-digit and reports no error. The retention sweep then dropped every segment older
// than half a minute, on every hourly pass, for as long as the setting stood -- and the only trace
// was an INFO line reading max_age_seconds=30.
func TestJobMetricsDurationKnobs(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  float64 // seconds
		ok    bool
	}{
		{"2592000", 2592000, true}, // bare number is still seconds
		{"30d", 30 * 86400, true},  // the value that used to mean 30 seconds
		{"720h", 720 * 3600, true},
		{"90m", 5400, true},
		{"0", 0, true},
		// Unparseable resolves to NO CAP -- the safe direction -- and is reported, never guessed at.
		{"30 days", 0, false},
		{"2,592,000", 0, false},
		{"1e6", 0, false},
		{"lots", 0, false},
	} {
		s, bad := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
			"HTCONDORDB_JOB_METRICS = true\nHTCONDORDB_JOB_METRICS_MAX_AGE = "+tc.value+"\n"))
		if s.metricsMaxAge != tc.want {
			t.Errorf("_MAX_AGE = %q -> %v seconds, want %v", tc.value, s.metricsMaxAge, tc.want)
		}
		if reported := len(bad) > 0; reported == tc.ok {
			t.Errorf("_MAX_AGE = %q -> reported=%v, want reported=%v (bad=%v)",
				tc.value, reported, !tc.ok, bad)
		}
	}

	// The same parser backs the throttle, where the cost is volume rather than data.
	s := mustResolve(t, syncOn+"HTCONDORDB_JOB_METRICS = true\nHTCONDORDB_JOB_METRICS_MIN_INTERVAL = 5m\n")
	if s.metricsMinInterval != 300*time.Second {
		t.Errorf("_MIN_INTERVAL = 5m -> %v, want 5m", s.metricsMinInterval)
	}
}

// TestJobMetricsSegmentSizeKnob pins the other truncation hazard. "8 MiB" -- the exact string both
// docs tables print as the default -- parsed to 8 BYTES, which the storage layer accepts without
// complaint and which produces roughly two files per record.
func TestJobMetricsSegmentSizeKnob(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
		ok    bool
	}{
		{"8 MiB", 8 << 20, true}, // the value that used to mean 8 bytes
		{"2MiB", 2 << 20, true},
		{"8388608", 8 << 20, true},
		{"0", 0, true}, // explicitly "library default"
		// Below the floor, above the structural ceiling, or not a size at all: library default,
		// and reported. A silently tiny segment is the failure this guards.
		{"8", 0, false},
		{"1024", 0, false},
		{"8 EiB", 0, false},
		{"-1", 0, false},
		{"big", 0, false},
	} {
		s, bad := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
			"HTCONDORDB_JOB_METRICS = true\nHTCONDORDB_JOB_METRICS_SEGMENT_SIZE = "+tc.value+"\n"))
		if s.metricsSegSize != tc.want {
			t.Errorf("_SEGMENT_SIZE = %q -> %d, want %d", tc.value, s.metricsSegSize, tc.want)
		}
		if reported := len(bad) > 0; reported == tc.ok {
			t.Errorf("_SEGMENT_SIZE = %q -> reported=%v, want reported=%v (bad=%v)",
				tc.value, reported, !tc.ok, bad)
		}
	}
}
