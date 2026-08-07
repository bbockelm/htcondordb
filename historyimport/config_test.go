package historyimport

import (
	"testing"
	"time"
)

func TestJobsFromConfig(t *testing.T) {
	kv := map[string]string{
		"HTCONDORDB_HISTORY_IMPORT":                              "ospool, cm-east",
		"HTCONDORDB_HISTORY_IMPORT_OSPOOL_POOL":                  "cm.osg-htc.org:9618",
		"HTCONDORDB_HISTORY_IMPORT_OSPOOL_SCHEDD_CONSTRAINT":     `regexp("ap40", Machine)`,
		"HTCONDORDB_HISTORY_IMPORT_OSPOOL_TABLE":                 "ospool_history",
		"HTCONDORDB_HISTORY_IMPORT_OSPOOL_CONSTRAINT":            "JobUniverse == 5",
		"HTCONDORDB_HISTORY_IMPORT_OSPOOL_INTERVAL_SECONDS":      "120",
		"HTCONDORDB_HISTORY_IMPORT_OSPOOL_MAX_RECORDS_PER_CYCLE": "50000",
		// cm-east: only the required POOL; the rest defaults.
		"HTCONDORDB_HISTORY_IMPORT_CM-EAST_POOL": "cm-east.example:9618",
	}
	get := func(k string) (string, bool) { v, ok := kv[k]; return v, ok }

	jobs, err := JobsFromConfig(get)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Fatalf("got %d jobs, want 2", len(jobs))
	}

	o := jobs[0]
	if o.Name != "ospool" || o.Pool != "cm.osg-htc.org:9618" || o.Table != "ospool_history" {
		t.Errorf("ospool basics wrong: %+v", o)
	}
	if o.ScheddConstraint != `regexp("ap40", Machine)` || o.Constraint != "JobUniverse == 5" {
		t.Errorf("ospool constraints wrong: %+v", o)
	}
	if o.Interval != 120*time.Second || o.MaxPerCycle != 50000 {
		t.Errorf("ospool interval/max wrong: %v / %d", o.Interval, o.MaxPerCycle)
	}

	e := jobs[1]
	if e.Name != "cm-east" || e.Pool != "cm-east.example:9618" {
		t.Errorf("cm-east basics wrong: %+v", e)
	}
	if e.Table != DefaultTable || e.Interval != DefaultInterval {
		t.Errorf("cm-east defaults wrong: table=%q interval=%v", e.Table, e.Interval)
	}
}

func TestJobsFromConfigUnconfigured(t *testing.T) {
	get := func(string) (string, bool) { return "", false }
	jobs, err := JobsFromConfig(get)
	if err != nil || jobs != nil {
		t.Errorf("unconfigured should be (nil, nil): %v, %v", jobs, err)
	}
}

func TestJobsFromConfigMissingPool(t *testing.T) {
	kv := map[string]string{"HTCONDORDB_HISTORY_IMPORT": "j1"}
	get := func(k string) (string, bool) { v, ok := kv[k]; return v, ok }
	if _, err := JobsFromConfig(get); err == nil {
		t.Error("a job without POOL should error")
	}
}

func TestJobsFromConfigDuplicate(t *testing.T) {
	kv := map[string]string{
		"HTCONDORDB_HISTORY_IMPORT":         "j1 J1",
		"HTCONDORDB_HISTORY_IMPORT_J1_POOL": "p:9618",
	}
	get := func(k string) (string, bool) { v, ok := kv[k]; return v, ok }
	if _, err := JobsFromConfig(get); err == nil {
		t.Error("a duplicate job name (case-insensitive) should error")
	}
}
