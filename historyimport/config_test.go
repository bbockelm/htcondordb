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

func TestJobsFromConfigSource(t *testing.T) {
	kv := map[string]string{
		"HTCONDORDB_HISTORY_IMPORT":                "hist epochs epochs2",
		"HTCONDORDB_HISTORY_IMPORT_HIST_POOL":      "p:9618",
		"HTCONDORDB_HISTORY_IMPORT_EPOCHS_POOL":    "p:9618",
		"HTCONDORDB_HISTORY_IMPORT_EPOCHS_SOURCE":  "epoch",
		"HTCONDORDB_HISTORY_IMPORT_EPOCHS2_POOL":   "p:9618",
		"HTCONDORDB_HISTORY_IMPORT_EPOCHS2_SOURCE": "epoch",
		"HTCONDORDB_HISTORY_IMPORT_EPOCHS2_TABLE":  "my_epochs",
	}
	get := func(k string) (string, bool) { v, ok := kv[k]; return v, ok }
	jobs, err := JobsFromConfig(get)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]Job{}
	for _, j := range jobs {
		byName[j.Name] = j
	}
	// Default source is history, default table "history".
	if byName["hist"].Source != SourceHistory || byName["hist"].Table != DefaultTable {
		t.Errorf("hist = %+v, want history/%s", byName["hist"], DefaultTable)
	}
	// Epoch source defaults the table to the epoch table (no collision with history).
	if byName["epochs"].Source != SourceEpoch || byName["epochs"].Table != DefaultEpochTable {
		t.Errorf("epochs = %+v, want epoch/%s", byName["epochs"], DefaultEpochTable)
	}
	// An explicit TABLE still wins for an epoch job.
	if byName["epochs2"].Source != SourceEpoch || byName["epochs2"].Table != "my_epochs" {
		t.Errorf("epochs2 = %+v, want epoch/my_epochs", byName["epochs2"])
	}
}

func TestJobsFromConfigInvalidSource(t *testing.T) {
	kv := map[string]string{
		"HTCONDORDB_HISTORY_IMPORT":          "j",
		"HTCONDORDB_HISTORY_IMPORT_J_POOL":   "p:9618",
		"HTCONDORDB_HISTORY_IMPORT_J_SOURCE": "transfer",
	}
	get := func(k string) (string, bool) { v, ok := kv[k]; return v, ok }
	if _, err := JobsFromConfig(get); err == nil {
		t.Error("an unsupported SOURCE should error")
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
