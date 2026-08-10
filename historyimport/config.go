// Package historyimport aggregates completed-job history from the schedds of one
// or more HTCondor pools into an htcondordb archive table, via remote
// condor_history. It is the network-pull counterpart to scheddsync, which tails a
// schedd's local history file: one importer can pull from every schedd in a pool
// (selected by a requirements expression) and land them all in a single table.
//
// Each import job names a pool, a schedd-selection constraint, and a target
// table; several jobs may run at once and may share a table. Imports are
// incremental (a per-schedd cursor drives condor_history's `since`) and stamp
// ScheddName on every record so aggregated tables stay attributable and
// dedupable across schedds.
package historyimport

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// ListKnob names the configured import jobs (space/comma separated).
	ListKnob = "HTCONDORDB_HISTORY_IMPORT"
	// knobPrefix + <UPPER(name)> + "_" prefixes a job's per-instance knobs, matching
	// the HTCONDORDB_REPLICATE_<NAME>_* convention used by the cedarsync manager.
	knobPrefix = "HTCONDORDB_HISTORY_IMPORT_"

	// DefaultTable is the target archive table for a completed-job (history) import
	// when a job sets no TABLE; DefaultEpochTable is the same for an epoch import.
	DefaultTable      = "history"
	DefaultEpochTable = "epoch_history"
	// DefaultInterval is the poll interval when a job sets no INTERVAL_SECONDS.
	DefaultInterval = 5 * time.Minute
)

// SourceKind selects which schedd history stream an import job pulls.
type SourceKind string

const (
	// SourceHistory imports completed-job records (condor_history) -- one per job.
	SourceHistory SourceKind = "history"
	// SourceEpoch imports per-run-instance records (condor_history -epochs) --
	// multiple per job, keyed by RunInstanceID and ordered by EpochWriteDate.
	SourceEpoch SourceKind = "epoch"
)

// Getter reads a configuration knob (config.Config.Get's shape), so config
// parsing is testable without a real config object.
type Getter func(key string) (value string, ok bool)

// Job is one configured import job: pull history from the schedds of Pool that
// match ScheddConstraint (optionally filtered by Constraint) into Table.
type Job struct {
	Name             string        // instance name from the ListKnob
	Pool             string        // collector address to discover schedds through
	ScheddConstraint string        // ClassAd expr selecting schedds ("" = all in the pool)
	Source           SourceKind    // which history stream to import (default SourceHistory)
	Table            string        // target archive table (may be shared across jobs)
	Constraint       string        // optional per-job history filter ("" = all records)
	Interval         time.Duration // poll interval
	MaxPerCycle      int           // per-schedd cap on records pulled in one cycle (0 = unlimited); bounds the first, bootstrap cycle
}

// JobsFromConfig parses the HTCONDORDB_HISTORY_IMPORT_<NAME>_* knobs into jobs.
// It returns nil (no error) when the feature is unconfigured, and an error when a
// named job is missing its required POOL.
func JobsFromConfig(get Getter) ([]Job, error) {
	raw, ok := get(ListKnob)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var jobs []Job
	seen := map[string]bool{}
	for _, name := range fields(raw) {
		if seen[strings.ToLower(name)] {
			return nil, fmt.Errorf("%s: duplicate import job %q", ListKnob, name)
		}
		seen[strings.ToLower(name)] = true

		p := knobPrefix + strings.ToUpper(name) + "_"
		j := Job{Name: name, Source: SourceHistory, Interval: DefaultInterval}

		pool, ok := get(p + "POOL")
		if !ok || strings.TrimSpace(pool) == "" {
			return nil, fmt.Errorf("%sPOOL is required for import job %q", p, name)
		}
		j.Pool = strings.TrimSpace(pool)

		if v, ok := get(p + "SOURCE"); ok && strings.TrimSpace(v) != "" {
			switch SourceKind(strings.ToLower(strings.TrimSpace(v))) {
			case SourceHistory:
				j.Source = SourceHistory
			case SourceEpoch:
				j.Source = SourceEpoch
			default:
				return nil, fmt.Errorf("%sSOURCE %q is invalid (want history or epoch)", p, v)
			}
		}
		// The default table depends on the source so a history and an epoch job with
		// no explicit TABLE do not collide in one table.
		j.Table = DefaultTable
		if j.Source == SourceEpoch {
			j.Table = DefaultEpochTable
		}

		if v, ok := get(p + "SCHEDD_CONSTRAINT"); ok {
			j.ScheddConstraint = strings.TrimSpace(v)
		}
		if v, ok := get(p + "TABLE"); ok && strings.TrimSpace(v) != "" {
			j.Table = strings.TrimSpace(v)
		}
		if v, ok := get(p + "CONSTRAINT"); ok {
			j.Constraint = strings.TrimSpace(v)
		}
		if v, ok := get(p + "INTERVAL_SECONDS"); ok {
			if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
				j.Interval = time.Duration(n) * time.Second
			}
		}
		if v, ok := get(p + "MAX_RECORDS_PER_CYCLE"); ok {
			if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
				j.MaxPerCycle = n
			}
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// fields splits a knob value on commas and whitespace, dropping empties.
func fields(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})
}
