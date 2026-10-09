package main

import (
	"errors"
	"slices"
	"strings"
	"sync"

	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/server"
)

// Owners in the server's table-ownership registry (server.TableOwners). Each manager registers
// the tables its writers maintain under its own name and re-registers on every reconfigure; the
// server then refuses remote writes to those tables, and DBSyncControl routes operator actions on
// them through the owner (synccontrol.go).
const (
	ownerScheddSync    = "schedd-sync"    // scheddSyncManager: the job_queue.log tables, history, epoch_history, job_metrics
	ownerReplication   = "replication"    // cedarSyncManager: every HTCONDORDB_REPLICATE_<NAME>_TARGET
	ownerHistoryImport = "history-import" // importerManager: every managed HTCONDORDB_HISTORY_IMPORT job's TABLE
	ownerFederation    = "federation"     // federationManager: the federated tables and federation_sources
)

// unionTables is a ∪ b, case-insensitively (the registry folds table names).
func unionTables(a, b []string) []string {
	out := slices.Clone(a)
	for _, t := range b {
		if !slices.ContainsFunc(out, func(x string) bool { return strings.EqualFold(x, t) }) {
			out = append(out, t)
		}
	}
	return out
}

// reconfigRetry carries a manager whose reapply was refused because another manager still held one
// of its tables (server.ErrTableClaimed) to the end of the reconfigure. condor_reconfig applies the
// managers one after another in a fixed order, so a table moving between writers in one
// reconfigure -- a hub becoming schedd sync, or a replication target taking a table schedd sync
// gives up -- is refused while the old writer still holds it, and succeeds once every manager has
// been applied. Each manager claims before it stops anything, so a refused apply changed nothing.
type reconfigRetry struct {
	mu      sync.Mutex
	pending []deferredApply
}

type deferredApply struct {
	name  string
	apply func(*config.Config) error
}

// run applies cfg with apply. An ownership conflict is deferred to flush, not returned.
func (r *reconfigRetry) run(name string, cfg *config.Config, apply func(*config.Config) error) error {
	err := apply(cfg)
	if errors.Is(err, server.ErrTableClaimed) {
		r.mu.Lock()
		r.pending = append(r.pending, deferredApply{name: name, apply: apply})
		r.mu.Unlock()
		return nil
	}
	return err
}

// flush re-applies the deferred managers once, in order, and returns the ones that still fail,
// keyed by name.
func (r *reconfigRetry) flush(cfg *config.Config) map[string]error {
	r.mu.Lock()
	pending := r.pending
	r.pending = nil
	r.mu.Unlock()
	var failed map[string]error
	for _, p := range pending {
		if err := p.apply(cfg); err != nil {
			if failed == nil {
				failed = map[string]error{}
			}
			failed[p.name] = err
		}
	}
	return failed
}
