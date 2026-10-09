package main

// Owners in the server's table-ownership registry (server.TableOwners). Each manager registers
// the tables its writers maintain under its own name and re-registers on every reconfigure; the
// server then refuses remote writes to those tables, and DBSyncControl routes operator actions on
// them through the owner (synccontrol.go).
const (
	ownerScheddSync    = "schedd-sync"    // scheddSyncManager: the job_queue.log tables, history, epoch_history, job_metrics
	ownerReplication   = "replication"    // cedarSyncManager: every HTCONDORDB_REPLICATE_<NAME>_TARGET
	ownerHistoryImport = "history-import" // importerManager: every managed HTCONDORDB_HISTORY_IMPORT job's TABLE
)
