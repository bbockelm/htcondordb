// Package command defines the CEDAR command integers htcondordb serves.
//
// The values come from HTCondor's old, unused transferd command block
// (TRANSFERD_BASE = 74000 in condor_commands.h, commented out and marked "Not
// used"). Reusing that reserved-but-idle range keeps htcondordb clear of every
// live HTCondor command int while staying inside the historical numbering
// scheme, so it will not collide with a future allocation in the low ranges.
package command

// Base is the command block htcondordb occupies: HTCondor's retired
// TRANSFERD_BASE. Nothing else in the tree uses it.
const Base = 74000

const (
	// DBSession is the multiplexed dbrpc session. A single authenticated CEDAR
	// connection carries the whole dbrpc mux (transactions, queries, watches).
	// It is registered at READ so any authorized reader may open it; the
	// handler then re-checks WRITE/DAEMON on the authenticated identity to pick
	// the effective access level (read-only + private-stripped at READ, full at
	// WRITE, replication/administrative surface at DAEMON).
	DBSession = Base + 0 // 74000

	// DBReplicate is the leader->follower commit stream in "leader-follower" HA
	// mode: a follower opens it against the leader and receives every committed
	// change. DAEMON-level.
	DBReplicate = Base + 1 // 74001

	// DBRaft tunnels the hashicorp/raft transport over CEDAR in "consistent" HA
	// mode (RequestVote/AppendEntries/InstallSnapshot), so raft inherits the
	// daemon's authentication and encryption. DAEMON-level.
	DBRaft = Base + 2 // 74002

	// DBControl answers cluster/HA control queries: who is the current leader
	// (for client redirect), the configured member set, and bootstrap
	// registration of the first N daemon-level peers. DAEMON-level for
	// mutating operations; leader lookup is READ.
	DBControl = Base + 3 // 74003

	// DBSyncControl carries administrative control of the daemon's sync sources -- the
	// schedd-sync tailers (jobs/history) and the managed change-data exporters -- via a ClassAd
	// request/response protocol. Currently: resync a source (re-read/re-export from the start).
	// DAEMON-level; registered in every mode (not just HA).
	DBSyncControl = Base + 4 // 74004

	// DBSyncStatus reports the daemon's per-source sync health -- how far behind each schedd-sync
	// tailer is, when it last synced, and whether it hit a durability gap -- as a ClassAd shaped
	// exactly like the one the daemon advertises to the collector.
	//
	// This exists because the collector ad is not always reachable. A client that finds the daemon
	// through its address file (no collector in the picture) can read the command address but has
	// nowhere to learn freshness from, and a client that must not read a mirror that has fallen
	// behind then has no way to tell. Asking the daemon directly, over the connection it already
	// has, closes that gap.
	//
	// READ-level, unlike the DAEMON-level DBSyncControl above: this only reads health, and the
	// clients that need it (readers deciding whether to trust the mirror) are readers.
	DBSyncStatus = Base + 5 // 74005
)
