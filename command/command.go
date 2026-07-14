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
)
