# High availability (`HTCONDORDB_HA_MODE`)

htcondordb runs standalone by default; two replicated modes are available.

## `standalone` (default)

A single read/write daemon.

## `leader-follower`

The leader is an ordinary read/write daemon. Each follower opens a DAEMON-level
session and consumes the leader's commit stream (the store's `Watch` feed),
applying every upsert/delete to its local database and persisting the stream
cursor, so a restart resumes without missing a change. If a follower has fallen
out of the leader's retention, the leader answers with a reset and replays full
state. Followers serve **read-only** queries from local state (offloading reads);
writes go to the leader. No transactional/quorum safety — replication is
asynchronous and best-effort.

Knobs: `HTCONDORDB_ROLE` (`leader`|`follower`), `HTCONDORDB_LEADER` (the leader's
address, for a follower), `HTCONDORDB_CURSOR_FILE`.

## `consistent`

Strong consistency via raft. A write is a `Batch` of mutations proposed to the
raft log; once a quorum durably accepts it, every node's FSM applies the same
batch, so all replicas converge and no acknowledged write is lost while a quorum
survives. **The raft transport is tunneled over CEDAR** (not raw TCP), so
replication inherits HTCondor authentication and encryption. Any node serves
reads; a write sent to a non-leader is answered with a redirect to the leader
(`ControlClient` follows it transparently).

Membership bootstraps from the initial leader, which is either given the peer set
explicitly (`HTCONDORDB_RAFT_PEERS = id1@addr1 id2@addr2 …`) or told the cluster
size `N` (`HTCONDORDB_RAFT_SIZE`) and adopts the first `N` DAEMON-authenticated
peers that register.

Knobs: `HTCONDORDB_RAFT_BOOTSTRAP` (bool, the initial leader),
`HTCONDORDB_RAFT_PEERS`, `HTCONDORDB_RAFT_SIZE`, `HTCONDORDB_NODE_ID`.

> Raft log + stable state are stored durably in boltdb (`<db>/raft/raft.db`) and
> FSM state in FileSnapshotStore snapshots, so a node's membership and committed
> log survive restarts (a restarted node replays its log into the FSM rather than
> re-bootstrapping). The REPL routes writes through the consistent path with
> `-consistent` (via `consistent.ControlClient`, which follows leader redirects).

See [Configuration](configuration.md) for the full knob list and
[Authorization](authorization.md) for the DAEMON-level replication surface.
