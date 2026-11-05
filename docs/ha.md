# High availability (`HTCONDORDB_HA_MODE`)

htcondordb runs as a single daemon unless you tell it otherwise. Two replicated
modes are available; pick by what you need.

| Mode | Pick it to | Writes go to | Reads served by |
|------|-----------|--------------|-----------------|
| `standalone` (default) | run one daemon | the daemon | the daemon |
| `leader-follower` | spread read load, keep a warm standby | the leader | the leader and every follower |
| `consistent` | never lose an acknowledged write | the leader (clients are redirected there) | any node |

`leader-follower` replicates asynchronously: a follower can be behind, and a
write acknowledged by a leader that then dies may not have reached one. Use
`consistent` where that matters.

## `leader-follower`

### On the leader

An ordinary read/write daemon, plus the mode. Followers connect at DAEMON level,
so authorize them:

```conf
HTCONDORDB_HA_MODE = leader-follower
HTCONDORDB_ROLE    = leader

ALLOW_DAEMON = $(ALLOW_DAEMON), condor@users.htcondor.org/db2.example.org, condor@users.htcondor.org/db3.example.org
```

### On each follower

```conf
HTCONDORDB_HA_MODE = leader-follower
HTCONDORDB_ROLE    = follower
HTCONDORDB_LEADER  = <db1.example.org:9618?sock=htcondordb>

# Where the replication cursor is kept, so a restart resumes instead of re-reading.
# Defaults to $(SPOOL)/htcondordb/.replica_cursor.
#HTCONDORDB_CURSOR_FILE = /var/lib/condor/spool/htcondordb/.replica_cursor
```

A follower serves reads from its own copy and refuses writes. Send writes to the
leader:

```sh
# read from the nearest follower
htcondordb-cli -addr '<db2.example.org:9618?sock=htcondordb>' -e "SELECT COUNT(*) FROM jobs"

# write to the leader
htcondordb-cli -addr '<db1.example.org:9618?sock=htcondordb>' -e "DELETE FROM machines WHERE Name == \"old.example.org\""
```

### Check it

Each daemon logs its mode as it starts:

```
htcondordb starting ... ha_mode=leader-follower role=follower read_only=true
starting leader-follower replication leader=<db1.example.org:9618?sock=htcondordb>
```

Compare a row count on the leader and a follower to see how far behind it is.

## `consistent`

A write is proposed to a raft log and applied by every node once a quorum has
durably accepted it, so no acknowledged write is lost while a quorum survives.
Replication is tunneled over CEDAR, so it uses the same HTCondor authentication
and encryption as everything else — and every node needs DAEMON on every other.

### A three-node cluster

Bring up one node with `HTCONDORDB_RAFT_BOOTSTRAP` and point the rest at it;
each joiner registers itself, so you do not enumerate members by hand.

On `db1` (the one that bootstraps):

```conf
HTCONDORDB_HA_MODE       = consistent
HTCONDORDB_RAFT_BOOTSTRAP = true
HTCONDORDB_RAFT_SIZE      = 3
HTCONDORDB_NODE_ID        = db1

ALLOW_DAEMON = $(ALLOW_DAEMON), condor@users.htcondor.org/db2.example.org, condor@users.htcondor.org/db3.example.org
```

On `db2` and `db3` (same but for the id, and no bootstrap):

```conf
HTCONDORDB_HA_MODE = consistent
HTCONDORDB_RAFT_SEED = <db1.example.org:9618?sock=htcondordb>
HTCONDORDB_NODE_ID   = db2

ALLOW_DAEMON = $(ALLOW_DAEMON), condor@users.htcondor.org/db1.example.org, condor@users.htcondor.org/db3.example.org
```

The bootstrap node admits the first `HTCONDORDB_RAFT_SIZE - 1` peers that
register and refuses the rest, so the quorum size is configured once, on one
node.

To fix membership at bootstrap instead, name every member on the bootstrap node
and leave `HTCONDORDB_RAFT_SEED` unset everywhere:

```conf
HTCONDORDB_RAFT_PEERS = db1@<db1.example.org:9618?sock=htcondordb> db2@<db2.example.org:9618?sock=htcondordb> db3@<db3.example.org:9618?sock=htcondordb>
```

### Reading and writing

Any node answers a read. A write needs `-consistent`, which routes it through
the cluster and follows the redirect to whichever node is leader — so the same
command works against any of them:

```sh
htcondordb-cli -addr '<db2.example.org:9618?sock=htcondordb>' -e "SELECT COUNT(*) FROM jobs"
htcondordb-cli -addr '<db2.example.org:9618?sock=htcondordb>' -consistent load -table machines < machines.ads
```

### Check it

```
consistent HA (raft over CEDAR) started node_id=db1 bootstrap=true cluster_size=3 peers=0
consistent HA: self-joining cluster ...
consistent HA: joined cluster ...
```

A joiner retries until the seed is up and has elected a leader, so
`waiting to join cluster` during a rolling start is expected rather than an
error.

## Knobs

| Knob | Default | Meaning |
|------|---------|---------|
| `HTCONDORDB_HA_MODE` | `standalone` | `standalone`, `leader-follower` or `consistent`. |
| `HTCONDORDB_ROLE` | `leader` | Leader-follower only: `leader` or `follower`. |
| `HTCONDORDB_LEADER` | — | Leader-follower only, required on a follower: the leader's address. |
| `HTCONDORDB_CURSOR_FILE` | `$(SPOOL)/htcondordb/.replica_cursor` | Where a follower keeps its stream position. |
| `HTCONDORDB_RAFT_BOOTSTRAP` | `false` | Consistent only: this node initializes a fresh cluster. Set it on exactly one. |
| `HTCONDORDB_RAFT_SIZE` | `0` | Consistent only, on the bootstrap node: the number of members to admit. |
| `HTCONDORDB_RAFT_SEED` | — | Consistent only, on a joiner: any existing member's address to register through. |
| `HTCONDORDB_RAFT_PEERS` | — | Consistent only: an explicit `id@address` member list, instead of seed-based joining. |
| `HTCONDORDB_NODE_ID` | the advertised address | This node's stable identity in the cluster. |

Raft state is kept under `<HTCONDORDB_DIR>/raft/`, so a restarted node rejoins
with its committed log rather than bootstrapping again. Both modes need a
persistent `HTCONDORDB_DIR`.

See [Configuration](configuration.md) for every knob and
[Authorization](authorization.md) for the DAEMON-level replication surface.
