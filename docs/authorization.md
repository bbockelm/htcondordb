# Authorization & security

## Access levels

One authenticated CEDAR connection carries an entire `dbrpc` multiplex. The
access level is decided once, at connect time, from the authenticated identity
(re-evaluated per connection, so a reconfigure takes effect on the next
connection):

- **READ** — read-only, and every returned ad has its private (secret)
  attributes stripped (claim ids, capabilities, transfer keys).
- **WRITE** — full read/write; private attributes visible.
- **DAEMON** — WRITE plus the HA/replication surface (commit stream, raft
  transport, cluster control).

The command is registered at READ; the handler escalates to WRITE/DAEMON by
re-checking the `ALLOW_`/`DENY_` policy on the peer. Private stripping and
read-only enforcement are implemented in `dbrpc` via per-connection
`ServeOptions`.

## Commands (from the retired transferd block)

| Command | Int | Level | Purpose |
|---------|-----|-------|---------|
| `DBSession` | 74000 | READ | The multiplexed DB RPC session. |
| `DBReplicate` | 74001 | DAEMON | (reserved) dedicated commit stream. |
| `DBRaft` | 74002 | DAEMON | Raft transport tunneled over CEDAR. |
| `DBControl` | 74003 | WRITE | Consistent-mode control (leader discovery, peer registration, write-batch apply). |

## Getting WRITE

Two independent things must both hold for a client to write (INSERT/UPDATE/DELETE):

1. **The client is authenticated**, so the daemon has an identity to authorize.
   HTCondor's default `SEC_*_AUTHENTICATION` is `OPTIONAL`, and OPTIONAL on both
   ends negotiates to *no* authentication — leaving the peer anonymous (`user=""`)
   and read-only. htcondordb therefore *prefers* authentication by default (it
   runs whenever a mutually-supported method exists, e.g. `FS` for a local client,
   and still admits a peer with no method as read-only). You normally don't need
   to set anything; to force it, `SEC_DEFAULT_AUTHENTICATION = REQUIRED`.

2. **The identity is authorized for WRITE.** With `ALLOW_WRITE` unset, WRITE is
   fail-closed (denied), even for an authenticated user — so you must grant it.

Quick start for **local development** (anonymous writes, no auth needed):

```
ALLOW_WRITE  = *
ALLOW_DAEMON = *          # only if you use an HA mode
```

**Identity-based** (recommended for real use): let FS/TOKEN/SSL map the user and
authorize that identity:

```
SEC_DEFAULT_AUTHENTICATION = PREFERRED      # (htcondordb already prefers it)
ALLOW_WRITE  = you@your.uid.domain
ALLOW_DAEMON = other-daemon@your.uid.domain
```

The daemon logs each connection's outcome at Info —
`htcondordb session opened … user=<fqu> level=READ|WRITE|DAEMON` — which is the
quickest way to see the identity you mapped to and the level it was granted.

Standard `SEC_*` and `ALLOW_`/`DENY_` knobs configure security and authorization.
