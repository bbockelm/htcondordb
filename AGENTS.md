# AGENTS.md

Notes for automated agents working in this repo. Keep it short; add only
non-obvious things that have bitten us.

## This daemon is on both ends of a wire protocol

htcondordb is the `dbrpc` **server**, and also a `dbrpc` **client** several
times over (`capi`, `cedarsync`, the exporters, `htcondordb-cli`, the Grafana
plugin, `history-import`). Neither end controls the other's version: an access
point upgrades its client on its own schedule, and a mirror is upgraded on the
operators'.

The full rules live in the `classad` repo's `AGENTS.md`, next to the protocol.
The three that get violated here:

**Never break an old client.** A released opcode's framing and reply shape are
fixed. New capability is a new opcode. Our clients are deployed on machines we
do not administer and nothing warns their operators.

**Ask, do not probe.** `Client.Supports(Feature)` answers from what the server
advertised in the CEDAR handshake. Reach for a new `Err*Unsupported` sentinel
only for a peer that advertises nothing, and say so in a comment.

**`SupportUnknown` is not `SupportNo`.** Every feature predates advertisement,
so a silent server may implement all of them.

When adding a server-side opcode, also add its `Feature` to
`serverFeatures` in classad's `dbrpc` and bump `ProtocolVersion`, or clients
keep probing for something we have.

## Projections may return more than was asked for

The server returns at least the transitive closure of the projected attributes,
which can be more attributes than the projection named. Callers must tolerate
extras; a caller needing exactly the requested set trims them itself.

This includes `capi`, which relays rows to a C++ schedd answering `condor_q`.
A projected `condor_q -l` served from the mirror may show attributes the
submitter did not request, where the same query served from the queue would
not. That is accepted: the alternative is parsing rows the byte relay exists to
avoid touching.

The reason for the rule is that the closure cannot be computed anywhere else.
Whether an attribute holds an expression over its siblings (`RequestMemory`
written over `MemoryUsage`) is a property of the stored ad, so a projection
that drops the siblings yields a plausible wrong number rather than an error.

## Before pinning a classad version

`classad`, `collections`, `db` and `dbrpc` are separate modules released
together. Pin them in one commit, only after the version is tagged, and run
`make lib-check-linux` before pushing anything touching `capi/` — every cgo
file there needs its own `#include <stdint.h>`.
