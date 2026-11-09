# bench — how fast the store hands results to a client

An iperf-shaped pair of modes on the `htcondordb` binary: one serves, one measures.

## Server

    htcondordb -benchmark -benchmark-dir /scratch/bench -benchmark-rows 200000 \
               -benchmark-listen 0.0.0.0:9777 -benchmark-flight 0.0.0.0:9778

Generates jobs, history and epoch_history, columnarizes and indexes them, then serves with
**anonymous reads and an open Flight SQL endpoint** — do not expose those ports.

`-benchmark-dir` is required and is guarded: the directory must be empty or carry the marker
from a previous run. The mode is a flag rather than a config key, and it intercepts before any
configuration is read, so it can never be holding a real SPOOL or pool password.

## Client

    htcondordb -benchmark-client 10.0.0.5:9777 -benchmark-client-flight 10.0.0.5:9778 \
               -benchmark-table history -benchmark-clients 8 -benchmark-iterations 5

Runs the load **in its own process**, which is the point: a generator sharing the daemon's
cores measures itself as much as the daemon. An in-process sweep of this workload reported four
concurrent readers as slower than one *and* slower than eight — contention between the clients
and the server they were timing, not a scaling curve.

It dials a **plain, unauthenticated** dbrpc socket, which is what benchmark mode serves. A live
daemon's dbrpc port authenticates with CEDAR, so pointing this at one does not work yet; measuring
a production `jobs` or `history` table needs the CEDAR dialer the CLI uses.

`-benchmark-self-test` measures in process and exits. It accepts the contention above to stay a
single command; it is for a quick look, not for a number about a deployment.

## Reading the output

**Transports are products, not speeds.** The stored form ships the ad carrying the expression it
was written with; the evaluated form ships the number that expression computes to. They differ by
roughly 10x and a consumer needs one or the other, so the report groups by form and a rate from
one group says nothing about the other.

**Check the arms chase references alike.** `dbrpc/text-projected` and `dbrpc/wire-refs` both
return self-contained rows. `dbrpc/wire` is the cheaper subset form — pairing it against
text-projected measures the reference closure as if it were the transport.

**The store's state is printed above the numbers, and the warnings matter.** The same query on
the same data differs severalfold on columnar coverage and indexes, so a rate published without
them is not comparable with anything.

Arrow is left out of the default sweep because it needs a second server endpoint; passing
`-benchmark-client-flight` adds it. `-benchmark-transports` names an explicit set instead.
Arrow filters with `-benchmark-sql-where`, which the caller asserts matches the ClassAd
`-benchmark-constraint`: the tool will not translate between the two languages.
