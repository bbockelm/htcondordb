# qbench — a condor_q that measures instead of printing

Issues the same `QUERY_JOB_ADS` a `condor_q` makes and throws the rows away, so a measurement
is of the schedd and not of the client's output path.

It exists because the obvious instruments mislead:

- **Timing `condor_q`** measures its formatting as much as the schedd's delivery. `-l` over
  20,000 ads spends most of two seconds in the client.
- **Timing a forked `condor_ping`** measures `fork()` under load. Sixteen CPU burners that
  never touch the schedd pushed a ping from 69 ms to 3410 ms, which reads exactly like an
  event-loop stall and is not one.

So the load runs K queries as goroutines in one process (`-k`), and `-probe` times
`DC_NOP_READ` **in-process** — no fork, no exec, no configuration parse; about 1 ms idle
against 69 ms for the forked equivalent.

`-mode raw` discards each response without building a ClassAd; `-mode parse` builds one per
ad. The two measure the same, which is itself the finding that client-side parsing is not a
cost worth optimising.

`-probe-stall-cmd` runs a command the moment a probe exceeds `-probe-stall`, because a stall
is the only moment the evidence exists:

    qbench -addr "$(condor_status -schedd -af MyAddress)" -const 'ProcId<4000' \
           -attrs ClusterId,ProcId,JobStatus -k 16 -probe \
           -probe-stall 1s -probe-stall-cmd 'sample $(pgrep -n condor_schedd) 3 -file /tmp/stall.txt'

Not built by `make`: `go build ./cmd/qbench`.
