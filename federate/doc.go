// Package federate runs an htcondordb as a federation hub: it fans the spokes of an AP set -- the
// per-access-point htcondordbs that run schedd sync -- into one catalog, so one query covers
// every AP.
//
// It reuses cedarsync's Runner (dial, Watch, backoff, cursor), one per (spoke, table), and
// replaces its sinks, because the stock replicate sinks produce wrong data in an aggregate: they
// key rows by the raw source key (two APs' "123.0" collide), ignore Reset (rows deleted at a spoke
// during a disconnect live on as phantoms), re-append an archive's whole history on every Reset,
// commit the cursor only at Synced, and stamp the source only when the row does not already claim
// one. See tableSink and archiveSink.
//
// Tables. jobs and syncstatus are mutable; history and epoch_history are archives; the hub's own
// federation_sources is mutable, one row per schedd, keyed by the lowercased schedd name (schedd
// names are case-insensitive, as in HTCondor) and persisted. Every replicated row carries
// ScheddName, overwritten from the spoke's validated identity. Mutable-table keys are produced by
// HubKey alone; the encoding is internal and may change (which would require rebuilding the hub's
// mutable tables), so readers select on ScheddName, ClusterId and ProcId, never on the key. The
// spoke's own Key attribute is carried through unchanged. The daemon registers every table the hub
// writes (HubTables) as owned, so clients may read and watch them but not write or drop them.
//
// Membership is sticky: a schedd missing from the collector is "absent", its rows are kept and its
// runners keep streaming from the last validated spoke address. Rows are deleted only by
// retirement -- RetireAfter unseen while the hub runs (in the collector or over a live stream),
// RetireAfter since it stopped matching the constraint ("retiring"), or an explicit Retire.
// Retirement removes the source's cursors, then deletes from the mutable tables only; archives are
// append-only and age out through their retention.
//
// Host validation (Discovery) catches misconfiguration. Trust in spoke ads is the collector's
// advertise authorization, as for any HTCondor ad; spokes behind CCB or NAT pair statically.
package federate
