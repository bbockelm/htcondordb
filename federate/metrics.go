package federate

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are a hub's counters and gauges. They are Prometheus collectors (mounted on the daemon's
// /metrics alongside the store's own) and are also read directly by tests, which count writes,
// dedup hits and phantom deletes rather than only inspecting final state.
type Metrics struct {
	EventsApplied    *prometheus.CounterVec // labels: table, schedd, kind (upsert|delete)
	IdenticalSkips   *prometheus.CounterVec // labels: table, schedd -- catch-up upserts equal to the stored row
	Resets           *prometheus.CounterVec // labels: table, schedd
	DedupHits        *prometheus.CounterVec // labels: table, schedd -- archive records already present
	MissingIdentity  *prometheus.CounterVec // labels: table, schedd -- archive records with no dedup identity
	PhantomDeletes   *prometheus.CounterVec // labels: table, schedd -- rows a Reset replay did not re-send
	Undecodable      *prometheus.CounterVec // labels: table, schedd -- changes whose ad could not be decoded
	BelowRetention   *prometheus.CounterVec // labels: table, schedd -- replayed records the hub's cap already dropped
	TooDeep          *prometheus.CounterVec // labels: table, schedd -- archive records nested too deeply to store
	RejectedSpokes   *prometheus.CounterVec // labels: reason
	Retired          prometheus.Counter
	SourceStaleness  *prometheus.GaugeVec // labels: schedd
	SourcesByState   *prometheus.GaugeVec // labels: state
	StreamConnected  *prometheus.GaugeVec // labels: table, schedd -- a watch session is open
	StreamSynced     *prometheus.GaugeVec // labels: table, schedd -- that session finished its catch-up
	stalenessMu      sync.Mutex
	stalenessSchedds map[string]bool
	streamsMu        sync.Mutex
	streams          map[streamKey]bool
}

// streamKey identifies one replication stream: one table of one source AP.
type streamKey struct{ table, schedd string }

// NewMetrics builds an unregistered metric set.
func NewMetrics() *Metrics {
	const ns, sub = "htcondordb", "federate"
	cv := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: name, Help: help}, labels)
	}
	return &Metrics{
		EventsApplied: cv("events_applied_total",
			"Replicated changes written to the hub, by table, source AP and kind (upsert or delete).", "table", "schedd", "kind"),
		IdenticalSkips: cv("identical_skips_total",
			"Catch-up upserts not written because the hub already held an identical row, by table and source AP.", "table", "schedd"),
		Resets: cv("resets_total",
			"Full replays (Reset) received from spokes, by table and source AP. Every spoke restart causes one per mutable table; an archive only after an unclean one.", "table", "schedd"),
		DedupHits: cv("dedup_hits_total",
			"Archive records dropped during catch-up because the hub already held one with the same identity, by table and source AP.", "table", "schedd"),
		MissingIdentity: cv("missing_identity_total",
			"Archive records appended without a dedup check because they lack GlobalJobId (or, for epochs, RunInstanceID), by table and source AP.", "table", "schedd"),
		PhantomDeletes: cv("phantom_deletes_total",
			"Hub rows deleted at the end of a Reset replay because the spoke no longer has them, by table and source AP.", "table", "schedd"),
		Undecodable: cv("undecodable_total",
			"Replicated upserts whose ad could not be decoded, by table and source AP. A mutable table keeps the row it holds for the key; an archive skips the record.", "table", "schedd"),
		BelowRetention: cv("below_retention_total",
			"Archive records of a Reset replay not appended because they are older than anything the hub's capped archive still holds for that AP (the cap already dropped them), by table and source AP.", "table", "schedd"),
		TooDeep: cv("too_deep_total",
			"Archive records not appended because a value in them nests deeper than the hub can store, by table and source AP. The refusal is permanent, so the record is skipped rather than failing the session.", "table", "schedd"),
		RejectedSpokes: cv("rejected_spokes_total",
			"Spoke ads not paired with a schedd, by reason (host_mismatch, ha_tie); counted per discovery pass.", "reason"),
		Retired: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "retired_total",
			Help: "Sources retired (rows deleted from the hub's mutable tables)."}),
		SourceStaleness: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "source_staleness_seconds",
			Help: "Upper bound on how far the hub's copy of each AP is behind its schedd. Absent while unknown."}, []string{"schedd"}),
		SourcesByState: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "sources",
			Help: "Federated sources by state."}, []string{"state"}),
		StreamConnected: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "stream_connected",
			Help: "1 while a watch session is open for this table of this AP, 0 otherwise."}, []string{"table", "schedd"}),
		StreamSynced: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "stream_synced",
			Help: "1 while this table's stream has finished its catch-up and is live, 0 while it is catching up or not connected. A stream stuck at 0 is replicating nothing durably, however many records it applies, because an archive commits its cursor only once catch-up completes; source_staleness_seconds cannot see this, and neither can the source's state, which is measured on the mutable streams alone."}, []string{"table", "schedd"}),
		stalenessSchedds: map[string]bool{},
		streams:          map[streamKey]bool{},
	}
}

// Describe implements prometheus.Collector.
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	for _, c := range m.collectors() {
		c.Describe(ch)
	}
}

// Collect implements prometheus.Collector.
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	for _, c := range m.collectors() {
		c.Collect(ch)
	}
}

func (m *Metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.EventsApplied, m.IdenticalSkips, m.Resets, m.DedupHits, m.MissingIdentity,
		m.PhantomDeletes, m.Undecodable, m.BelowRetention, m.TooDeep, m.RejectedSpokes, m.Retired, m.SourceStaleness, m.SourcesByState,
		m.StreamConnected, m.StreamSynced}
}

// setStaleness publishes the per-source staleness gauges, dropping sources no longer present or
// whose staleness is unknown, so a retired AP does not leave a frozen series behind.
func (m *Metrics) setStaleness(known map[string]float64) {
	m.stalenessMu.Lock()
	defer m.stalenessMu.Unlock()
	for s := range m.stalenessSchedds {
		if _, ok := known[s]; !ok {
			m.SourceStaleness.DeleteLabelValues(s)
			delete(m.stalenessSchedds, s)
		}
	}
	for s, v := range known {
		m.SourceStaleness.WithLabelValues(s).Set(v)
		m.stalenessSchedds[s] = true
	}
}

// setStreams publishes the per-stream connected/synced gauges, dropping the streams of a source
// that is gone so a retired AP leaves no frozen series behind (as setStaleness does).
func (m *Metrics) setStreams(now map[streamKey]cedarsyncStatus) {
	m.streamsMu.Lock()
	defer m.streamsMu.Unlock()
	for k := range m.streams {
		if _, ok := now[k]; !ok {
			m.StreamConnected.DeleteLabelValues(k.table, k.schedd)
			m.StreamSynced.DeleteLabelValues(k.table, k.schedd)
			delete(m.streams, k)
		}
	}
	for k, st := range now {
		m.StreamConnected.WithLabelValues(k.table, k.schedd).Set(b2f(st.connected))
		m.StreamSynced.WithLabelValues(k.table, k.schedd).Set(b2f(st.synced))
		m.streams[k] = true
	}
}

// cedarsyncStatus is the part of a stream's status the gauges carry, so metrics.go does not
// depend on the runner type.
type cedarsyncStatus struct{ connected, synced bool }

func b2f(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
