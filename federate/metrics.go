package federate

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are a hub's counters and gauges. They are Prometheus collectors (mounted on the daemon's
// /metrics alongside the store's own) and are also read directly by tests, which count writes,
// dedup hits and phantom deletes rather than only inspecting final state.
type Metrics struct {
	EventsApplied    *prometheus.CounterVec // labels: table, kind (upsert|delete)
	IdenticalSkips   *prometheus.CounterVec // labels: table -- catch-up upserts equal to the stored row
	Resets           *prometheus.CounterVec // labels: table
	DedupHits        *prometheus.CounterVec // labels: table -- archive records already present
	MissingIdentity  *prometheus.CounterVec // labels: table -- archive records with no dedup identity
	PhantomDeletes   *prometheus.CounterVec // labels: table -- rows a Reset replay did not re-send
	RejectedSpokes   *prometheus.CounterVec // labels: reason
	Retired          prometheus.Counter
	SourceStaleness  *prometheus.GaugeVec // labels: schedd
	SourcesByState   *prometheus.GaugeVec // labels: state
	stalenessMu      sync.Mutex
	stalenessSchedds map[string]bool
}

// NewMetrics builds an unregistered metric set.
func NewMetrics() *Metrics {
	const ns, sub = "htcondordb", "federate"
	cv := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: name, Help: help}, labels)
	}
	return &Metrics{
		EventsApplied: cv("events_applied_total",
			"Replicated changes written to the hub, by table and kind (upsert or delete).", "table", "kind"),
		IdenticalSkips: cv("identical_skips_total",
			"Catch-up upserts not written because the hub already held an identical row, by table.", "table"),
		Resets: cv("resets_total",
			"Full replays (Reset) received from spokes, by table. Every spoke restart causes one per table.", "table"),
		DedupHits: cv("dedup_hits_total",
			"Archive records dropped during catch-up because the hub already held one with the same identity, by table.", "table"),
		MissingIdentity: cv("missing_identity_total",
			"Archive records appended without a dedup check because they lack GlobalJobId (or, for epochs, RunInstanceID), by table.", "table"),
		PhantomDeletes: cv("phantom_deletes_total",
			"Hub rows deleted at the end of a Reset replay because the spoke no longer has them, by table.", "table"),
		RejectedSpokes: cv("rejected_spokes_total",
			"Spoke ads not paired with a schedd, by reason (host_mismatch, ha_tie, no_name).", "reason"),
		Retired: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "retired_total",
			Help: "Sources retired (rows deleted from the hub's mutable tables)."}),
		SourceStaleness: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "source_staleness_seconds",
			Help: "Upper bound on how far the hub's copy of each AP is behind its schedd. Absent while unknown."}, []string{"schedd"}),
		SourcesByState: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "sources",
			Help: "Federated sources by state."}, []string{"state"}),
		stalenessSchedds: map[string]bool{},
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
		m.PhantomDeletes, m.RejectedSpokes, m.Retired, m.SourceStaleness, m.SourcesByState}
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
