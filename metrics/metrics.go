// Package metrics exposes an htcondordb catalog's per-table storage footprint and
// operational timing counters as Prometheus metrics, so an operator can watch the
// database's health -- and, crucially, see which part of the store is "blocking the
// world" -- without attaching a profiler. Metrics are computed live on each scrape
// from the catalog, so they never go stale.
package metrics

import (
	"fmt"
	"github.com/PelicanPlatform/classad/collections"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/htcondordb/dbad"
)

const namespace = "htcondordb"

// Prometheus column-alias prefixes for materialized views: a group column aliased
// label_<x> becomes a Prometheus label <x>; a metric column aliased metric_<y> becomes a
// sample of the metric <view>_<y>. Columns without either prefix are not exported.
const (
	viewLabelPrefix  = "label_"
	viewMetricPrefix = "metric_"
)

// catalogCollector implements prometheus.Collector over a db.Catalog, emitting
// per-table storage gauges and operational timing counters. Reading on Collect
// (rather than caching) keeps the numbers exact and lock-scoped to the scrape.
type catalogCollector struct {
	cat *db.Catalog

	// storage gauges (label: table)
	ads      *prometheus.Desc
	arena    *prometheus.Desc
	used     *prometheus.Desc
	live     *prometheus.Desc
	dead     *prometheus.Desc
	segments *prometheus.Desc

	// operational timing counters (labels: table, op) -- a {seconds_total, ops_total}
	// pair per stall point, so a scraper derives rate() and mean latency (seconds/ops).
	opSeconds *prometheus.Desc
	opOps     *prometheus.Desc
	opMax     *prometheus.Desc

	// Archive-only backlog gauges. An append-only table never compacts, so its segment
	// count only grows, and every sealed segment costs an mmap at open (plus a second for
	// its sidecar). Running out of mappings is a fail-to-START, so segment count is the
	// signal worth alerting on -- and it is invisible without these, because an archive is
	// not one of cat.Tables().
	staleIndex *prometheus.Desc
	mappings   *prometheus.Desc
}

func newCatalogCollector(cat *db.Catalog) *catalogCollector {
	tbl := []string{"table"}
	tblOp := []string{"table", "op"}
	return &catalogCollector{
		cat: cat,
		ads: prometheus.NewDesc(namespace+"_ads",
			"Number of live ads held, by table.", tbl, nil),
		arena: prometheus.NewDesc(namespace+"_arena_bytes",
			"Compressed arena bytes reserved for record storage (the dominant resident footprint), by table.", tbl, nil),
		used: prometheus.NewDesc(namespace+"_used_bytes",
			"Compressed bytes written into segments (live plus reclaimable dead), by table.", tbl, nil),
		live: prometheus.NewDesc(namespace+"_live_bytes",
			"Compressed bytes of live records, by table.", tbl, nil),
		dead: prometheus.NewDesc(namespace+"_dead_bytes",
			"Compressed bytes of superseded records reclaimable by compaction, by table.", tbl, nil),
		segments: prometheus.NewDesc(namespace+"_segments",
			"Number of arena segments, by table.", tbl, nil),
		opSeconds: prometheus.NewDesc(namespace+"_op_seconds_total",
			"Cumulative wall time spent in each store stall point (shard write lock wait/hold, segment allocation, durability sync, compaction/retrain/reindex, snapshot lock), by table and op.", tblOp, nil),
		opOps: prometheus.NewDesc(namespace+"_op_ops_total",
			"Cumulative number of times each store stall point ran, by table and op. Divide op_seconds_total by this for mean latency.", tblOp, nil),
		staleIndex: prometheus.NewDesc(namespace+"_stale_index_segments",
			"Sealed segments whose index was built under a superseded configuration and awaits rebuild, by table. Normally zero; a persistent non-zero value means a reindex is failing or never runs.", tbl, nil),
		mappings: prometheus.NewDesc(namespace+"_estimated_mmaps",
			"Estimated memory mappings a table holds: one per sealed segment plus one per sidecar. Compare against vm.max_map_count (65530 by default, shared process-wide across every table) -- exhausting it prevents the daemon from STARTING, not merely from querying.", tbl, nil),
		opMax: prometheus.NewDesc(namespace+"_op_max_seconds",
			"Longest single occurrence of each store stall point (worst-case latency since start), by table and op. The mean (op_seconds_total/op_ops_total) hides tail stalls; this surfaces them.", tblOp, nil),
	}
}

func (c *catalogCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ads
	ch <- c.arena
	ch <- c.used
	ch <- c.live
	ch <- c.dead
	ch <- c.segments
	ch <- c.opSeconds
	ch <- c.opOps
	ch <- c.opMax
	ch <- c.staleIndex
	ch <- c.mappings
}

func (c *catalogCollector) Collect(ch chan<- prometheus.Metric) {
	for _, name := range c.cat.Tables() {
		t, ok := c.cat.Table(name)
		if !ok {
			continue
		}
		st := t.Stats()
		gauge := func(d *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, name)
		}
		gauge(c.ads, float64(st.Ads))
		gauge(c.arena, float64(st.ArenaBytes))
		gauge(c.used, float64(st.UsedBytes))
		gauge(c.live, float64(st.LiveBytes()))
		gauge(c.dead, float64(st.DeadBytes))
		gauge(c.segments, float64(st.Segments))

		c.collectOpStats(ch, name, t.OpStats())
	}

	// Archive (history) tables are a separate namespace in the catalog, so iterating
	// cat.Tables() alone silently omits them -- and they are exactly where segment count
	// matters, since an append-only table never compacts and its segments only accumulate.
	for _, name := range c.cat.ArchiveTables() {
		a, ok := c.cat.ArchiveTable(name)
		if !ok {
			continue
		}
		st := a.Stats()
		gauge := func(d *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, name)
		}
		gauge(c.ads, float64(st.Ads))
		gauge(c.arena, float64(st.ArenaBytes))
		gauge(c.used, float64(st.UsedBytes))
		gauge(c.live, float64(st.LiveBytes()))
		gauge(c.dead, float64(st.DeadBytes))
		gauge(c.segments, float64(st.Segments))

		stale, sealed := a.StaleIndexSegments()
		gauge(c.staleIndex, float64(stale))
		// One mapping per sealed segment, plus one per sidecar once a query touches it.
		// Reported as the ceiling case, since that is the number that runs out.
		gauge(c.mappings, float64(sealed*2))

		c.collectOpStats(ch, name, a.OpStats())
	}
}

// collectOpStats emits the {seconds_total, ops_total, max_seconds} triple per stall point,
// shared by mutable and archive tables so both report timings on the same terms.
func (c *catalogCollector) collectOpStats(ch chan<- prometheus.Metric, name string, o db.OpStats) {
	for _, e := range opStatList(o) {
		ch <- prometheus.MustNewConstMetric(c.opOps, prometheus.CounterValue, float64(e.stat.Count), name, e.op)
		ch <- prometheus.MustNewConstMetric(c.opSeconds, prometheus.CounterValue, float64(e.stat.Nanos)/1e9, name, e.op)
		ch <- prometheus.MustNewConstMetric(c.opMax, prometheus.GaugeValue, float64(e.stat.MaxNanos)/1e9, name, e.op)
	}
}

// opStatList flattens a db.OpStats into (op-name, counter) pairs for the op= label.
func opStatList(o db.OpStats) []struct {
	op   string
	stat db.OpStat
} {
	return []struct {
		op   string
		stat db.OpStat
	}{
		{"shard_write_wait", o.ShardWriteWait},
		{"shard_write_hold", o.ShardWriteHold},
		{"segment_alloc", o.SegmentAlloc},
		{"sync", o.Sync},
		{"commit_sync", o.CommitSync},
		{"compact", o.Compact},
		{"retrain", o.Retrain},
		{"reindex", o.Reindex},
		{"snapshot_lock", o.SnapshotLock},
	}
}

// viewCollector implements prometheus.Collector over a catalog's materialized views. Each
// view exports one sample per group: its label_* columns become Prometheus labels and its
// metric_* columns become gauges named <view>_<suffix>. Views (and their label sets) are
// dynamic, so Descs are built per scrape rather than up front, and this behaves as an
// unchecked collector (Describe emits nothing).
type viewCollector struct {
	cat *db.Catalog
}

func (c *viewCollector) Describe(chan<- *prometheus.Desc) {}

func (c *viewCollector) Collect(ch chan<- prometheus.Metric) {
	for _, name := range c.cat.Views() {
		backing, ok := c.cat.ViewBacking(name)
		if !ok {
			continue // a stale/failed view has no backing to scrape
		}
		v, ok := c.cat.View(name)
		if !ok {
			continue
		}
		spec := v.Spec()
		// A continuous aggregate is a time series recorded to an archive, not a
		// current-state gauge. Scraping its per-bucket rows would emit many samples of the
		// same metric with no distinguishing label (the time bucket is not a label_ column),
		// which the Prometheus registry rejects as duplicate series -- failing the whole
		// scrape. Skip it; it is read as a time series via SQL instead.
		if spec.IsContinuous() {
			continue
		}

		// Label columns: group columns whose alias carries the label_ prefix.
		type labelCol struct{ attr, key string }
		var labels []labelCol
		for _, g := range spec.Groups {
			if key, ok := strings.CutPrefix(g.Alias, viewLabelPrefix); ok {
				labels = append(labels, labelCol{attr: g.Alias, key: key})
			}
		}
		labelNames := make([]string, len(labels))
		for i, l := range labels {
			labelNames[i] = l.key
		}

		// Metric columns: aggregate columns whose alias carries the metric_ prefix.
		type metricCol struct {
			attr string
			desc *prometheus.Desc
		}
		var metricCols []metricCol
		for _, m := range spec.Metrics {
			if suffix, ok := strings.CutPrefix(m.Alias, viewMetricPrefix); ok {
				desc := prometheus.NewDesc(name+"_"+suffix,
					fmt.Sprintf("Materialized view %q metric %q.", name, suffix), labelNames, nil)
				metricCols = append(metricCols, metricCol{attr: m.Alias, desc: desc})
			}
		}
		if len(metricCols) == 0 {
			continue
		}

		backing.ForEach(func(ad *classad.ClassAd) bool {
			labelValues := make([]string, len(labels))
			for i, l := range labels {
				s, ok := ad.EvaluateAttrString(l.attr)
				if !ok {
					s = ad.EvaluateAttr(l.attr).String()
				}
				labelValues[i] = s
			}
			for _, mc := range metricCols {
				val, ok := ad.EvaluateAttrNumber(mc.attr)
				if !ok {
					continue
				}
				// NewConstMetric (not Must*) so an operator's invalid alias skips the
				// sample instead of aborting the whole scrape.
				if metric, err := prometheus.NewConstMetric(mc.desc, prometheus.GaugeValue, val, labelValues...); err == nil {
					ch <- metric
				}
			}
			return true
		})
	}
}

// syncCollector emits schedd-sync tailer health (label: kind, source) and the daemon-managed
// change-data exporter health (label: exporter, kind). Both are the "is anything falling behind"
// signals an operator alerts on -- lag_bytes climbing or an exporter's last_beat going stale.
// The source funcs are queried per scrape (an unchecked collector: Describe emits nothing) so a
// set that changes at runtime -- tailers restarted on reconfigure, exporters added via dbrpc --
// is always current.
type syncCollector struct {
	sources   func() []dbad.StatusSource
	exporters func() []dbad.ExporterStatus
	importers func() []dbad.ImporterStatus

	syncLag      *prometheus.Desc
	syncCaughtUp *prometheus.Desc
	syncFileSize *prometheus.Desc
	syncOffset   *prometheus.Desc
	syncLastTime *prometheus.Desc
	syncResyncs  *prometheus.Desc
	expUp        *prometheus.Desc
	expRestarts  *prometheus.Desc
	expIndexed   *prometheus.Desc
	expSkipped   *prometheus.Desc
	expInFlight  *prometheus.Desc
	expLastBeat  *prometheus.Desc
	impUp        *prometheus.Desc
	impRestarts  *prometheus.Desc
	impImported  *prometheus.Desc
	impSchedds   *prometheus.Desc
	impFailures  *prometheus.Desc
	impLastBeat  *prometheus.Desc
	impLastCycle *prometheus.Desc
}

func newSyncCollector(sources func() []dbad.StatusSource, exporters func() []dbad.ExporterStatus, importers func() []dbad.ImporterStatus) *syncCollector {
	sync := []string{"kind", "source"}
	exp := []string{"exporter", "kind"}
	imp := []string{"import_job"}
	return &syncCollector{
		sources:   sources,
		exporters: exporters,
		importers: importers,
		syncLag: prometheus.NewDesc(namespace+"_sync_lag_bytes",
			"Bytes the schedd-sync tailer is behind the source file (live: source size minus committed offset), by kind and source. A climbing value means the sync is falling behind.", sync, nil),
		syncCaughtUp: prometheus.NewDesc(namespace+"_sync_caught_up",
			"1 if the schedd-sync tailer has consumed the source file to EOF, else 0, by kind and source.", sync, nil),
		syncFileSize: prometheus.NewDesc(namespace+"_sync_file_bytes",
			"Current size of the source file the schedd-sync tailer is following, by kind and source.", sync, nil),
		syncOffset: prometheus.NewDesc(namespace+"_sync_offset_bytes",
			"Committed byte offset the schedd-sync tailer has durably consumed, by kind and source.", sync, nil),
		syncLastTime: prometheus.NewDesc(namespace+"_sync_last_timestamp_seconds",
			"Unix time of the schedd-sync tailer's last successful sync, by kind and source. Staleness (now minus this) flags a wedged tailer.", sync, nil),
		syncResyncs: prometheus.NewDesc(namespace+"_sync_resyncs_total",
			"Number of full resyncs the history tailer has performed (a gap/rotation was detected), by kind and source.", sync, nil),
		expUp: prometheus.NewDesc(namespace+"_exporter_up",
			"1 if the daemon-managed change-data exporter process is running, else 0, by exporter and kind.", exp, nil),
		expRestarts: prometheus.NewDesc(namespace+"_exporter_restarts_total",
			"Number of times the daemon has restarted this exporter (crash or wedged-liveness), by exporter and kind.", exp, nil),
		expIndexed: prometheus.NewDesc(namespace+"_exporter_docs_indexed_total",
			"Cumulative documents/records the exporter has delivered downstream, by exporter and kind.", exp, nil),
		expSkipped: prometheus.NewDesc(namespace+"_exporter_docs_skipped_total",
			"Cumulative documents/records the exporter dropped (untransformable), by exporter and kind.", exp, nil),
		expInFlight: prometheus.NewDesc(namespace+"_exporter_in_flight",
			"Documents/records the exporter has sent but not yet had acknowledged downstream, by exporter and kind.", exp, nil),
		expLastBeat: prometheus.NewDesc(namespace+"_exporter_last_beat_timestamp_seconds",
			"Unix time of the exporter's last liveness/progress beat, by exporter and kind. Staleness (now minus this) is what the daemon's liveness monitor restarts on.", exp, nil),
		impUp: prometheus.NewDesc(namespace+"_import_job_up",
			"1 if the daemon-managed history-import runner process is running, else 0, by import_job.", imp, nil),
		impRestarts: prometheus.NewDesc(namespace+"_import_job_restarts_total",
			"Number of times the daemon has restarted this history-import runner (crash or wedged-liveness), by import_job.", imp, nil),
		impImported: prometheus.NewDesc(namespace+"_import_job_imported_total",
			"Cumulative history records this runner has imported since it started, by import_job.", imp, nil),
		impSchedds: prometheus.NewDesc(namespace+"_import_job_last_cycle_schedds",
			"Schedds imported in the runner's last cycle, by import_job.", imp, nil),
		impFailures: prometheus.NewDesc(namespace+"_import_job_last_cycle_failures",
			"Schedds that errored in the runner's last cycle, by import_job. A persistently non-zero value flags an unreachable schedd.", imp, nil),
		impLastBeat: prometheus.NewDesc(namespace+"_import_job_last_beat_timestamp_seconds",
			"Unix time of the runner's last liveness beat, by import_job. Staleness (now minus this) is what the daemon's liveness monitor restarts on.", imp, nil),
		impLastCycle: prometheus.NewDesc(namespace+"_import_job_last_cycle_timestamp_seconds",
			"Unix time of the runner's last completed import cycle, by import_job.", imp, nil),
	}
}

func (c *syncCollector) Describe(chan<- *prometheus.Desc) {}

func (c *syncCollector) Collect(ch chan<- prometheus.Metric) {
	if c.sources != nil {
		for _, s := range dbad.LiveStatuses(c.sources) {
			g := func(d *prometheus.Desc, v float64) {
				ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, s.Kind, s.Source)
			}
			g(c.syncLag, float64(s.LagBytes))
			g(c.syncCaughtUp, b2f(s.CaughtUp))
			g(c.syncFileSize, float64(s.FileSize))
			g(c.syncOffset, float64(s.Offset))
			if !s.LastSync.IsZero() {
				g(c.syncLastTime, float64(s.LastSync.Unix()))
			}
			ch <- prometheus.MustNewConstMetric(c.syncResyncs, prometheus.CounterValue, float64(s.Resyncs), s.Kind, s.Source)
		}
	}
	if c.exporters != nil {
		for _, e := range c.exporters() {
			g := func(d *prometheus.Desc, v float64) {
				ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, e.Name, e.Kind)
			}
			g(c.expUp, b2f(e.Running))
			ch <- prometheus.MustNewConstMetric(c.expRestarts, prometheus.CounterValue, float64(e.Restarts), e.Name, e.Kind)
			ch <- prometheus.MustNewConstMetric(c.expIndexed, prometheus.CounterValue, float64(e.DocsIndexed), e.Name, e.Kind)
			ch <- prometheus.MustNewConstMetric(c.expSkipped, prometheus.CounterValue, float64(e.DocsSkipped), e.Name, e.Kind)
			g(c.expInFlight, float64(e.InFlight))
			if !e.LastBeat.IsZero() {
				g(c.expLastBeat, float64(e.LastBeat.Unix()))
			}
		}
	}
	if c.importers != nil {
		for _, im := range c.importers() {
			g := func(d *prometheus.Desc, v float64) {
				ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, im.Name)
			}
			g(c.impUp, b2f(im.Running))
			ch <- prometheus.MustNewConstMetric(c.impRestarts, prometheus.CounterValue, float64(im.Restarts), im.Name)
			ch <- prometheus.MustNewConstMetric(c.impImported, prometheus.CounterValue, float64(im.ImportedTotal), im.Name)
			g(c.impSchedds, float64(im.Schedds))
			g(c.impFailures, float64(im.Failures))
			if !im.LastBeat.IsZero() {
				g(c.impLastBeat, float64(im.LastBeat.Unix()))
			}
			if !im.LastCycle.IsZero() {
				g(c.impLastCycle, float64(im.LastCycle.Unix()))
			}
		}
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// Handler returns an http.Handler serving Prometheus metrics for the catalog: the
// per-table storage gauges and operational timing counters above, the materialized-view
// gauges, the schedd-sync + exporter health gauges, plus the standard Go runtime and process
// (RSS, open FDs, ...) collectors. sources, exporters, and importers may be nil (their metric
// families are then simply absent). It uses a private registry so it can be mounted without
// global-registry collisions.
func Handler(cat *db.Catalog, sources func() []dbad.StatusSource, exporters func() []dbad.ExporterStatus, importers func() []dbad.ImporterStatus) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		// Store integrity, deliberately process-wide rather than per table: it counts bucket-chain links that
		// named a segment the shard does not have, which is a segment-LIFETIME bug -- a reader walking a
		// mapping it did not hold alive -- and not a property of any one table's data.
		//
		// Zero on a healthy store. Nonzero means a chain walk terminated early instead of panicking, so a
		// lookup may have missed a key that exists: a wrong answer rather than a crash. Alert on any increase,
		// not on a threshold.
		//
		// A counter by nature, exported as a gauge because the value is read from the library on scrape rather
		// than accumulated here; _total keeps the name honest about its monotonicity.
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "htcondordb_store_corrupt_chain_links_total",
			Help: "Bucket-chain links naming a segment the shard does not have. Nonzero indicates a segment-lifetime bug; a lookup may have missed an existing key.",
		}, func() float64 { return float64(collections.CorruptChainLinks()) }),
		newCatalogCollector(cat),
		&viewCollector{cat: cat},
		newSyncCollector(sources, exporters, importers),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
