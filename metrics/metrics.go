// Package metrics exposes an htcondordb catalog's per-table storage footprint and
// operational timing counters as Prometheus metrics, so an operator can watch the
// database's health -- and, crucially, see which part of the store is "blocking the
// world" -- without attaching a profiler. Metrics are computed live on each scrape
// from the catalog, so they never go stale.
package metrics

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
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

		for _, e := range opStatList(t.OpStats()) {
			ch <- prometheus.MustNewConstMetric(c.opOps, prometheus.CounterValue, float64(e.stat.Count), name, e.op)
			ch <- prometheus.MustNewConstMetric(c.opSeconds, prometheus.CounterValue, float64(e.stat.Nanos)/1e9, name, e.op)
		}
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

// Handler returns an http.Handler serving Prometheus metrics for the catalog: the
// per-table storage gauges and operational timing counters above, the materialized-view
// gauges, plus the standard Go runtime and process (RSS, open FDs, ...) collectors. It uses
// a private registry so it can be mounted without global-registry collisions.
func Handler(cat *db.Catalog) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		newCatalogCollector(cat),
		&viewCollector{cat: cat},
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
