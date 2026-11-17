package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// BackupStats is what the metrics endpoint needs to know about the daemon's snapshots. It mirrors
// server.BackupStats; the caller adapts, so this package does not import the server.
type BackupStats struct {
	Total        int64
	Failures     int64
	LastSuccess  time.Time
	LastFailure  time.Time
	LastBytes    int64
	LastDuration time.Duration
}

// backupCollector exports the daemon's backup history.
//
// The series that matters operationally is last_success_timestamp_seconds: a backup regime fails
// silently -- the snapshot that does not happen logs a warning nobody reads and leaves no artifact
// -- so the alert worth having is on the AGE of the last success, not on a failure counter. Alert
// on time() - htcondordb_backup_last_success_timestamp_seconds exceeding a couple of intervals.
// A zero value means no snapshot has succeeded since this daemon started, which is also the value
// a never-configured backup regime reports; absence of the series means the endpoint is older than
// this metric.
type backupCollector struct {
	stats func() BackupStats

	total       *prometheus.Desc
	failures    *prometheus.Desc
	lastSuccess *prometheus.Desc
	lastFailure *prometheus.Desc
	lastBytes   *prometheus.Desc
	lastSeconds *prometheus.Desc
}

// NewBackupCollector builds the backup collector. Pass it to Handler as an extra collector; the
// caller adapts server.BackupStats into BackupStats so this package does not import the server.
func NewBackupCollector(stats func() BackupStats) prometheus.Collector {
	return newBackupCollector(stats)
}

func newBackupCollector(stats func() BackupStats) *backupCollector {
	return &backupCollector{
		stats: stats,
		total: prometheus.NewDesc(namespace+"_backup_attempts_total",
			"Snapshots attempted since daemon start (periodic and on-demand).", nil, nil),
		failures: prometheus.NewDesc(namespace+"_backup_failures_total",
			"Snapshots that returned an error since daemon start.", nil, nil),
		lastSuccess: prometheus.NewDesc(namespace+"_backup_last_success_timestamp_seconds",
			"Unix time of the last successful snapshot; 0 if none since start. Alert on its age, not on the failure counter.", nil, nil),
		lastFailure: prometheus.NewDesc(namespace+"_backup_last_failure_timestamp_seconds",
			"Unix time of the last failed snapshot; 0 if none since start.", nil, nil),
		lastBytes: prometheus.NewDesc(namespace+"_backup_last_bytes",
			"Size in bytes of the last successful snapshot. A sharp drop can mean tables went missing.", nil, nil),
		lastSeconds: prometheus.NewDesc(namespace+"_backup_last_duration_seconds",
			"Wall-clock seconds the last successful snapshot took.", nil, nil),
	}
}

func (c *backupCollector) Describe(chan<- *prometheus.Desc) {}

func (c *backupCollector) Collect(ch chan<- prometheus.Metric) {
	if c.stats == nil {
		return
	}
	s := c.stats()
	// Unix() on the zero time is a large negative number; report 0 for "never" so a dashboard
	// shows an obviously-absent value rather than a date in year 1.
	stamp := func(t time.Time) float64 {
		if t.IsZero() {
			return 0
		}
		return float64(t.Unix())
	}
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.CounterValue, float64(s.Total))
	ch <- prometheus.MustNewConstMetric(c.failures, prometheus.CounterValue, float64(s.Failures))
	ch <- prometheus.MustNewConstMetric(c.lastSuccess, prometheus.GaugeValue, stamp(s.LastSuccess))
	ch <- prometheus.MustNewConstMetric(c.lastFailure, prometheus.GaugeValue, stamp(s.LastFailure))
	ch <- prometheus.MustNewConstMetric(c.lastBytes, prometheus.GaugeValue, float64(s.LastBytes))
	ch <- prometheus.MustNewConstMetric(c.lastSeconds, prometheus.GaugeValue, s.LastDuration.Seconds())
}
