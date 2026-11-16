package federate

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// seriesOf returns every series of vec as label-pair -> value, so a test can tell a series set to
// zero from one that is not published at all. The difference matters here: a stream that has
// never synced must read 0, not be missing, because an absent series alerts on nothing.
func seriesOf(vec *prometheus.GaugeVec) map[[2]string]float64 {
	ch := make(chan prometheus.Metric)
	go func() { vec.Collect(ch); close(ch) }()
	out := map[[2]string]float64{}
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			panic(err)
		}
		var k [2]string
		for _, p := range d.Label {
			switch p.GetName() {
			case "table":
				k[0] = p.GetValue()
			case "schedd":
				k[1] = p.GetValue()
			}
		}
		out[k] = d.Gauge.GetValue()
	}
	return out
}

// TestStreamSyncedIsPublishedPerStream pins what the gauge is for: an archive stream that
// connects but never finishes its catch-up replicates nothing durably, and nothing else reports
// it -- the source's own state is decided by the mutable streams alone, so an AP whose history
// has been catching up for days still reads fresh.
func TestStreamSyncedIsPublishedPerStream(t *testing.T) {
	m := NewMetrics()
	const ap = "ap2001.example"
	m.setStreams(map[streamKey]cedarsyncStatus{
		{TableJobs, ap}:    {connected: true, synced: true},
		{TableHistory, ap}: {connected: true, synced: false}, // connected for days, never caught up
	})

	synced := seriesOf(m.StreamSynced)
	if got, ok := synced[[2]string{TableHistory, ap}]; !ok || got != 0 {
		t.Errorf("history stream_synced = %v (present=%v), want a published 0: a stream that has "+
			"never caught up has to read zero, not go missing", got, ok)
	}
	if got := synced[[2]string{TableJobs, ap}]; got != 1 {
		t.Errorf("jobs stream_synced = %v, want 1", got)
	}
	if got, ok := seriesOf(m.StreamConnected)[[2]string{TableHistory, ap}]; !ok || got != 1 {
		t.Errorf("history stream_connected = %v (present=%v), want 1: the stream is connected, "+
			"which is exactly why its silence looks healthy", got, ok)
	}
}

// TestStreamGaugesDropRetiredSources keeps a retired AP from leaving a frozen series behind,
// which would otherwise alert forever on a stream that no longer exists.
func TestStreamGaugesDropRetiredSources(t *testing.T) {
	m := NewMetrics()
	const gone, kept = "retired.example", "live.example"
	m.setStreams(map[streamKey]cedarsyncStatus{
		{TableHistory, gone}: {connected: true, synced: false},
		{TableHistory, kept}: {connected: true, synced: true},
	})
	m.setStreams(map[streamKey]cedarsyncStatus{
		{TableHistory, kept}: {connected: true, synced: true},
	})

	for _, vec := range []*prometheus.GaugeVec{m.StreamSynced, m.StreamConnected} {
		got := seriesOf(vec)
		if _, ok := got[[2]string{TableHistory, gone}]; ok {
			t.Errorf("a retired AP kept its series: %v", got)
		}
		if _, ok := got[[2]string{TableHistory, kept}]; !ok {
			t.Errorf("a live AP lost its series: %v", got)
		}
	}
}
