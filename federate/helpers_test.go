package federate

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func openCatalog(t *testing.T, dir string) *db.Catalog {
	t.Helper()
	cat, err := db.OpenCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

func mustTable(t *testing.T, cat *db.Catalog, name string) *db.DB {
	t.Helper()
	d, err := cat.CreateTable(name)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func parseAd(t *testing.T, text string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.Parse("[" + text + "]")
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	return ad
}

// jobAd is a minimal proc ad as a spoke's jobs table holds it.
func jobAd(t *testing.T, cluster, proc int, extra string) *classad.ClassAd {
	t.Helper()
	text := fmt.Sprintf(`ClusterId = %d; ProcId = %d; Key = "%d.%d"; JobStatus = 1; Owner = "alice"`, cluster, proc, cluster, proc)
	if extra != "" {
		text += "; " + extra
	}
	return parseAd(t, text)
}

func upsert(key string, ad *classad.ClassAd) replicate.Change {
	return replicate.Change{Kind: replicate.KindUpsert, Key: key, Ad: ad}
}

func del(key string) replicate.Change { return replicate.Change{Kind: replicate.KindDelete, Key: key} }

func reset() replicate.Change { return replicate.Change{Kind: replicate.KindReset} }

func synced(cur string) replicate.Change {
	return replicate.Change{Kind: replicate.KindSynced, Cursor: []byte(cur)}
}

func apply(t *testing.T, s replicate.Sink, changes ...replicate.Change) {
	t.Helper()
	for _, c := range changes {
		if err := s.Apply(c); err != nil {
			t.Fatalf("apply %s %q: %v", c.Kind, c.Key, err)
		}
	}
}

func countWhere(t *testing.T, d *db.DB, constraint string) int {
	t.Helper()
	seq, err := d.Query(constraint)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for range seq {
		n++
	}
	return n
}

func countArchive(t *testing.T, a *db.ArchiveTable, constraint string) int {
	t.Helper()
	seq, err := a.Query(constraint)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for range seq {
		n++
	}
	return n
}

// val reads a counter or gauge. (Not prometheus/testutil: it would add a module dependency.)
func val(m prometheus.Metric) float64 {
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		panic(err)
	}
	if out.Counter != nil {
		return out.Counter.GetValue()
	}
	return out.Gauge.GetValue()
}

// writeCounter watches a hub table and counts the writes committed to it -- the honest measure
// of "a replay of an unchanged source writes nothing", which final state alone cannot show.
type writeCounter struct {
	d       *db.DB
	mu      sync.Mutex
	upserts int
	deletes int
	marks   map[string]bool
	nmark   int
	cond    *sync.Cond
	done    chan struct{}
}

const markPrefix = "zz-test-mark-"

func countWrites(t *testing.T, d *db.DB) *writeCounter {
	t.Helper()
	cur, err := d.WatchCursor()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	seq, err := d.Watch(ctx, cur)
	if err != nil {
		t.Fatal(err)
	}
	wc := &writeCounter{d: d, marks: map[string]bool{}, done: make(chan struct{})}
	wc.cond = sync.NewCond(&wc.mu)
	live := make(chan struct{})
	go func() {
		defer close(wc.done)
		synced := false
		for ev := range seq {
			if ev.Kind == db.WatchSynced && !synced {
				synced = true
				close(live)
			}
			wc.mu.Lock()
			switch {
			case strings.HasPrefix(ev.Key, markPrefix):
				wc.marks[ev.Key] = true
				wc.cond.Broadcast()
			case ev.Kind == db.WatchUpsert:
				wc.upserts++
			case ev.Kind == db.WatchDelete:
				wc.deletes++
			}
			wc.mu.Unlock()
		}
	}()
	t.Cleanup(func() { cancel(); <-wc.done })
	// Return only once the watch is registered and live. A watcher that registers while a delete
	// commits can see that delete twice or not at all (classad v0.31.0 collections: commitSeq
	// advances under the shard lock, but the delete reaches the delete journal only after the
	// unlock and sync, so catch-up and the live stream disagree about it).
	select {
	case <-live:
	case <-time.After(10 * time.Second):
		t.Fatal("write counter's watch never went live")
	}
	return wc
}

// settle commits a marker row and waits for the watch to deliver it, so every write committed
// before the call has been counted -- without sleeping for a guessed interval.
func (wc *writeCounter) settle(t *testing.T) (upserts, deletes int) {
	t.Helper()
	wc.mu.Lock()
	wc.nmark++
	key := fmt.Sprintf("%s%d", markPrefix, wc.nmark)
	wc.mu.Unlock()
	tx := wc.d.Begin()
	tx.NewClassAd(key, classad.New())
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	timer := time.AfterFunc(10*time.Second, func() { wc.mu.Lock(); wc.cond.Broadcast(); wc.mu.Unlock() })
	defer timer.Stop()
	deadline := time.Now().Add(10 * time.Second)
	wc.mu.Lock()
	defer wc.mu.Unlock()
	for !wc.marks[key] {
		if time.Now().After(deadline) {
			t.Fatal("write counter never saw its marker")
		}
		wc.cond.Wait()
	}
	return wc.upserts, wc.deletes
}

// tableTotal sums a per-table counter over every schedd it carries (and, for EventsApplied, over
// one kind). The counters are labelled by source AP as well as by table, and several tests drive
// two APs into one table: what they assert is what the TABLE saw, so they must not name one AP's
// series and miss the other's.
func tableTotal(vec *prometheus.CounterVec, table string, kind ...string) float64 {
	ch := make(chan prometheus.Metric)
	go func() { vec.Collect(ch); close(ch) }()
	total := 0.0
	for m := range ch {
		var out dto.Metric
		if err := m.Write(&out); err != nil {
			panic(err)
		}
		labels := map[string]string{}
		for _, p := range out.Label {
			labels[p.GetName()] = p.GetValue()
		}
		if labels["table"] != table {
			continue
		}
		if len(kind) > 0 && labels["kind"] != kind[0] {
			continue
		}
		total += out.Counter.GetValue()
	}
	return total
}
