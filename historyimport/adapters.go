package historyimport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"

	htcondor "github.com/bbockelm/golang-htcondor"
)

// CollectorDiscovery implements Discovery by querying a pool's collector for its
// Schedd ads. It authenticates ambiently from the process's HTCondor config, like
// every other pool client here.
type CollectorDiscovery struct{}

// Schedds returns the schedds advertising to pool that satisfy constraint.
func (CollectorDiscovery) Schedds(ctx context.Context, pool, constraint string) ([]ScheddRef, error) {
	coll := htcondor.NewCollector(pool)
	ads, _, err := coll.QueryAdsWithOptions(ctx, "Schedd", constraint, &htcondor.QueryOptions{
		Projection: []string{"Name", "MyAddress", "ScheddIpAddr"},
	})
	if err != nil {
		return nil, err
	}
	out := make([]ScheddRef, 0, len(ads))
	for _, ad := range ads {
		name, _ := ad.EvaluateAttrString("Name")
		addr, ok := ad.EvaluateAttrString("MyAddress")
		if !ok || addr == "" {
			addr, _ = ad.EvaluateAttrString("ScheddIpAddr")
		}
		if name == "" || addr == "" {
			continue // an ad we cannot contact is not actionable
		}
		out = append(out, ScheddRef{Name: name, Address: addr})
	}
	return out, nil
}

// ScheddHistorySource implements Source via a remote condor_history stream
// (QUERY_SCHEDD_HISTORY). A yield error cancels the stream so the schedd stops
// sending as soon as the importer has what it needs (e.g. reached archived
// records).
type ScheddHistorySource struct {
	// BufferSize / WriteTimeout tune the underlying stream; zero uses defaults.
	BufferSize   int
	WriteTimeout time.Duration
}

// History streams sd's completed-job history newest-first, stopping the backward
// scan at since, filtered by constraint, capped at limit.
func (s ScheddHistorySource) History(ctx context.Context, sd ScheddRef, constraint, since string, limit int, yield func(*classad.ClassAd) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sc := htcondor.NewSchedd(sd.Name, sd.Address)
	opts := &htcondor.HistoryQueryOptions{
		Source:    htcondor.HistorySourceJobHistory,
		Backwards: true, // newest first, so `since` and recovery dedup can stop early
		Since:     since,
		Limit:     limit, // 0 = unlimited
	}
	streamOpts := &htcondor.StreamOptions{BufferSize: s.BufferSize, WriteTimeout: s.WriteTimeout}

	ch, err := sc.QueryHistoryStream(ctx, constraint, opts, streamOpts)
	if err != nil {
		return err
	}
	for r := range ch {
		if r.Err != nil {
			return r.Err
		}
		if err := yield(r.Ad); err != nil {
			cancel()
			for range ch { //nolint:revive // drain so the producer goroutine exits
			}
			return err
		}
	}
	return nil
}

// ArchiveWriter implements Writer against a live db.Catalog, appending to (and
// creating on first use) the target archive table. It is the in-process writer;
// an out-of-process importer would use a dbrpc-backed Writer instead.
type ArchiveWriter struct {
	Cat *db.Catalog

	mu sync.Mutex
}

// archiveConfig indexes the attributes the importer relies on: GlobalJobId for the
// recovery-dedup Has() lookup, ScheddName so per-schedd queries prune, and the
// import time as a zone attribute for retention pruning.
func archiveConfig() db.ArchiveConfig {
	return db.ArchiveConfig{
		CategoricalAttrs: []string{"GlobalJobId", ScheddNameAttr},
		ZoneAttrs:        []string{EnteredHistoryAttr},
	}
}

func (a *ArchiveWriter) table(name string) (*db.ArchiveTable, error) {
	if t, ok := a.Cat.ArchiveTable(name); ok {
		return t, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if t, ok := a.Cat.ArchiveTable(name); ok { // re-check under lock
		return t, nil
	}
	return a.Cat.CreateArchiveTable(name, archiveConfig())
}

// Append adds a record to the table.
func (a *ArchiveWriter) Append(_ context.Context, table string, ad *classad.ClassAd) error {
	t, err := a.table(table)
	if err != nil {
		return err
	}
	return t.Append(ad)
}

// Has reports whether the table already holds a record with this GlobalJobId.
func (a *ArchiveWriter) Has(_ context.Context, table, gid string) (bool, error) {
	t, ok := a.Cat.ArchiveTable(table)
	if !ok {
		return false, nil // table not created yet: nothing is present
	}
	seq, err := t.Query(fmt.Sprintf("GlobalJobId == %q", gid))
	if err != nil {
		return false, err
	}
	for range seq {
		return true, nil
	}
	return false, nil
}

// MapCursors is an in-memory Cursors (tests, and a base for a durable store).
type MapCursors struct {
	mu sync.Mutex
	m  map[string]string
}

// NewMapCursors returns an empty in-memory cursor store.
func NewMapCursors() *MapCursors { return &MapCursors{m: map[string]string{}} }

func cursorKey(job, schedd string) string { return job + "\x00" + schedd }

// Get returns the stored cursor for (job, schedd).
func (c *MapCursors) Get(job, schedd string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[cursorKey(job, schedd)]
	return v, ok
}

// Set stores the cursor for (job, schedd).
func (c *MapCursors) Set(job, schedd, cur string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[cursorKey(job, schedd)] = cur
	return nil
}
