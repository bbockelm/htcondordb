package flightsvc

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/grpc/metadata"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// cachedServer is a server over one in-process store, with the schema cache on and a clock a
// test can move. The dbrpc client is privileged, so this exercises the cache's lifetime; the
// privilege SCOPING is a different question and has its own test below.
func cachedServer(t *testing.T, ttl time.Duration) (*Server, *db.DB, func()) {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		ad, perr := classad.ParseOld(fmt.Sprintf("Owner = \"alice\"\nClusterId = %d\n", i))
		if perr != nil {
			t.Fatal(perr)
		}
		if err := d.Put(fmt.Sprintf("k%d", i), ad); err != nil {
			t.Fatal(err)
		}
	}
	srv := dbrpc.NewServer(d)
	cc, sc := net.Pipe()
	go func() {
		_ = srv.ServeConnOpts(dbrpc.NewStreamConn(sc),
			dbrpc.ServeOptions{ReadOnly: true, Privileged: true, IncludePrivate: true})
	}()
	client := dbrpc.NewClient(dbrpc.NewStreamConn(cc))

	svc, err := NewServer(client, arrowtype.Options{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	svc.SetSchemaTTL(ttl)
	return svc, d, func() { client.Close(); srv.Close(); d.Close() }
}

func hasField(s *arrow.Schema, name string) bool { return len(s.FieldIndices(name)) > 0 }

func deriveSchema(t *testing.T, svc *Server, table string) *arrow.Schema {
	t.Helper()
	s, _, err := svc.tableSchema(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestSchemaCacheHoldsThenExpires is the cache's whole contract, stated as what a client sees.
//
// A cached schema is a STALE schema, and that is the trade being made: a column that appears in
// the table does not appear in the schema until the entry ages out. So the test adds one and
// checks both halves -- held while the entry is live, picked up once it is not. Asserting only
// the second half would pass against no cache at all.
func TestSchemaCacheHoldsThenExpires(t *testing.T) {
	svc, d, done := cachedServer(t, time.Minute)
	defer done()

	base := time.Now()
	off := time.Duration(0)
	svc.schemas.now = func() time.Time { return base.Add(off) }

	if got := deriveSchema(t, svc, dbrpc.DefaultTable); hasField(got, "LateArrival") {
		t.Fatal("LateArrival in the schema before the ad carrying it was written")
	}

	ad, perr := classad.ParseOld("Owner = \"alice\"\nClusterId = 99\nLateArrival = 7\n")
	if perr != nil {
		t.Fatal(perr)
	}
	if err := d.Put("late", ad); err != nil {
		t.Fatal(err)
	}

	if got := deriveSchema(t, svc, dbrpc.DefaultTable); hasField(got, "LateArrival") {
		t.Error("a live cache entry was bypassed: the new column appeared immediately")
	}

	off = time.Minute + time.Second
	if got := deriveSchema(t, svc, dbrpc.DefaultTable); !hasField(got, "LateArrival") {
		t.Error("the entry expired and the new column still did not appear")
	}
}

// TestSchemaCacheOffDerivesEveryTime is the control. A zero TTL is what an embedder that has
// not asked for the cache gets, and it must behave as it did before the cache existed --
// otherwise the test above proves only that something is slow.
func TestSchemaCacheOffDerivesEveryTime(t *testing.T) {
	svc, d, done := cachedServer(t, 0)
	defer done()
	if svc.schemas != nil {
		t.Fatal("a zero TTL built a cache")
	}

	deriveSchema(t, svc, dbrpc.DefaultTable)
	ad, perr := classad.ParseOld("Owner = \"alice\"\nClusterId = 99\nLateArrival = 7\n")
	if perr != nil {
		t.Fatal(perr)
	}
	if err := d.Put("late", ad); err != nil {
		t.Fatal(err)
	}
	if got := deriveSchema(t, svc, dbrpc.DefaultTable); !hasField(got, "LateArrival") {
		t.Error("with the cache off, a new column did not appear on the next derivation")
	}
}

// userKey carries the identity a test's RPC authenticates as, so one server can be driven as
// two different principals.
type userKey struct{}

type ctxAuth struct{}

func (ctxAuth) Name() string { return "ctx" }
func (ctxAuth) Authenticate(ctx context.Context, _ metadata.MD) (string, error) {
	u, _ := ctx.Value(userKey{}).(string)
	return u, nil
}

func asUser(u string) context.Context {
	return context.WithValue(context.Background(), userKey{}, u)
}

// TestSchemaCacheIsScopedByPrivilege is the one that matters.
//
// A DAEMON peer's type report includes the private attributes; a READ peer's does not, because
// an attribute's numeric bounds are a disclosure even where its value is not. So the two derive
// different schemas from the same table, and a cache that did not key on that bit would serve
// whichever was computed first to whoever asked second -- handing a READ peer the names of
// private columns, through the one path that is supposed to make that impossible by
// construction.
//
// Both orders are checked. With an unscoped key, DAEMON-then-READ leaks the private column to
// READ, and READ-then-DAEMON silently denies DAEMON a column it is entitled to; a test of one
// order could be passed by a mutation that happens to fail the other way.
func TestSchemaCacheIsScopedByPrivilege(t *testing.T) {
	for _, order := range [][]string{{"root", "alice"}, {"alice", "root"}} {
		t.Run(order[0]+"-then-"+order[1], func(t *testing.T) {
			cat, done := store(t)
			defer done()

			sessions, err := NewSessions(cat, ctxAuth{}, func(perm, _, u string) bool {
				if perm == "DAEMON" {
					return u == "root"
				}
				return u == "root" || u == "alice"
			}, arrowtype.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer sessions.Close()

			svc, err := NewSessionServer(sessions, arrowtype.Options{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			svc.SetSchemaTTL(time.Minute)

			got := map[string]*arrow.Schema{}
			for _, u := range order {
				got[u] = deriveSchemaAs(t, svc, asUser(u), dbrpc.DefaultTable)
			}
			if hasField(got["alice"], "ClaimId") {
				t.Error("a READ peer was served a private column out of the cache")
			}
			if !hasField(got["root"], "ClaimId") {
				t.Error("a DAEMON peer lost a private column it is entitled to")
			}
			if !hasField(got["alice"], "Owner") || !hasField(got["root"], "Owner") {
				t.Error("both lost Owner; the difference above proves nothing")
			}
			if n := svc.schemas.size(); n != 2 {
				t.Errorf("%d cache entries for one table at two levels, want 2", n)
			}
		})
	}
}

func deriveSchemaAs(t *testing.T, svc *Server, ctx context.Context, table string) *arrow.Schema {
	t.Helper()
	s, _, err := svc.tableSchema(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestSchemaCacheIsBounded: the key holds a client-supplied table name, so an unbounded map is
// a client-driven allocation. Past the bound the oldest goes, which keeps the hot tables.
func TestSchemaCacheIsBounded(t *testing.T) {
	c := newSchemaCache(time.Hour)
	base := time.Now()
	off := time.Duration(0)
	c.now = func() time.Time { return base.Add(off) }

	empty := arrow.NewSchema(nil, nil)
	for i := 0; i < maxSchemaEntries+20; i++ {
		off = time.Duration(i) * time.Second
		c.put(schemaKey{table: fmt.Sprintf("t%d", i)}, empty, nil)
	}
	if n := c.size(); n > maxSchemaEntries {
		t.Errorf("%d entries, want at most %d", n, maxSchemaEntries)
	}
	// The most recent put survives; the first does not.
	if _, _, ok := c.get(schemaKey{table: fmt.Sprintf("t%d", maxSchemaEntries+19)}); !ok {
		t.Error("the newest entry was evicted")
	}
	if _, _, ok := c.get(schemaKey{table: "t0"}); ok {
		t.Error("the oldest entry survived a full cache")
	}
}

// TestSchemaCacheEvictsExpiredFirst: a daemon whose map filled with stale entries for cold
// tables must not evict a hot one to make room.
func TestSchemaCacheEvictsExpiredFirst(t *testing.T) {
	c := newSchemaCache(time.Minute)
	base := time.Now()
	off := time.Duration(0)
	c.now = func() time.Time { return base.Add(off) }

	empty := arrow.NewSchema(nil, nil)
	for i := 0; i < maxSchemaEntries-1; i++ {
		c.put(schemaKey{table: fmt.Sprintf("cold%d", i)}, empty, nil)
	}
	off = 2 * time.Minute // every cold entry is now stale
	c.put(schemaKey{table: "hot"}, empty, nil)
	for i := 0; i < 10; i++ {
		c.put(schemaKey{table: fmt.Sprintf("new%d", i)}, empty, nil)
	}
	if _, _, ok := c.get(schemaKey{table: "hot"}); !ok {
		t.Error("a live entry was evicted while stale ones remained")
	}
}

// TestNilSchemaCacheIsUsable: the zero-TTL server leaves the field nil, and every call site
// goes through these methods, so a nil receiver has to be safe rather than guarded at each one.
func TestNilSchemaCacheIsUsable(t *testing.T) {
	var c *schemaCache
	if _, _, ok := c.get(schemaKey{table: "ads"}); ok {
		t.Error("a nil cache reported a hit")
	}
	c.put(schemaKey{table: "ads"}, arrow.NewSchema(nil, nil), nil)
	if c.size() != 0 {
		t.Error("a nil cache grew")
	}
}

// TestSettingsReachTheServer closes the "parses and never arrives" hole for every field at
// once. Three of the four have no effect a client can see directly -- a budget, a stream cap
// and a cache lifetime are all invisible until they bite -- so a wiring mistake in any of them
// would leave the endpoint behaving as if unconfigured, which is exactly how the Version field
// shipped broken once already.
func TestSettingsReachTheServer(t *testing.T) {
	svc, _, done := cachedServer(t, 0)
	defer done()

	set := Settings{
		StreamBudgetBytes: 7 << 20,
		MaxStreams:        3,
		SchemaTTL:         45 * time.Second,
		Version:           "test-build",
	}
	if err := applySettings(svc, set); err != nil {
		t.Fatal(err)
	}
	if svc.streamBudget != set.StreamBudgetBytes {
		t.Errorf("stream budget = %d, want %d", svc.streamBudget, set.StreamBudgetBytes)
	}
	if cap(svc.streams) != set.MaxStreams {
		t.Errorf("stream cap = %d, want %d", cap(svc.streams), set.MaxStreams)
	}
	if svc.schemas == nil {
		t.Fatal("no schema cache after a non-zero SchemaTTL")
	}
	if svc.schemas.ttl != set.SchemaTTL {
		t.Errorf("schema TTL = %v, want %v", svc.schemas.ttl, set.SchemaTTL)
	}

	// And zero values put it back to the unconfigured behaviour, so a host that configures
	// nothing is not quietly carrying a previous server's settings.
	if err := applySettings(svc, Settings{}); err != nil {
		t.Fatal(err)
	}
	if svc.schemas != nil {
		t.Error("a zero SchemaTTL left the cache in place")
	}
}
