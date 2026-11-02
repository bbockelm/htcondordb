package flightsvc

import (
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// Holding a table's derived Arrow schema for a short while.
//
// Deriving a schema means sampling records and classifying them, and every statement needs one
// -- GetFlightInfo to answer the shape, DoGet to build the encoder, and a prepared statement
// again at the bind. Unsampled, a client that prepares once and executes in a loop pays a
// profile scan per call, inside the daemon, competing with the store's own work.
//
// TWO THINGS THIS FIXES, and one it does not.
//
// It stops the repeat scans, which is the obvious part.
//
// It also makes the schema a client is PROMISED and the schema it is DELIVERED the same object
// in the common case. The two arrive on separate RPCs that each derived independently, so on a
// moving table they were two measurements of different moments: a column could appear between
// them, and a client that trusted the first read the second by position. Within the TTL both
// now read one entry.
//
// It does NOT make that a guarantee. The entry can expire between the two calls, and nothing
// here pins a storage generation -- the columnar schema has none to pin (dbrpc's type report
// carries profiles and a sample size, and no generation), so there is no way yet to say "the
// same schema or an error". A client reading the stream's own schema, which is what every
// Arrow reader does, is unaffected either way; the exposure is a client that reads
// execute_schema and then trusts it positionally.
//
// SCOPED BY PRIVILEGE, which is not optional. An unprivileged session's type report has the
// private attributes' profiles filtered out of it server-side, precisely because an attribute's
// numeric BOUNDS are a disclosure even when its value is not. So a privileged reader and an
// unprivileged one derive different schemas from the same table, and one cache shared between
// them would serve the private column names to whoever asked second. The key carries that bit,
// and it is the only identity-dependent input to the derivation -- so two readers at the same
// level share correctly, and two at different levels cannot.
type schemaCache struct {
	mu  sync.Mutex
	ttl time.Duration
	// now is injectable so a test can age an entry without sleeping.
	now func() time.Time
	ent map[schemaKey]schemaEntry
}

type schemaKey struct {
	table string
	// private is whether the deriving session saw private attributes.
	private bool
}

type schemaEntry struct {
	schema    *arrow.Schema
	decisions []arrowtype.Decision
	at        time.Time
}

// maxSchemaEntries bounds the cache. It is a daemon-resident map keyed partly by table name,
// and table names come from clients: a client asking for a thousand tables that do not exist
// must not grow this, and one that asks for a thousand that do should not either. Past the
// bound the oldest entry goes, which for this access pattern -- a handful of hot tables -- means
// the hot ones stay.
const maxSchemaEntries = 64

func newSchemaCache(ttl time.Duration) *schemaCache {
	return &schemaCache{ttl: ttl, now: time.Now, ent: map[schemaKey]schemaEntry{}}
}

// get returns a live entry, or ok=false. A zero TTL is a disabled cache and always misses,
// which is the behaviour an unconfigured embedder gets.
func (c *schemaCache) get(k schemaKey) (*arrow.Schema, []arrowtype.Decision, bool) {
	if c == nil || c.ttl <= 0 {
		return nil, nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.ent[k]
	if !ok {
		return nil, nil, false
	}
	if c.now().Sub(e.at) >= c.ttl {
		delete(c.ent, k)
		return nil, nil, false
	}
	// Handed out as-is, not copied. Nothing downstream mutates a decision: project builds a
	// new slice, schemaOf and Columns only read, and the encoder takes the decisions by
	// value. Stated here because a copy is the kind of thing a later reader removes as
	// redundant, or adds in a panic -- the invariant is that these are read-only.
	return e.schema, e.decisions, true
}

func (c *schemaCache) put(k schemaKey, schema *arrow.Schema, decisions []arrowtype.Decision) {
	if c == nil || c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.ent) >= maxSchemaEntries {
		c.evictOldestLocked()
	}
	c.ent[k] = schemaEntry{schema: schema, decisions: decisions, at: c.now()}
}

// evictOldestLocked drops expired entries first and, if that freed nothing, the oldest live
// one. Sweeping expiries first is what keeps a long-lived daemon from evicting a hot table
// because the map filled with stale entries for cold ones.
func (c *schemaCache) evictOldestLocked() {
	now := c.now()
	for k, e := range c.ent {
		if now.Sub(e.at) >= c.ttl {
			delete(c.ent, k)
		}
	}
	if len(c.ent) < maxSchemaEntries {
		return
	}
	var oldestKey schemaKey
	var oldest time.Time
	first := true
	for k, e := range c.ent {
		if first || e.at.Before(oldest) {
			oldestKey, oldest, first = k, e.at, false
		}
	}
	if !first {
		delete(c.ent, oldestKey)
	}
}

// size reports the live entry count, for tests and for a future stats surface.
func (c *schemaCache) size() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.ent)
}
