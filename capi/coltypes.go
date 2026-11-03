package main

import (
	"strings"
	"sync"
	"time"

	"github.com/bbockelm/htcondordb/arrowtype"
	"github.com/bbockelm/htcondordb/repl"
)

// Telling a caller what a column HOLDS, rather than letting it guess from the first cell.
//
// The Python driver's `description` documents its own type_code as "what arrived, not a
// schema": it reads the first non-null value of each column and reports that value's type.
// Three things are wrong with it, and they are the reason this exists.
//
//   - It is a guess. A column whose first row happens to be an integer and whose second is a
//     string reports INTEGER, and nothing ever revisits it.
//   - It is None for a column that is null all the way down, which is precisely the column a
//     caller most needs told about -- an absent attribute and a typed column with no values in
//     this result are indistinguishable.
//   - Reading METADATA forces I/O. A type can only come from data, so asking for `description`
//     on an unfetched cursor has to pull the first batch. PEP 249 expects the opposite: the
//     description is available after execute(), before anything is fetched.
//
// A declared type answers all three. It comes from the same derivation the Flight endpoint
// publishes, so a column means the same thing through the DB-API, through ADBC and in a
// Grafana panel.
//
// CACHED PER CONNECTION, because deriving a table's columns samples records and a driver runs
// many statements on one connection. Not shared between connections: the derivation is scoped
// to what the connection's own session may see, and an unprivileged connection must not be
// handed a privileged one's column list.
//
// NEVER FATAL. Every failure -- a table that has gone away, a daemon too old to profile, a
// statement whose shape has no declaration -- yields no types, and the caller falls back to
// what it did before. A driver that could not report rows because it could not report their
// types would be a worse driver.

// colTypeTTL is how long a connection reuses a table's derived columns. It matches the Flight
// endpoint's schema cache so the two do not disagree for longer than either is willing to be
// wrong.
const colTypeTTL = 30 * time.Second

type colTypeEntry struct {
	decisions []arrowtype.Decision
	at        time.Time
}

// colTypeCache holds a connection's recently derived table columns.
type colTypeCache struct {
	mu  sync.Mutex
	ent map[string]colTypeEntry
	now func() time.Time
}

func newColTypeCache() *colTypeCache {
	return &colTypeCache{ent: map[string]colTypeEntry{}, now: time.Now}
}

func (c *colTypeCache) get(table string) ([]arrowtype.Decision, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.ent[strings.ToLower(table)]
	if !ok || c.now().Sub(e.at) >= colTypeTTL {
		return nil, false
	}
	return e.decisions, true
}

func (c *colTypeCache) put(table string, decisions []arrowtype.Decision) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Bounded. Table names reach this from the caller's SQL, so a client looping over
	// generated names would otherwise grow a per-connection map for the connection's life.
	if len(c.ent) >= 32 {
		c.ent = map[string]colTypeEntry{}
	}
	c.ent[strings.ToLower(table)] = colTypeEntry{decisions: decisions, at: c.now()}
}

// columnKinds names the type of each column a statement returns, aligned with the headers, or
// returns nil when it cannot say.
//
// NIL RATHER THAN A LIST OF EMPTY STRINGS when nothing is known, so a caller can tell "this
// library does not report types" from "this column has no type". A per-column empty string is
// the second of those and is used for exactly that: a column the derivation declined to type.
func columnKinds(ex *repl.Executor, cache *colTypeCache, st *repl.Statement, headers []string) []string {
	if ex == nil || st == nil || st.Kind != repl.StmtSelect || st.Table == "" {
		return nil
	}
	table, ok := cache.get(st.Table)
	if !ok {
		var err error
		if table, err = ex.TableDecisions(st.Table); err != nil {
			return nil
		}
		cache.put(st.Table, table)
	}

	decisions := table
	if !isStar(st) {
		var err error
		if decisions, err = repl.ArrowDecisions(st, table); err != nil {
			return nil
		}
	}

	byName := make(map[string]string, len(decisions))
	for _, d := range decisions {
		if d.Verdict != arrowtype.Typed {
			continue
		}
		if kind, ok := arrowtype.KindName(d.Field.Type); ok {
			byName[strings.ToLower(d.Name)] = kind
		}
	}
	// Aligned with the HEADERS the caller will see, not with the decisions: a SELECT *
	// reports the union of the matched ads' attributes, which is a different list from the
	// table's declared columns, and a type landing against the wrong header would be worse
	// than no type at all.
	out := make([]string, len(headers))
	any := false
	for i, h := range headers {
		if kind, ok := byName[strings.ToLower(h)]; ok {
			out[i] = kind
			any = true
		}
	}
	if !any {
		return nil
	}
	return out
}
