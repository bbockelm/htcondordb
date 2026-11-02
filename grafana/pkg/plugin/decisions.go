package plugin

import (
	"strings"
	"sync"
	"time"

	"github.com/bbockelm/htcondordb/arrowtype"
	"github.com/bbockelm/htcondordb/repl"
)

// Asking the store what a result's columns hold, rather than guessing from the cells.
//
// This plugin used to infer every column's type by parsing its values, which is the guess the
// Arrow type contract exists to replace -- and a guess made in a separate module, so it drifted
// from the daemon's answer without either side being able to see it. Now the same derivation
// the Flight endpoint publishes types the frame.
//
// CACHED, because it is not free: deriving a table's columns samples records, and a Grafana
// dashboard refreshes several panels on a timer. Without a cache every panel would put a
// sampling scan through the daemon on every tick.
//
// NEVER FATAL. Every failure here -- a query this plugin's own executor can run but repl's
// parser spells differently, a table that has gone away, a daemon too old to profile -- falls
// back to inference, which is exactly what this did before. A panel that stops rendering
// because the type lookup failed would be a worse plugin than one that occasionally guesses.

// decisionTTL is how long a table's columns are reused. Short, because the schema follows the
// table and a column that appears should show up in a panel on the order of a refresh rather
// than on the order of a restart. It matches the endpoint's own schema cache so the two do not
// disagree for longer than either is willing to be wrong.
const decisionTTL = 30 * time.Second

type decisionEntry struct {
	decisions []arrowtype.Decision
	at        time.Time
}

// decisionCache holds recently derived table columns.
type decisionCache struct {
	mu  sync.Mutex
	ent map[string]decisionEntry
	now func() time.Time
}

func newDecisionCache() *decisionCache {
	return &decisionCache{ent: map[string]decisionEntry{}, now: time.Now}
}

func (c *decisionCache) get(table string) ([]arrowtype.Decision, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.ent[strings.ToLower(table)]
	if !ok || c.now().Sub(e.at) >= decisionTTL {
		return nil, false
	}
	return e.decisions, true
}

func (c *decisionCache) put(table string, decisions []arrowtype.Decision) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Bounded: table names reach this from a dashboard, and a dashboard is edited by
	// people. Past the bound the map is dropped rather than swept -- re-deriving a
	// handful of tables costs one sampling scan each and happens about never.
	if len(c.ent) >= 64 {
		c.ent = map[string]decisionEntry{}
	}
	c.ent[strings.ToLower(table)] = decisionEntry{decisions: decisions, at: c.now()}
}

// decisionsFor types the columns a query will return, or returns nil to fall back to inference.
func (d *Datasource) decisionsFor(exec *repl.Executor, sql string) []arrowtype.Decision {
	st, err := repl.Parse(sql)
	if err != nil || st.Kind != repl.StmtSelect || st.Table == "" {
		return nil
	}
	table, ok := d.decisions.get(st.Table)
	if !ok {
		table, err = exec.TableDecisions(st.Table)
		if err != nil {
			return nil
		}
		d.decisions.put(st.Table, table)
	}
	// A `SELECT *` returns the ad's own attributes, so the table's columns ARE the
	// result's columns; anything else is typed from its select list.
	for _, it := range st.Items {
		if it.Star {
			return table
		}
	}
	decisions, err := repl.ArrowDecisions(st, table)
	if err != nil {
		return nil
	}
	return decisions
}
