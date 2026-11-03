package flightsvc

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// What the endpoint has settled on for a table's columns, so the schema stops moving on a whim.
//
// Every schema this endpoint publishes is MEASURED: a sample of a table that is still being
// written to, re-taken whenever the cache expires. That is sound for one read and wrong for
// everything a consumer does with a catalog. A column whose sample happens to contain one string
// this minute and none the next changes type; an attribute a pool starts setting appears in the
// middle of the column order; a DuckDB or Grafana that cached the schema is holding something
// that was true once.
//
// NOBODY WRITES THIS DOWN. It is not a schema an operator authors and the endpoint obeys -- ads
// are not typed and this does not pretend they are. It is the endpoint remembering what it
// measured, so that two derivations of an unchanged table agree even though each is a fresh
// sample. The one place a human states a type is HTCONDORDB_FLIGHT_ATTR_KINDS, which is older
// than this and answers a different question: the type is known and the sample does not show it.
//
// THE SCHEMA IS ALLOWED TO MOVE. It just has to earn it. This follows what the columnar
// accelerator does for its group schemas: re-derive every pass, adopt only when successive
// derivations agree, and write down what changed and why. A retype is the interesting case --
// one stray string in a numeric column moves nothing, while a column that has genuinely become a
// string moves once retypeAgreement derivations in a row have said so.
//
// ORDER IS APPEND-ONLY EVEN WHEN TYPE IS NOT, and the asymmetry is deliberate. A type that
// changes costs a client values it must re-read; a POSITION that changes costs it every column
// after the one that moved, silently, because nothing in the protocol says the fifth column is
// not what it was. So a new attribute is appended, never inserted, and a column whose attribute
// has left the sample keeps its place as nulls rather than renumbering the rest. A schema a
// client cached is therefore always a PREFIX of the current one, whatever the types have done.
//
// ONE COLUMN IS EXEMPT, and it is worth naming rather than discovering. The tail (`_ad`) is not
// an attribute -- it is this endpoint's own column, holding whatever the typed columns did not
// take -- and it is always LAST, so appending an attribute moves it. A client that reads the
// tail by position therefore has to re-read the schema; one that reads it by name, which is what
// a special column deserves, is unaffected. Putting it first instead would make turning the tail
// on shift every other column, which is worse.
//
// EVERY ADOPTED CHANGE BUMPS A GENERATION AND IS LOGGED. The generation rides in the published
// Arrow schema's metadata, so a consumer sees it on every fetch without asking; the log says when
// a column changed and on what evidence. The fingerprint (fingerprint.go) keeps doing the pin,
// because a content hash is what is correct across processes and restarts; the generation is for
// the person asking why a column is a string today.
//
// NEVER A CORRECTNESS DEPENDENCY. The file is an optimization for stability. If it cannot be read
// the endpoint starts from the measurement, as it did before this existed; if it cannot be
// written the memory lives for this process only. Both are logged. A daemon with no on-disk
// directory at all -- an in-memory store -- gets the in-process half, which is still enough to
// stop the schema moving between two RPCs of one session.

// retypeAgreement is how many successive derivations must measure the same new type before a
// settled column takes it.
//
// Three rather than two because the failure this guards against is a sample that wobbles, and a
// wobble produces two of anything often enough. Three rather than ten because each derivation
// costs a cache expiry -- at the default TTL a genuine change is adopted inside a couple of
// minutes, which is a reasonable time to be wrong about a column and an unreasonable one to make
// a dashboard wait.
const retypeAgreement = 3

// typeChangeMax bounds the retained change log. A stable table logs nothing, so a change is rare
// and keeping a long window is cheap.
const typeChangeMax = 64

// defaultColumnLimit bounds one table's remembered columns. A job ad carries a few hundred
// attributes; a thousand leaves room for a pool's own conventions without letting a runaway
// attribute namespace become a startup cost.
const defaultColumnLimit = 1000

// columnType is one column the endpoint has settled on.
//
// Kind is the arrowtype kind name rather than an Arrow type string, because it is the same
// vocabulary HTCONDORDB_FLIGHT_ATTR_KINDS uses and the same one arrowtype.KnownKinds validates:
// an operator reading this file and an operator writing that knob should not have to learn two
// spellings of int64.
type columnType struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Nullable bool   `json:"nullable"`
	// Pending is the retype the measurement has been asking for and how many successive
	// derivations have asked for it. Persisted, because the evidence is the expensive part:
	// dropping it at a restart would mean a column that has genuinely changed starts over
	// every time the daemon does, and on a daemon that restarts often it would never settle.
	Pending *pendingType `json:"pending,omitempty"`
}

// pendingType is a candidate retype and the evidence for it.
type pendingType struct {
	Kind     string `json:"kind"`
	Nullable bool   `json:"nullable"`
	Seen     int    `json:"seen"`
}

// tableTypes is one table's settled columns, in the order they were first seen.
type tableTypes struct {
	// Generation increments on every adopted change. It starts at 1, so 0 means a table this
	// endpoint has not derived -- a distinction a consumer comparing generations needs.
	Generation int64        `json:"generation"`
	Columns    []columnType `json:"columns"`
	Changes    []typeChange `json:"changes,omitempty"`
}

// typeChange is one adopted change, for the log.
type typeChange struct {
	Unix       int64     `json:"unix"`
	Generation int64     `json:"generation"`
	Reason     string    `json:"reason"`
	Added      []string  `json:"added,omitempty"`
	Retyped    []retyped `json:"retyped,omitempty"`
}

// retyped is one column whose type changed across a generation.
type retyped struct {
	Name   string `json:"name"`
	Before string `json:"before"`
	After  string `json:"after"`
}

// typeMemory holds every table's settled columns, and persists them.
//
// SCOPED BY TABLE ONLY, not by privilege, which is the one thing here that needs justifying: the
// schema cache IS privilege-scoped, because an unprivileged type report has private attributes
// filtered out. A memory keyed the same way would be two memories of one table that drift apart,
// and the privileged one would leak into the file. So this records only what an UNPRIVILEGED
// reader may see, and a privileged reader's extra columns are served from its own measurement and
// never written down. The file is therefore safe to read: it names no attribute a READ peer could
// not already list.
type typeMemory struct {
	mu    sync.Mutex
	path  string // empty disables persistence
	tbl   map[string]*tableTypes
	dirty bool
	// limit caps the columns one table may remember. A table whose ads carry per-job attribute
	// names -- which happens -- would otherwise grow this file without bound, and an unbounded
	// file read at startup is a slow start that gets slower. Past the cap new attributes are
	// still SERVED, from the measurement; they are just not written down, so they are not
	// stable across a restart.
	limit int
	// warn reports a condition an operator should know about. Nil is allowed: this type is
	// usable from a test with no logger.
	warn func(string)
	// note reports an adopted change -- an ordinary event, not a problem, and the reason this
	// is separate from warn. A change log only a file holds is not visible: an operator would
	// have to know the file exists and go and read it, having first wondered. The daemon log
	// is what they already watch, so a retype says so there, once, when it happens.
	note func(string)
	// now is the clock, so a test can assert what a change log records.
	now func() time.Time
}

// newTypeMemory loads the file, or starts empty.
//
// A missing file is the ordinary first-run case and not worth a warning. A file that exists and
// cannot be read or parsed IS worth one: it means a schema an operator may be relying on has
// silently reset, and the symptom -- clients re-fetching catalogs -- points nowhere near here.
func newTypeMemory(path string, warn, note func(string)) *typeMemory {
	d := &typeMemory{path: path, tbl: map[string]*tableTypes{},
		limit: defaultColumnLimit, warn: warn, note: note, now: time.Now}
	if path == "" {
		return d
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) && warn != nil {
			warn(fmt.Sprintf("cannot read the Flight column types at %s (%v); starting from "+
				"the measurement, so published column types may change", path, err))
		}
		return d
	}
	var tbl map[string]*tableTypes
	if err := json.Unmarshal(raw, &tbl); err != nil {
		if warn != nil {
			warn(fmt.Sprintf("the Flight column types at %s are not readable JSON (%v); "+
				"starting from the measurement", path, err))
		}
		return d
	}
	for name, t := range tbl {
		if t != nil {
			d.tbl[strings.ToLower(name)] = t
		}
	}
	return d
}

// reconcile returns the decisions to publish for a table: the settled columns in their settled
// order, then whatever the measurement found that is not remembered yet.
//
// CALLED ONCE PER DERIVATION, which is what makes the agreement count mean anything. The caller
// reaches here only on a schema-cache miss, so "successive derivations" is successive samples
// separated by the cache TTL, not successive requests.
//
// It also EXTENDS the memory with what it just appended and adopts any retype whose evidence is
// now complete, which is why it takes the write lock rather than being a pure function.
// Remembering on first sight is what makes the order stable: an attribute's position is fixed by
// when it was first seen, not by where this minute's measurement happens to sort it.
//
// private says whether the measurement behind these decisions could see private attributes. A
// privileged reader's extra columns are served but not recorded -- see the type comment.
func (d *typeMemory) reconcile(table string, measured []arrowtype.Decision, private bool) []arrowtype.Decision {
	if d == nil {
		return measured
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	key := strings.ToLower(table)
	t := d.tbl[key]
	first := t == nil
	if first {
		t = &tableTypes{}
		d.tbl[key] = t
	}

	byName := make(map[string]arrowtype.Decision, len(measured))
	var tail *arrowtype.Decision
	for i := range measured {
		if measured[i].Tail {
			// The tail is not an attribute and is not remembered: it is this endpoint's own
			// column, always last, and its type never varies.
			tail = &measured[i]
			continue
		}
		byName[strings.ToLower(measured[i].Name)] = measured[i]
	}

	out := make([]arrowtype.Decision, 0, len(measured)+1)
	seen := make(map[string]bool, len(t.Columns))
	var adopted []retyped
	for i := range t.Columns {
		col := &t.Columns[i]
		lower := strings.ToLower(col.Name)
		seen[lower] = true
		m, present := byName[lower]
		switch {
		case !present:
			// Absent from THIS sample, which is not evidence of anything: an attribute the
			// pool sets on some ads and not others leaves and re-enters a sample on its own.
			// The column keeps its place and any evidence it had already gathered.
		case sameShape(m, *col):
			// The measurement agrees. Any candidate retype was a wobble, and the wobble is
			// over: clearing it here is what makes the agreement SUCCESSIVE rather than
			// cumulative, so a column that disagrees one sample in three never moves.
			if col.Pending != nil {
				col.Pending = nil
				d.dirty = true
			}
			out = append(out, m)
			continue
		default:
			if before, after, ok := d.noteRetype(col, m); ok {
				adopted = append(adopted, retyped{Name: col.Name, Before: before, After: after})
				// Settled this derivation, so publish what was just adopted rather than
				// making the client wait one more sample for a type already decided.
				out = append(out, m)
				continue
			}
		}
		// Either the measurement now says something else and has not yet earned it, or the
		// attribute is absent from this sample. Both publish the SETTLED column: the first
		// because the evidence is not in, the second because dropping a column renumbers
		// every one after it.
		dec, ok := settledDecision(*col)
		if !ok {
			// A kind this build does not know, from a file written by a newer one. Serving
			// it as something else would be a column nobody settled on, so it is left out
			// and said so.
			if d.warn != nil {
				d.warn(fmt.Sprintf("the Flight column types name kind %q for %s.%s, which "+
					"this build does not know; the column is omitted", col.Kind, table, col.Name))
			}
			continue
		}
		out = append(out, dec)
	}

	// Whatever the measurement found that is not remembered yet, IN THE MEASUREMENT'S OWN ORDER.
	// Sorting them would be deterministic too and would throw away the order the derivation
	// produced, which is the schema's -- and the first derivation of a table is every column at
	// once, so sorting it would alphabetize the whole schema once and for all. Remembering on
	// first sight is what makes the order stable afterwards; it does not need to impose one.
	var added []string
	capped := false
	for i := range measured {
		if measured[i].Tail {
			continue
		}
		name := strings.ToLower(measured[i].Name)
		if seen[name] {
			continue
		}
		seen[name] = true
		m := measured[i]
		// Served either way. The cap bounds what is WRITTEN DOWN, never what a client is
		// shown: hiding data to stay under a file-size limit would be a wrong answer in
		// service of an operational convenience.
		out = append(out, m)
		if private {
			// Not recorded: the file must name nothing a READ peer could not list.
			continue
		}
		if len(t.Columns) >= d.limit {
			capped = true
			continue
		}
		if col, ok := columnOf(m); ok {
			t.Columns = append(t.Columns, col)
			added = append(added, col.Name)
			d.dirty = true
		}
	}
	if capped && d.warn != nil {
		d.warn(fmt.Sprintf("table %s has reached the Flight column limit of %d; further "+
			"attributes are served but their column types and positions are not stable "+
			"across a restart", table, d.limit))
	}

	d.recordChange(table, t, first, added, adopted)

	if tail != nil {
		out = append(out, *tail)
	}
	return out
}

// noteRetype folds one disagreeing measurement into a column's evidence, and adopts the new type
// once retypeAgreement successive derivations have asked for it. ok reports that it adopted.
//
// The candidate is REPLACED rather than merged when the measurement asks for something different
// again, so a column oscillating between two wrong answers accumulates evidence for neither.
func (d *typeMemory) noteRetype(col *columnType, m arrowtype.Decision) (before, after string, ok bool) {
	want, hasType := columnOf(m)
	if !hasType {
		// The measurement declined to type it at all this time -- every value undefined in
		// this sample, say. That is not a competing type, so it is not evidence for one.
		return "", "", false
	}
	d.dirty = true
	if col.Pending == nil || col.Pending.Kind != want.Kind || col.Pending.Nullable != want.Nullable {
		col.Pending = &pendingType{Kind: want.Kind, Nullable: want.Nullable, Seen: 1}
		return "", "", false
	}
	col.Pending.Seen++
	if col.Pending.Seen < retypeAgreement {
		return "", "", false
	}
	before, after = col.Kind, want.Kind
	col.Kind, col.Nullable = want.Kind, want.Nullable
	col.Pending = nil
	return before, after, true
}

// recordChange bumps the generation and appends a log entry when this derivation changed
// anything. A derivation that changed nothing -- the steady state -- leaves both alone, so a
// stable table holds one generation indefinitely and a consumer comparing generations is
// comparing something that means what it says.
func (d *typeMemory) recordChange(table string, t *tableTypes, first bool, added []string, adopted []retyped) {
	if len(added) == 0 && len(adopted) == 0 {
		return
	}
	t.Generation++
	ev := typeChange{Unix: d.now().Unix(), Generation: t.Generation,
		Added: added, Retyped: adopted, Reason: explainChange(first, added, adopted)}
	t.Changes = append(t.Changes, ev)
	if len(t.Changes) > typeChangeMax {
		t.Changes = t.Changes[len(t.Changes)-typeChangeMax:]
	}
	d.dirty = true
	// Announced only for a RETYPE, and only after the first derivation. A table's first
	// derivation appends every column it has and says nothing anyone needs to read; an
	// attribute appearing and being appended is routine and costs a client nothing, because
	// the schema it cached is still a prefix. A column changing type is the one that can make
	// a consumer's cached catalog wrong about a value, so it is the one worth a line.
	if d.note != nil && len(adopted) > 0 {
		d.note(fmt.Sprintf("Flight SQL: table %s is now at schema generation %d: %s",
			table, t.Generation, ev.Reason))
	}
}

// explainChange says what a generation was for, in the terms an operator asking "why is this
// column a string today" needs: what moved, and on what evidence.
func explainChange(first bool, added []string, adopted []retyped) string {
	var parts []string
	switch {
	case first && len(added) > 0:
		parts = append(parts, fmt.Sprintf("first derivation of the table: %d columns", len(added)))
	case len(added) > 0:
		parts = append(parts, fmt.Sprintf("%d attribute(s) appeared in the sample and were appended: %s",
			len(added), strings.Join(shortList(added), ", ")))
	}
	if len(adopted) > 0 {
		names := make([]string, 0, len(adopted))
		for _, r := range adopted {
			names = append(names, fmt.Sprintf("%s %s->%s", r.Name, r.Before, r.After))
		}
		parts = append(parts, fmt.Sprintf("%d column(s) retyped after %d successive derivations "+
			"measured the new type: %s", len(adopted), retypeAgreement, strings.Join(shortList(names), ", ")))
	}
	return strings.Join(parts, "; ")
}

// shortList bounds what a reason names, so one derivation that appends two hundred attributes
// does not write a two-hundred-name sentence into the log sixty-four times over.
func shortList(names []string) []string {
	const max = 8
	if len(names) <= max {
		return names
	}
	out := append([]string(nil), names[:max]...)
	return append(out, fmt.Sprintf("and %d more", len(names)-max))
}

// sameShape reports whether a measured decision still matches what was settled. Only the
// published shape is compared -- the kind and the nullability -- because the reason an attribute
// got its type is prose that moves with the sample.
func sameShape(m arrowtype.Decision, col columnType) bool {
	got, ok := columnOf(m)
	return ok && got.Kind == col.Kind && got.Nullable == col.Nullable
}

// columnOf renders a decision as a remembered column, or reports that it has none: an untyped
// attribute is not a column, so there is nothing to remember and nothing to keep stable.
func columnOf(m arrowtype.Decision) (columnType, bool) {
	if m.Verdict != arrowtype.Typed || m.Tail {
		return columnType{}, false
	}
	kind, ok := arrowtype.KindName(m.Field.Type)
	if !ok {
		return columnType{}, false
	}
	return columnType{Name: m.Name, Kind: kind, Nullable: m.Field.Nullable}, true
}

// settledDecision builds the decision a settled column publishes.
func settledDecision(col columnType) (arrowtype.Decision, bool) {
	dt, ok := arrowtype.TypeOfKind(col.Kind)
	if !ok {
		return arrowtype.Decision{}, false
	}
	return arrowtype.Decision{
		Name:    col.Name,
		Verdict: arrowtype.Typed,
		// Always nullable: the sample that made it non-nullable is not the sample in hand,
		// and a non-nullable Arrow column that receives a null is a lie the builder cannot
		// catch.
		Field:  arrow.Field{Name: col.Name, Type: dt, Nullable: true},
		Reason: "settled",
		Detail: "this column's type is what the endpoint has settled on for the table, which " +
			"this sample does not currently measure",
	}, true
}

// generation reports a table's current schema generation, 0 for a table never derived.
func (d *typeMemory) generation(table string) int64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if t := d.tbl[strings.ToLower(table)]; t != nil {
		return t.Generation
	}
	return 0
}

// changes reports a table's change log, most recent last.
func (d *typeMemory) changes(table string) []typeChange {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	t := d.tbl[strings.ToLower(table)]
	if t == nil {
		return nil
	}
	return append([]typeChange(nil), t.Changes...)
}

// columns reports a table's settled columns, for a test or a diagnostic.
func (d *typeMemory) columns(table string) []columnType {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	t := d.tbl[strings.ToLower(table)]
	if t == nil {
		return nil
	}
	return append([]columnType(nil), t.Columns...)
}

// tables lists the tables held, sorted, for a diagnostic.
func (d *typeMemory) tables() []string {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, 0, len(d.tbl))
	for name := range d.tbl {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// flush writes the file if anything has changed. Called off the read path.
func (d *typeMemory) flush() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.path == "" || !d.dirty {
		d.mu.Unlock()
		return
	}
	raw, err := json.MarshalIndent(d.tbl, "", "  ")
	path := d.path
	d.dirty = false
	warn := d.warn
	d.mu.Unlock()
	if err != nil {
		if warn != nil {
			warn(fmt.Sprintf("cannot encode the Flight column types: %v", err))
		}
		return
	}
	// Temp-and-rename, because a half-written file read at the next start is worse than none:
	// it would publish a truncated column list as though it were the whole one.
	tmp := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		if warn != nil {
			warn(fmt.Sprintf("cannot create the directory for %s: %v", path, err))
		}
		return
	}
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		if warn != nil {
			warn(fmt.Sprintf("cannot write the Flight column types to %s (%v); column types "+
				"will be re-measured after a restart", tmp, err))
		}
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		if warn != nil {
			warn(fmt.Sprintf("cannot replace the Flight column types at %s: %v", path, err))
		}
	}
}
