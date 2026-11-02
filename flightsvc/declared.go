package flightsvc

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// Declaring a table's columns, so the schema stops moving under its readers.
//
// Every schema this endpoint publishes is MEASURED: a sample of a table that is still being
// written to, re-taken whenever the cache expires. That is sound for one read and wrong for
// everything a consumer does with a catalog. A column whose sample happens to contain one
// string this minute and none the next changes type; an attribute a pool starts setting appears
// in the middle of the column order; a DuckDB or Grafana that cached the schema is holding
// something that was true once.
//
// The pin (pin.go) made that VISIBLE -- a fetch refuses rather than delivering rows under a
// superseded shape -- which is the right failure but still a failure. This is what makes it
// rare.
//
// THE RULE IS MONOTONE: A DECLARATION IS A FLOOR, NOT A CEILING.
//
//   - A declared column keeps its declared type and position for as long as the declaration
//     stands. The measurement cannot retype it; an admin can.
//   - An attribute nobody has declared is typed by measurement and APPENDED, never inserted.
//   - A declared column whose attribute has vanished from the sample stays, as a column of
//     nulls. Dropping it would renumber everything after it.
//
// The consequence is the one that matters to a client: a schema it cached is always a PREFIX of
// the current one. A positional reader cannot be made to read the wrong column by anything that
// happens to the table; the worst that happens is that it does not know about a new column at
// the end. That is a property worth more than keeping the schema minimal.
//
// ONE COLUMN IS EXEMPT, and it is worth naming rather than discovering. The tail (`_ad`) is not
// an attribute -- it is this endpoint's own column, holding whatever the typed columns did not
// take -- and it is always LAST, so appending an attribute moves it. A client that reads the
// tail by position therefore has to re-read the schema; one that reads it by name, which is
// what a special column deserves, is unaffected. Putting it first instead would make turning
// the tail on shift every other column, which is worse.
//
// WHAT IT COSTS. An attribute that genuinely changes type keeps its old column, and the values
// that no longer fit are coerced or nulled -- counted, per column, by the encoder, which is
// where a type that has stopped describing its data should show up. An admin who wants the new
// type says so (HTCONDORDB_FLIGHT_ATTR_KINDS), which is the same knob that answers every other
// "the measurement is wrong about this one" case.
//
// NEVER A CORRECTNESS DEPENDENCY. The file is an optimization for stability. If it cannot be
// read the endpoint starts from the measurement, as it did before this existed; if it cannot be
// written the declaration lives for this process only. Both are logged. A daemon with no
// on-disk directory at all -- an in-memory store -- gets the in-process half, which is still
// enough to stop the schema moving between two RPCs of one session.

// declaredColumn is one column of a table's declaration.
//
// Kind is the arrowtype kind name rather than an Arrow type string, because it is the same
// vocabulary HTCONDORDB_FLIGHT_ATTR_KINDS uses and the same one arrowtype.KnownKinds validates:
// an operator reading this file and an operator writing that knob should not have to learn two
// spellings of int64.
type declaredColumn struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Nullable bool   `json:"nullable"`
}

// declaredTable is one table's columns, in the order they were declared.
type declaredTable struct {
	Columns []declaredColumn `json:"columns"`
}

// declarations holds every table's declaration, and persists them.
//
// SCOPED BY TABLE ONLY, not by privilege, which is the one thing here that needs justifying:
// the schema cache IS privilege-scoped, because an unprivileged type report has private
// attributes filtered out. A declaration keyed the same way would be two declarations of one
// table that drift apart, and the privileged one would leak into the file. So the declaration
// records only what an UNPRIVILEGED reader may see, and a privileged reader's extra columns are
// appended from its own measurement and never written down. The file is therefore safe to read:
// it names no attribute a READ peer could not already list.
type declarations struct {
	mu    sync.Mutex
	path  string // empty disables persistence
	tbl   map[string]*declaredTable
	dirty bool
	// limit caps the columns one table may declare. A table whose ads carry per-job
	// attribute names -- which happens -- would otherwise grow this file without bound,
	// and an unbounded file read at startup is a slow start that gets slower. Past the cap
	// new attributes are still SERVED, from the measurement; they are just not written
	// down, so they are not stable across a restart.
	limit int
	// warn reports a condition an operator should know about. Nil is allowed: this type is
	// usable from a test with no logger.
	warn func(string)
}

// defaultDeclarationLimit bounds one table's declared columns. A job ad carries a few hundred
// attributes; a thousand leaves room for a pool's own conventions without letting a runaway
// attribute namespace become a startup cost.
const defaultDeclarationLimit = 1000

// newDeclarations loads the declaration file, or starts empty.
//
// A missing file is the ordinary first-run case and not worth a warning. A file that exists and
// cannot be read or parsed IS worth one: it means a schema an operator may be relying on has
// silently reset, and the symptom -- clients re-fetching catalogs -- points nowhere near here.
func newDeclarations(path string, warn func(string)) *declarations {
	d := &declarations{path: path, tbl: map[string]*declaredTable{},
		limit: defaultDeclarationLimit, warn: warn}
	if path == "" {
		return d
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) && warn != nil {
			warn(fmt.Sprintf("cannot read the Flight schema declaration at %s (%v); "+
				"starting from the measurement, so published column types may change", path, err))
		}
		return d
	}
	var tbl map[string]*declaredTable
	if err := json.Unmarshal(raw, &tbl); err != nil {
		if warn != nil {
			warn(fmt.Sprintf("the Flight schema declaration at %s is not readable JSON (%v); "+
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

// reconcile returns the decisions to publish for a table: the declaration's columns in the
// declaration's order, then whatever the measurement found that is not declared yet.
//
// It also EXTENDS the declaration with what it just appended, which is why it takes the write
// lock rather than being a pure function. Declaring on first sight is what makes the order
// stable: an attribute's position is fixed by when it was first seen, not by where this
// minute's measurement happens to sort it.
//
// private says whether the measurement behind these decisions could see private attributes. A
// privileged reader's extra columns are served but not recorded -- see the type comment.
func (d *declarations) reconcile(table string, measured []arrowtype.Decision, private bool) []arrowtype.Decision {
	if d == nil {
		return measured
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	key := strings.ToLower(table)
	t := d.tbl[key]
	if t == nil {
		t = &declaredTable{}
		d.tbl[key] = t
	}

	byName := make(map[string]arrowtype.Decision, len(measured))
	var tail *arrowtype.Decision
	for i := range measured {
		if measured[i].Tail {
			// The tail is not an attribute and is not declared: it is this endpoint's
			// own column, always last, and its type never varies.
			tail = &measured[i]
			continue
		}
		byName[strings.ToLower(measured[i].Name)] = measured[i]
	}

	out := make([]arrowtype.Decision, 0, len(measured)+1)
	seen := make(map[string]bool, len(t.Columns))
	for _, col := range t.Columns {
		lower := strings.ToLower(col.Name)
		seen[lower] = true
		if m, ok := byName[lower]; ok && sameShape(m, col) {
			// The measurement still agrees. Take its decision, which carries the
			// reason and detail the declaration does not store.
			out = append(out, m)
			continue
		}
		// Either the measurement now says something else, or the attribute is absent
		// from this sample. Both publish the DECLARED column: the first because a
		// declaration outranks a measurement, the second because dropping a column
		// renumbers every one after it.
		dec, ok := declaredDecision(col)
		if !ok {
			// A kind this build does not know, from a file written by a newer one.
			// Serving it as something else would be a column the operator never
			// declared, so it is left out and said so.
			if d.warn != nil {
				d.warn(fmt.Sprintf("the Flight schema declaration names kind %q for %s.%s, "+
					"which this build does not know; the column is omitted", col.Kind, table, col.Name))
			}
			continue
		}
		out = append(out, dec)
	}

	// Whatever the measurement found that is not declared yet, IN THE MEASUREMENT'S OWN
	// ORDER. Sorting them would be deterministic too and would throw away the order the
	// derivation produced, which is the schema's -- and the first derivation of a table is
	// every column at once, so sorting it would alphabetize the whole schema once and for
	// all. Declaring on first sight is what makes the order stable afterwards; it does not
	// need to impose one.
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
		// Served either way. The cap bounds what is WRITTEN DOWN, never what a client
		// is shown: hiding data to stay under a file-size limit would be a wrong
		// answer in service of an operational convenience.
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
			d.dirty = true
		}
	}
	if capped && d.warn != nil {
		d.warn(fmt.Sprintf("table %s has reached the Flight schema declaration limit of %d "+
			"columns; further attributes are served but their column types and positions "+
			"are not stable across a restart", table, d.limit))
	}

	if tail != nil {
		out = append(out, *tail)
	}
	return out
}

// sameShape reports whether a measured decision still matches what was declared. Only the
// published shape is compared -- the kind and the nullability -- because the reason an
// attribute got its type is prose that moves with the sample.
func sameShape(m arrowtype.Decision, col declaredColumn) bool {
	got, ok := columnOf(m)
	return ok && got.Kind == col.Kind && got.Nullable == col.Nullable
}

// columnOf renders a decision as a declaration entry, or reports that it has none: an untyped
// attribute is not a column, so there is nothing to declare and nothing to keep stable.
func columnOf(m arrowtype.Decision) (declaredColumn, bool) {
	if m.Verdict != arrowtype.Typed || m.Tail {
		return declaredColumn{}, false
	}
	kind, ok := arrowtype.KindName(m.Field.Type)
	if !ok {
		return declaredColumn{}, false
	}
	return declaredColumn{Name: m.Name, Kind: kind, Nullable: m.Field.Nullable}, true
}

// declaredDecision builds the decision a declared column publishes.
func declaredDecision(col declaredColumn) (arrowtype.Decision, bool) {
	dt, ok := arrowtype.TypeOfKind(col.Kind)
	if !ok {
		return arrowtype.Decision{}, false
	}
	return arrowtype.Decision{
		Name:    col.Name,
		Verdict: arrowtype.Typed,
		// Always nullable: the sample that made it non-nullable is not the sample in
		// hand, and a non-nullable Arrow column that receives a null is a lie the
		// builder cannot catch.
		Field:  arrow.Field{Name: col.Name, Type: dt, Nullable: true},
		Reason: "declared",
		Detail: "this column's type is declared for the table rather than measured from the sample",
	}, true
}

// flush writes the declaration if it has changed. Called off the read path.
func (d *declarations) flush() {
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
			warn(fmt.Sprintf("cannot encode the Flight schema declaration: %v", err))
		}
		return
	}
	// Temp-and-rename, because a half-written declaration read at the next start is worse
	// than no declaration: it would publish a truncated column list as though it were the
	// whole one.
	tmp := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		if warn != nil {
			warn(fmt.Sprintf("cannot create the directory for %s: %v", path, err))
		}
		return
	}
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		if warn != nil {
			warn(fmt.Sprintf("cannot write the Flight schema declaration to %s (%v); "+
				"column types will be re-measured after a restart", tmp, err))
		}
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		if warn != nil {
			warn(fmt.Sprintf("cannot replace the Flight schema declaration at %s: %v", path, err))
		}
	}
}

// columns reports a table's declared columns, for a test or a diagnostic.
func (d *declarations) columns(table string) []declaredColumn {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	t := d.tbl[strings.ToLower(table)]
	if t == nil {
		return nil
	}
	return append([]declaredColumn(nil), t.Columns...)
}
