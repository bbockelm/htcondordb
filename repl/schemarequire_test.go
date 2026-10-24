package repl

import (
	"strings"
	"testing"
)

// TestSchemaRequireEmpty: with nothing required, say so plainly and point at the two ways to
// change it, rather than printing an empty list.
func TestSchemaRequireEmpty(t *testing.T) {
	sess, _, out, cleanup := schemaSession(t, 500)
	defer cleanup()

	sess.schemaCmd(out, "require")
	got := out.String()
	if !strings.Contains(got, "no required schema attributes") {
		t.Errorf("output should say nothing is required:\n%s", got)
	}
	if !strings.Contains(got, "HTCONDORDB_SCHEMA_REQUIRE_ATTRS") {
		t.Errorf("output should name the config knob:\n%s", got)
	}
}

// TestSchemaRequireAddShowDrop walks the whole loop over the wire. The listing has to distinguish
// a required attribute that became a field from one that was refused -- a name that silently does
// nothing is the failure mode the report exists for.
func TestSchemaRequireAddShowDrop(t *testing.T) {
	sess, d, out, cleanup := schemaSession(t, 3000)
	defer cleanup()

	sess.schemaCmd(out, "require add WallClock")
	if got := out.String(); !strings.Contains(got, "schema.rebuild") {
		t.Errorf("add should name the rebuild the change needs:\n%s", got)
	}
	if got := d.SchemaRequireAttrs(); len(got) != 1 || got[0] != "wallclock" {
		t.Fatalf("required set = %v, want [wallclock]", got)
	}

	// A derivation has to run before the attribute can be a field; the listing must be honest
	// about that rather than implying the slot already exists.
	out.Reset()
	sess.schemaCmd(out, "require")
	if got := out.String(); !strings.Contains(got, "WallClock") && !strings.Contains(got, "wallclock") {
		t.Errorf("listing should name the required attribute:\n%s", got)
	}

	if !d.ReschemaScan(4000, 4) {
		t.Skip("no sealed segments to sample")
	}
	out.Reset()
	sess.schemaCmd(out, "require")
	got := out.String()
	if !strings.Contains(got, "because they were required") {
		t.Errorf("listing should separate the required attributes that became fields:\n%s", got)
	}
	if !strings.Contains(got, ".schema rebuild") {
		t.Errorf("listing should say a change needs a rebuild:\n%s", got)
	}

	out.Reset()
	sess.schemaCmd(out, "require drop WallClock")
	if got := d.SchemaRequireAttrs(); len(got) != 0 {
		t.Errorf("required set after drop = %v, want empty", got)
	}
}

// TestSchemaRequireRefusalIsShown: an expression-valued attribute cannot be a column, and the
// listing has to say so. Without it the operator sees a name in the required set, no field in the
// schema, and nothing connecting the two.
func TestSchemaRequireRefusalIsShown(t *testing.T) {
	sess, d, out, cleanup := schemaSession(t, 3000)
	defer cleanup()

	// Nothing in the fixture carries this attribute at all -- the zero-occurrence case.
	sess.schemaCmd(out, "require add NoSuchAttribute")
	if !d.ReschemaScan(4000, 4) {
		t.Skip("no sealed segments to sample")
	}
	out.Reset()
	sess.schemaCmd(out, "require")
	got := out.String()
	if !strings.Contains(got, "refused by the last derivation") {
		t.Fatalf("listing should report the refusal:\n%s", got)
	}
	if !strings.Contains(got, "absent from the sample") {
		t.Errorf("listing should give the reason:\n%s", got)
	}
}

// TestSchemaRequireUsage: `add` with no attribute prints usage, and says the change lands on the
// current table -- the one thing a reader cannot infer, since add/drop deliberately take no
// [table] argument (an attribute name and a table name are indistinguishable there).
func TestSchemaRequireUsage(t *testing.T) {
	sess, _, out, cleanup := schemaSession(t, 100)
	defer cleanup()

	sess.schemaCmd(out, "require add")
	got := out.String()
	if !strings.Contains(got, "usage:") {
		t.Errorf("add with no attribute should print usage:\n%s", got)
	}
	if !strings.Contains(got, "current table") {
		t.Errorf("usage should say which table the change applies to:\n%s", got)
	}
}
