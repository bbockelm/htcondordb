package repl

import "testing"

// starValue reads one attribute's cell out of a SELECT * result.
func starValue(t *testing.T, e *Executor, attr string) string {
	t.Helper()
	r := mustExec(t, e, "SELECT * FROM ads")
	for i, c := range r.Columns {
		if c == attr {
			return r.Rows[0][i]
		}
	}
	t.Fatalf("SELECT * has no column %q", attr)
	return ""
}

// A narrow SELECT of an expression-valued attribute must agree with SELECT *: the
// projection has to carry the siblings the expression reads, or it evaluates to undefined.
//
// Both representations are covered because reference chasing was for a while a no-op on
// inline-name (persistent) collections, whose expressions reference attributes by name
// rather than id (classad #135) -- and a daemon runs a persistent store, so an
// in-memory-only test passed while the real thing was broken.
func TestProjectedSelectAgreesWithStar(t *testing.T) {
	for _, tc := range []struct {
		mode    string
		newExec func(*testing.T) (*Executor, func())
	}{
		{"memory", newTestExec},
		{"persistent", newPersistentExec},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			e, cleanup := tc.newExec(t)
			defer cleanup()

			mustExec(t, e, "INSERT INTO ads (Key, Memory, Req) VALUES ('big', 2048, Memory > 1024)")
			want := starValue(t, e, "Req")
			got := mustExec(t, e, "SELECT Req FROM ads").Rows[0][0]
			if got != want {
				t.Errorf("SELECT Req = %q but SELECT * reports %q", got, want)
			}
		})
	}
}

// Selecting an expression's dependency alongside it works on every store -- the documented
// workaround while the persistent gap stands.
func TestProjectionWithDependencyAgreesEverywhere(t *testing.T) {
	for _, tc := range []struct {
		mode    string
		newExec func(*testing.T) (*Executor, func())
	}{
		{"memory", newTestExec},
		{"persistent", newPersistentExec},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			e, cleanup := tc.newExec(t)
			defer cleanup()

			mustExec(t, e, "INSERT INTO ads (Key, Memory, Req) VALUES ('big', 2048, Memory > 1024)")
			want := starValue(t, e, "Req")
			r := mustExec(t, e, "SELECT Memory, Req FROM ads")
			if got := r.Rows[0][1]; got != want {
				t.Errorf("SELECT Memory, Req gave Req=%q but SELECT * reports %q", got, want)
			}
		})
	}
}
