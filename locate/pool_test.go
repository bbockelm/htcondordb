package locate

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/golang-htcondor/config"
)

// ad builds an HTCondorDB ad the way the daemon advertises one: a Name, the Machine it runs
// on, and the command address a client dials.
func ad(t *testing.T, name, machine, address string) *classad.ClassAd {
	t.Helper()
	a := classad.New()
	if name != "" {
		a.InsertAttrString("Name", name)
	}
	if machine != "" {
		a.InsertAttrString("Machine", machine)
	}
	if address != "" {
		a.InsertAttrString("MyAddress", address)
	}
	return a
}

// matches evaluates a constraint against an ad the way the collector does when it filters a
// query. Testing the constraint's effect rather than its text is the point: the spelling can
// change freely, but "-name db.example.com finds the daemon there" must not.
func matches(t *testing.T, constraint string, a *classad.ClassAd) bool {
	t.Helper()
	if constraint == "" {
		return true // no constraint: the collector returns every ad of the type
	}
	expr, err := classad.ParseExpr(constraint)
	if err != nil {
		t.Fatalf("constraint %q does not parse: %v", constraint, err)
	}
	v := a.EvaluateExprWithTarget(expr, nil)
	if !v.IsBool() {
		// undefined is how a ClassAd says "this ad lacks the attribute"; the collector
		// treats that as no match.
		return false
	}
	b, err := v.BoolValue()
	if err != nil {
		t.Fatalf("constraint %q evaluated to a non-boolean: %v", constraint, err)
	}
	return b
}

func TestPoolConstraintSelectsTheNamedDaemon(t *testing.T) {
	// A default-named daemon (no HTCONDORDB_NAME) and one an operator named.
	defaultNamed := ad(t, "htcondordb@db.example.com", "db.example.com", "<10.0.0.1:9618>")
	operatorNamed := ad(t, "archive", "db2.example.com", "<10.0.0.2:9618>")

	for _, tc := range []struct {
		name         string // what the user passes as -name
		wantDefault  bool
		wantOperator bool
	}{
		// The host alone is what a user types by analogy with condor_status -name, and it
		// must find the daemon whose Name is "htcondordb@<host>".
		{name: "db.example.com", wantDefault: true},
		{name: "db2.example.com", wantOperator: true},
		// The advertised Name, verbatim.
		{name: "htcondordb@db.example.com", wantDefault: true},
		{name: "archive", wantOperator: true},
		// name@host, the HTCondor spelling for a named daemon: both halves must agree, so
		// the right name on the wrong host matches nothing.
		{name: "archive@db2.example.com", wantOperator: true},
		{name: "archive@db.example.com"},
		// Nothing in the pool is called this.
		{name: "other.example.com"},
	} {
		c := PoolConstraint(tc.name)
		if got := matches(t, c, defaultNamed); got != tc.wantDefault {
			t.Errorf("-name %q vs the default-named daemon: matched=%v, want %v (constraint %s)",
				tc.name, got, tc.wantDefault, c)
		}
		if got := matches(t, c, operatorNamed); got != tc.wantOperator {
			t.Errorf("-name %q vs the operator-named daemon: matched=%v, want %v (constraint %s)",
				tc.name, got, tc.wantOperator, c)
		}
	}
}

func TestPoolConstraintEmptyNameMatchesEverything(t *testing.T) {
	if c := PoolConstraint("   "); c != "" {
		t.Fatalf("a blank name should not constrain the query, got %q", c)
	}
}

// A name is user input and lands inside a ClassAd expression. An unescaped quote would either
// fail to parse or, worse, change what the constraint selects.
func TestPoolConstraintQuotesTheName(t *testing.T) {
	c := PoolConstraint(`db" || true || "x`)
	victim := ad(t, "htcondordb@db.example.com", "db.example.com", "<10.0.0.1:9618>")
	if matches(t, c, victim) {
		t.Fatalf("an injected name matched an unrelated daemon: %s", c)
	}
}

func TestPickDaemonReturnsTheAddress(t *testing.T) {
	addr, err := pickDaemon([]*classad.ClassAd{
		ad(t, "htcondordb@db.example.com", "db.example.com", " <10.0.0.1:9618?sock=db> "),
	}, "cm.example.com", "db.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if addr != "<10.0.0.1:9618?sock=db>" {
		t.Fatalf("got %q, want the ad's MyAddress, trimmed", addr)
	}
}

func TestPickDaemonNoneAdvertising(t *testing.T) {
	_, err := pickDaemon(nil, "cm.example.com", "db.example.com")
	if err == nil {
		t.Fatal("want an error when the collector knows no such daemon")
	}
	// The message has to say both halves of what was asked, or the user cannot tell a
	// misspelled name from the wrong pool.
	for _, want := range []string{"db.example.com", "cm.example.com"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestPickDaemonAmbiguousListsCandidates(t *testing.T) {
	_, err := pickDaemon([]*classad.ClassAd{
		ad(t, "htcondordb@z.example.com", "z.example.com", "<10.0.0.9:9618>"),
		ad(t, "htcondordb@a.example.com", "a.example.com", "<10.0.0.1:9618>"),
	}, "cm.example.com", "")
	if err == nil {
		t.Fatal("want an error rather than a silently chosen database")
	}
	// Sorted, so the same pool produces the same message however the collector ordered it.
	if !strings.Contains(err.Error(), "htcondordb@a.example.com, htcondordb@z.example.com") {
		t.Fatalf("error should list the candidates in sorted order: %v", err)
	}
}

// An ad under our MyType with no address is not something to dial.
func TestPickDaemonAdWithoutAddress(t *testing.T) {
	_, err := pickDaemon([]*classad.ClassAd{ad(t, "htcondordb@db.example.com", "db.example.com", "")},
		"cm.example.com", "")
	if err == nil {
		t.Fatal("want an error when the ad carries no MyAddress")
	}
	if !strings.Contains(err.Error(), "MyAddress") {
		t.Fatalf("error should name the missing attribute: %v", err)
	}
}

// With no pool named and no COLLECTOR_HOST, there is nothing to ask. That has to be a clear
// error at the start rather than a dial of "" (or of whatever the empty-string collector
// address resolves to) seconds later.
func TestDaemonInPoolWithoutACollector(t *testing.T) {
	// An empty configuration, not the host's: a developer (or a CI image) whose condor
	// configuration names a collector must not turn this into a real query -- or, worse,
	// into a skip that hides the case entirely.
	cfg := config.NewEmpty()
	cfg.Set("COLLECTOR_HOST", "")
	_, err := DaemonInPool(t.Context(), cfg, "", "db.example.com")
	if err == nil {
		t.Fatal("want an error when no collector is named")
	}
	if !strings.Contains(err.Error(), "COLLECTOR_HOST") {
		t.Fatalf("error should name the knob that fixes it: %v", err)
	}
}
