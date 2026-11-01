package main

import (
	"os"
	"strings"
	"testing"

	"github.com/bbockelm/htcondordb/repl"
)

// TestParseFlagsDialect: `.dialect` is interactive, so without this flag the ANSI dialect could
// not be reached from a script -- and checking what an ADBC client will see is exactly the kind
// of thing one scripts.
func TestParseFlagsDialect(t *testing.T) {
	saved := os.Args
	defer func() { os.Args = saved }()

	os.Args = []string{"htcondordb-cli", "-dialect", "ansi", "-e", "SELECT 1"}
	f := parseFlags()
	if f.dialect != "ansi" {
		t.Fatalf("dialect = %q, want ansi", f.dialect)
	}
	// The statement must survive: a flag that swallowed an argument would leave it in args
	// and run it as a bare statement, or not at all.
	if f.stmt != "SELECT 1" || len(f.args) != 0 {
		t.Fatalf("stmt = %q, args = %v", f.stmt, f.args)
	}

	// Absent means unset, which is what leaves the executor on its default.
	os.Args = []string{"htcondordb-cli", "-e", "SELECT 1"}
	if f := parseFlags(); f.dialect != "" {
		t.Errorf("dialect = %q with no flag, want empty", f.dialect)
	}

	// The value is parsed by the same function the meta-command uses, so a bad one is
	// rejected rather than silently falling back to the default.
	if _, err := repl.ParseDialect("nonsense"); err == nil {
		t.Error("ParseDialect accepted a bad dialect, so -dialect would too")
	}
}

// TestDialectIsInTheUsage: a flag nobody can discover is a flag nobody uses.
func TestDialectIsInTheUsage(t *testing.T) {
	if !strings.Contains(usageText, "-dialect") {
		t.Error("-dialect is missing from the usage text")
	}
	if !strings.Contains(usageText, ".dialect") {
		t.Error("the meta-command list does not mention .dialect")
	}
}

// TestApplyDialectFlag checks the wiring from the flag to the executor, which is a separate
// thing from parsing it. A flag that lands in the struct and is never applied is a silent
// no-op: it looks like it worked, every query keeps its old meaning, and nothing says so.
func TestApplyDialectFlag(t *testing.T) {
	// SetDialect and Dialect do not touch the client, so a nil one is enough here.
	exec := repl.NewExecutor(nil, repl.ExecConfig{})
	if exec.Dialect() != repl.DialectClassAd {
		t.Fatalf("a fresh executor is %v, want classad", exec.Dialect())
	}

	if err := applyDialectFlag(exec, "ansi"); err != nil {
		t.Fatalf("applying -dialect ansi: %v", err)
	}
	if exec.Dialect() != repl.DialectANSI {
		t.Error("-dialect ansi did not reach the executor")
	}

	if err := applyDialectFlag(exec, "classad"); err != nil {
		t.Fatal(err)
	}
	if exec.Dialect() != repl.DialectClassAd {
		t.Error("-dialect classad did not reach the executor")
	}

	// An empty name leaves whatever was there, rather than resetting it.
	exec.SetDialect(repl.DialectANSI)
	if err := applyDialectFlag(exec, ""); err != nil {
		t.Fatal(err)
	}
	if exec.Dialect() != repl.DialectANSI {
		t.Error("an absent -dialect changed the dialect")
	}

	// A bad value is an error, not a silent fall back to the default -- which would run
	// every query in a dialect the user did not ask for.
	if err := applyDialectFlag(exec, "nonsense"); err == nil {
		t.Error("a bad -dialect was accepted")
	}
}
