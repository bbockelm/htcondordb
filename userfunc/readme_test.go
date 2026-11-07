package userfunc

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// The README carries a worked script function. This test is what keeps it true: it takes the
// script and the configuration out of the README itself rather than a copy, so an edit that breaks
// the example breaks a test instead of only an administrator's afternoon.
//
// As everywhere else in this package, the function name is used by this test alone -- classad's
// registry is process-wide and has no unregister.

var (
	readmeScriptBlock = regexp.MustCompile("(?s)```\n(# /etc/condor/scripts/policy\\.cas\n.*?)```")
	readmeScriptPath  = regexp.MustCompile(`(?m)^(HTCONDORDB_SCRIPT_MEMEFFICIENCY_PATH\s*=\s*).*$`)
	readmeScriptCfg   = regexp.MustCompile("(?s)```conf\n(HTCONDORDB_SCRIPT_FUNCTIONS.*?)```")
)

func TestReadmeScriptExample(t *testing.T) {
	readme, err := os.ReadFile(filepath.Join("..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}

	script := readmeScriptBlock.FindSubmatch(readme)
	if script == nil {
		t.Fatal("the README no longer holds the policy.cas script block")
	}
	path := filepath.Join(t.TempDir(), "policy.cas")
	if err := os.WriteFile(path, script[1], 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := readmeScriptCfg.FindSubmatch(readme)
	if cfg == nil {
		t.Fatal("the README no longer holds the script configuration block")
	}
	// Only the path moves: the README names a file this test does not have, and nothing else
	// about the two lines is this test's to rewrite.
	body := readmeScriptPath.ReplaceAllString(string(cfg[1]), "${1}"+path)
	if !strings.Contains(body, path) {
		t.Fatalf("the configuration block does not set MEMEFFICIENCY_PATH:\n%s", body)
	}

	m := newWasmMgr(t)
	if err := m.Apply(wasmCfg(t, body)); err != nil {
		t.Fatalf("apply the README's configuration: %v", err)
	}

	// The ad is picked so the answer is checkable by eye: 256 of 2048 MB is an eighth, which is
	// under the threshold the README's WHERE clause uses.
	ad, err := classad.Parse(`[ Owner = "alice"; RequestMemory = 2048; MemoryUsage = 256 ]`)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ src, want string }{
		{`memEfficiency(RequestMemory, MemoryUsage)`, "12.5"},
		{`memEfficiency(RequestMemory, MemoryUsage) < 25`, "true"},
		{`memEfficiency(2048, 512)`, "25"}, // a quarter, and not under the threshold
		{`memEfficiency(2048, 512) < 25`, "false"},
		// The guards the README shows, in the order it shows them.
		{`memEfficiency(RequestMemory, Absent)`, "undefined"},
		{`memEfficiency(0, MemoryUsage)`, "error"},
	} {
		expr, perr := classad.ParseExpr(c.src)
		if perr != nil {
			t.Fatalf("parse %s: %v", c.src, perr)
		}
		if got := expr.Eval(ad).String(); got != c.want {
			t.Errorf("%s = %s, want %s", c.src, got, c.want)
		}
	}
}
