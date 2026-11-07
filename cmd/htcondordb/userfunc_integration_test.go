//go:build unix

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// This is the realistic script user-function integration test: it compiles the actual htcondordb
// daemon AND the actual htcondordb-cli, starts a daemon with ClassAd Script functions configured,
// and asks it questions through the client an operator would use.
//
// WHY THE CLIENT IS PART OF THE TEST. A query is evaluated in two places. A WHERE clause is sent
// to the daemon and runs where the records are; a SELECT item is computed by whatever asked the
// question, over the records that came back. Both sides load user functions from the same
// configuration keys, so a function configured on only one side works in one position and fails in
// the other -- and the person writing the SQL has no way to tell which position they are in. The
// package-level tests in userfunc drive the manager directly and cannot see that split at all,
// because there is only one process in them. These tests have two, and the client's configuration
// is the variable.
//
// WHAT COUNTS AS EVIDENCE. "The query succeeded" is not evidence that a function ran: a query that
// never reached the function succeeds too, and a column of `error` is a successful query. So every
// assertion here is on a VALUE only the configured function could produce -- the string "hot",
// which no arithmetic in the query language yields, and 37.5, which is neither of its arguments.

var (
	cliBuildOnce sync.Once
	cliBinPath   string
	cliBuildErr  error
)

// htcondordbCLIBinary builds the client once, the way htcondordbBinary builds the daemon. The
// client is a separate process on purpose: it is the half of the split these tests exist for.
func htcondordbCLIBinary(t *testing.T) string {
	cliBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "htcondordb-cli-bin")
		if err != nil {
			cliBuildErr = err
			return
		}
		cliBinPath = filepath.Join(dir, "htcondordb-cli")
		out, err := exec.Command("go", "build", "-o", cliBinPath, "../htcondordb-cli").CombinedOutput()
		if err != nil {
			cliBuildErr = fmt.Errorf("building htcondordb-cli: %w\n%s", err, out)
		}
	})
	if cliBuildErr != nil {
		t.Fatalf("%v", cliBuildErr)
	}
	return cliBinPath
}

// testdataPath resolves a fixture to an absolute path, because it is named in a config file the
// daemon reads from its own working directory.
func testdataPath(t *testing.T, name string) string {
	t.Helper()
	p, err := filepath.Abs(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// scriptFunctionConfig is the configuration an administrator writes to register the two defs in
// e2e_policy.cas: one list, and one path per name, both paths naming the same file.
func scriptFunctionConfig(t *testing.T) string {
	t.Helper()
	p := testdataPath(t, "e2e_policy.cas")
	return fmt.Sprintf("HTCONDORDB_SCRIPT_FUNCTIONS = e2eScriptPct e2eScriptBand\n"+
		"HTCONDORDB_SCRIPT_E2ESCRIPTPCT_PATH = %s\n"+
		"HTCONDORDB_SCRIPT_E2ESCRIPTBAND_PATH = %s\n", p, p)
}

// cliResult is what one `htcondordb-cli -e` run produced. Output and exit status are both part of
// what is being established: the client is documented to carry on after a function it cannot load,
// so "what it printed" and "whether it failed" are different questions.
type cliResult struct {
	stdout string
	stderr string
	err    error
}

// runCLI executes one statement against addr with the given CONDOR_CONFIG, in a separate process.
// The config is passed through the environment rather than t.Setenv so that the daemon, the
// client, and the test each read the configuration meant for them.
func runCLI(t *testing.T, bin, cfgPath, addr, stmt string) cliResult {
	t.Helper()
	cmd := exec.Command(bin, "-addr", addr, "-e", stmt)
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+cfgPath)
	var out, errOut strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	return cliResult{stdout: out.String(), stderr: errOut.String(), err: err}
}

// mustCLI runs a statement that has to succeed, and reports both streams when it does not.
func mustCLI(t *testing.T, bin, cfgPath, addr, stmt string) cliResult {
	t.Helper()
	r := runCLI(t, bin, cfgPath, addr, stmt)
	if r.err != nil {
		t.Fatalf("%s\nfailed: %v\nstdout:\n%s\nstderr:\n%s", stmt, r.err, r.stdout, r.stderr)
	}
	return r
}

// seedSlots creates the table these tests query and fills it with four rows chosen so that every
// answer a script function can give is reachable: a value, a value over the band threshold, an
// `error` (division by zero), and an `undefined` (an attribute the record does not carry).
func seedSlots(t *testing.T, bin, cfgPath, addr string) {
	t.Helper()
	for _, stmt := range []string{
		"CREATE TABLE slots",
		`INSERT INTO slots (Key, Machine, Used, Total) VALUES ('a', 'm1', 3, 8)`,
		`INSERT INTO slots (Key, Machine, Used, Total) VALUES ('b', 'm2', 7, 8)`,
		`INSERT INTO slots (Key, Machine, Used, Total) VALUES ('c', 'm3', 5, 0)`,
		`INSERT INTO slots (Key, Machine, Total) VALUES ('d', 'm4', 8)`,
	} {
		mustCLI(t, bin, cfgPath, addr, stmt)
	}
}

// TestScriptUserFunctionBothSidesIntegration establishes that a script function configured on the
// daemon AND on the client works in both halves of a query, with the values a reader can check by
// hand: 3 of 8 is 37.5 percent and bands "cold"; 7 of 8 is 87.5 and bands "hot".
//
// It also establishes the two answers that are not values. A record with no Used attribute reaches
// the function as `undefined` and comes back `undefined` rather than failing the row, and a total
// of zero comes back `error` -- the distinction the function itself is making, carried all the way
// out to a column.
func TestScriptUserFunctionBothSidesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test (builds + forks the daemon and the client)")
	}
	bin := htcondordbBinary(t)
	cli := htcondordbCLIBinary(t)
	id := fsIdentity(t)

	srvDir := t.TempDir()
	addr := startNode(t, bin, srvDir, writeNodeConfig(t, srvDir, id, scriptFunctionConfig(t)))

	// The client gets the same two keys out of the same file, which is the configuration the
	// documentation tells an administrator to put where their clients read it.
	cliDir := t.TempDir()
	cliCfg := writeNodeConfig(t, cliDir, id, scriptFunctionConfig(t))

	seedSlots(t, cli, cliCfg, addr)

	// A SELECT item is computed by the client. Each row's values are the function's and nothing
	// else's: 37.5 is neither 3 nor 8, and "hot" is not a number at all.
	sel := mustCLI(t, cli, cliCfg, addr,
		`SELECT Machine, e2eScriptPct(Used, Total) AS pct, e2eScriptBand(Used, Total) AS band FROM slots ORDER BY Machine`)
	for _, want := range []string{
		"m1      | 37.5      | cold",
		"m2      | 87.5      | hot",
		"m3      | error     | unknown",
		"m4      | undefined | unknown",
	} {
		if !strings.Contains(sel.stdout, want) {
			t.Errorf("SELECT output missing %q; got:\n%s", want, sel.stdout)
		}
	}

	// A WHERE clause runs on the daemon. Only m2 bands "hot", so a single row named m2 is the
	// daemon having evaluated a function the client's own filtering never saw.
	where := mustCLI(t, cli, cliCfg, addr, `SELECT Machine FROM slots WHERE e2eScriptBand(Used, Total) == "hot"`)
	if !strings.Contains(where.stdout, "m2") || strings.Contains(where.stdout, "m1") {
		t.Errorf("WHERE e2eScriptBand(...) == \"hot\" should match m2 alone; got:\n%s", where.stdout)
	}
	if !strings.Contains(where.stdout, "(1 row") {
		t.Errorf("WHERE matched other than one row; got:\n%s", where.stdout)
	}

	// The daemon says which file each name was taken out of, which is what an operator reads to
	// confirm the function they configured is the one running.
	log := daemonLog(t, srvDir)
	for _, want := range []string{"e2eScriptPct", "e2eScriptBand", testdataPath(t, "e2e_policy.cas")} {
		if !strings.Contains(log, want) {
			t.Errorf("daemon log does not name %q; got:\n%s", want, log)
		}
	}
}

// TestScriptUserFunctionServerOnlyIntegration establishes what happens when the configuration is
// on the daemon alone, which is the mistake the documentation warns about: WHERE works, SELECT
// does not, and nothing in between says so.
//
// Both halves of the split are asserted from the SAME daemon, so the difference between them is
// the client's configuration and nothing else.
func TestScriptUserFunctionServerOnlyIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test (builds + forks the daemon and the client)")
	}
	bin := htcondordbBinary(t)
	cli := htcondordbCLIBinary(t)
	id := fsIdentity(t)

	srvDir := t.TempDir()
	addr := startNode(t, bin, srvDir, writeNodeConfig(t, srvDir, id, scriptFunctionConfig(t)))

	// Seeding needs no user function, so it runs with the configured client; the queries below are
	// the ones that use an unconfigured one.
	loaderDir := t.TempDir()
	seedSlots(t, cli, writeNodeConfig(t, loaderDir, id, scriptFunctionConfig(t)), addr)

	// A client that was never told about the function at all.
	bareDir := t.TempDir()
	bareCfg := writeNodeConfig(t, bareDir, id, "")

	// WHERE still works, because the expression is the daemon's to evaluate. m2 alone bands "hot",
	// and this client could not have decided that: it has no such function.
	where := mustCLI(t, cli, bareCfg, addr, `SELECT Machine FROM slots WHERE e2eScriptBand(Used, Total) == "hot"`)
	if !strings.Contains(where.stdout, "m2") || !strings.Contains(where.stdout, "(1 row") {
		t.Errorf("WHERE should still match m2 alone with the function on the daemon only; got:\n%s", where.stdout)
	}

	// SELECT does not. Every row answers `error`, including the two that have a perfectly good
	// answer on the daemon -- that is the asymmetry, and the query still exits 0.
	sel := mustCLI(t, cli, bareCfg, addr, `SELECT Machine, e2eScriptPct(Used, Total) AS pct FROM slots ORDER BY Machine`)
	if strings.Contains(sel.stdout, "37.5") || strings.Contains(sel.stdout, "87.5") {
		t.Errorf("SELECT computed a value without the function configured on the client; got:\n%s", sel.stdout)
	}
	for _, want := range []string{"m1      | error", "m2      | error"} {
		if !strings.Contains(sel.stdout, want) {
			t.Errorf("SELECT output missing %q; got:\n%s", want, sel.stdout)
		}
	}
	// Nothing on either stream names the function. A client with no configuration for it has
	// nothing to fail to load, so there is no warning to print -- the column of `error` is the
	// whole of the diagnosis available to whoever ran the query.
	if strings.Contains(sel.stderr, "e2eScriptPct") {
		t.Errorf("unconfigured client named the function on stderr; got:\n%s", sel.stderr)
	}

	// A client that WAS told about the function but cannot read the file is the case the
	// documentation describes: it warns and carries on.
	brokenDir := t.TempDir()
	missing := filepath.Join(brokenDir, "absent.cas")
	brokenCfg := writeNodeConfig(t, brokenDir, id, fmt.Sprintf(
		"HTCONDORDB_SCRIPT_FUNCTIONS = e2eScriptPct\nHTCONDORDB_SCRIPT_E2ESCRIPTPCT_PATH = %s\n", missing))

	// Carries on: a query that does not call the function is unaffected, and the tool exits 0.
	unaffected := mustCLI(t, cli, brokenCfg, addr, `SELECT Machine FROM slots ORDER BY Machine`)
	if !strings.Contains(unaffected.stdout, "m1") || !strings.Contains(unaffected.stdout, "(4 rows") {
		t.Errorf("a client that could not load a function should still answer queries that do not call it; got:\n%s", unaffected.stdout)
	}
	// Warns: the function and the file it could not read, so the person reading the warning knows
	// which path to go and fix.
	for _, want := range []string{"e2eScriptPct", missing} {
		if !strings.Contains(unaffected.stderr, want) {
			t.Errorf("client warning does not name %q; got:\n%s", want, unaffected.stderr)
		}
	}
}

// TestScriptUserFunctionReadingAdFailsStartupIntegration establishes that a script function
// reaching the subject ad stops the daemon rather than being registered.
//
// This is the behaviour worth a whole process to prove. A function that cannot be registered and
// is configured anyway would answer `error` for every call for the life of the daemon, and a
// constraint that is `error` rejects the record -- answers that are quietly wrong, served by a
// daemon that looks healthy. So the check is not only that the message is right but that the
// daemon EXITS, and never reaches the point of advertising an address for anyone to connect to.
func TestScriptUserFunctionReadingAdFailsStartupIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test (builds + forks the daemon)")
	}
	bin := htcondordbBinary(t)
	id := fsIdentity(t)

	dir := t.TempDir()
	fixture := testdataPath(t, "e2e_readsad.cas")
	cfg := writeNodeConfig(t, dir, id, fmt.Sprintf(
		"HTCONDORDB_SCRIPT_FUNCTIONS = e2eScriptReadsAd\nHTCONDORDB_SCRIPT_E2ESCRIPTREADSAD_PATH = %s\n", fixture))

	// Bounded, because the failure this test is looking for is a daemon that keeps RUNNING: left
	// unbounded, a regression here would hang the run rather than fail it, and a hung test says
	// nothing to whoever has to read the result.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "-listen", "127.0.0.1:0")
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+cfg)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("daemon was still running after 30s with a script function that reads the ad; output:\n%s", out)
	}
	if err == nil {
		t.Fatalf("daemon started with a script function that reads the ad; output:\n%s", out)
	}
	if code := cmd.ProcessState.ExitCode(); code <= 0 {
		t.Errorf("daemon exit code = %d, want non-zero", code)
	}

	// The operator-visible message names the function, the file it came out of, and the attribute
	// that made it unregisterable -- which together are the whole fix.
	stderr := string(out)
	for _, want := range []string{"e2eScriptReadsAd", fixture, "Memory"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("start-up failure does not name %q; got:\n%s", want, stderr)
		}
	}

	// The same failure reaches the daemon log, which is where it is read when the daemon was
	// started by condor_master and nobody saw stderr.
	if log := daemonLog(t, dir); !strings.Contains(log, "e2eScriptReadsAd") {
		t.Errorf("daemon log does not name the refused function; got:\n%s", log)
	}

	// Nothing is served: the address file is the daemon saying it is ready to be connected to.
	if b, rerr := os.ReadFile(filepath.Join(dir, "addr")); rerr == nil && len(strings.TrimSpace(string(b))) > 0 {
		t.Errorf("daemon advertised an address %q despite refusing to start", strings.TrimSpace(string(b)))
	}
}

// daemonLog returns everything the daemon wrote to its log directory, joined. The file name is the
// daemon's to choose, so the test reads the directory rather than guessing it.
func daemonLog(t *testing.T, dir string) string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "log", "*"))
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, f := range files {
		if content, rerr := os.ReadFile(f); rerr == nil {
			b.Write(content)
		}
	}
	return b.String()
}
