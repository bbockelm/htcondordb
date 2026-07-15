package repl

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
)

// session holds the mutable REPL state the meta-commands change: the current
// output target and serialization format.
type session struct {
	exec     *Executor
	base     io.Writer // the original output (restored by `.output stdout`)
	out      io.Writer // current output target
	outFile  *os.File  // non-nil when output is redirected to a file
	outPath  string
	format   Format
	table    string   // current table for meta-commands (.use changes it)
	readLine ReadLine // input source, for interactive prompts (e.g. .suggest -i)
}

// ReadLine reads one input line (without the trailing newline), returning io.EOF
// at end of input. Implementations own their own prompting and editing: the CLI
// supplies a readline-backed one (arrow-key history, line editing) for an
// interactive terminal, and ScanLines for piped input.
type ReadLine func() (string, error)

// ScanLines returns a ReadLine over r: one line per call, io.EOF at the end, with
// no prompting or editing. Use it for piped/non-interactive input.
func ScanLines(r io.Reader) ReadLine {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	return func() (string, error) {
		if sc.Scan() {
			return sc.Text(), nil
		}
		if err := sc.Err(); err != nil {
			return "", err
		}
		return "", io.EOF
	}
}

// Run drives the read/execute/print loop: it reads statements via readLine and
// writes results to the session output (console by default, redirectable with
// .output), until io.EOF, ctx cancellation, or a quit meta-command. Each
// non-empty line is one statement (an optional trailing ';' is allowed); lines
// beginning with '.' or '\' are meta-commands. Errors are printed to console and
// do not stop the loop.
func Run(ctx context.Context, e *Executor, readLine ReadLine, console io.Writer) error {
	s := &session{exec: e, base: console, out: console, format: FormatTable, table: DefaultTable, readLine: readLine}
	defer s.closeOutput()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		raw, err := readLine()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if isMeta(line) {
			if quit := s.runMeta(console, line); quit {
				return nil
			}
			continue
		}
		res, execErr := e.ExecString(line)
		if execErr != nil {
			fmt.Fprintf(console, "error: %v\n", execErr) // errors go to the console
			if h := HintFor(execErr); h != "" {
				fmt.Fprintf(console, "  hint: %s\n", h)
			}
			continue
		}
		FormatResult(s.out, res, s.format)
	}
}

func isMeta(line string) bool {
	return strings.HasPrefix(line, ".") || strings.HasPrefix(line, "\\")
}

// HintFor returns an actionable hint for common, confusing errors, or "".
func HintFor(err error) string {
	if err == nil {
		return ""
	}
	if strings.Contains(err.Error(), "read-only connection") {
		return "the daemon authorized this connection READ-only, so writes are refused. " +
			"Writing (INSERT/UPDATE/DELETE) needs WRITE authorization: check the daemon's " +
			"ALLOW_WRITE / DENY_WRITE and that your client authenticated (an anonymous client " +
			"typically only gets READ). If the daemon is a leader-follower follower or a " +
			"consistent replica it serves reads only -- direct writes to the leader (or use -consistent)."
	}
	return ""
}

// runMeta handles a meta-command; it returns true if the loop should quit.
// console is where status/help/errors print (always the terminal, even when
// query output is redirected to a file).
func (s *session) runMeta(console io.Writer, line string) bool {
	fields := strings.Fields(line)
	cmd := strings.ToLower(fields[0])
	arg := strings.TrimSpace(strings.TrimPrefix(line, fields[0]))
	switch cmd {
	case ".quit", ".q", "\\q", ".exit":
		return true
	case ".help", "\\h", ".h", "\\?":
		fmt.Fprint(console, helpText)
	case ".format", ".mode":
		s.setFormat(console, arg)
	case ".output", ".out", "\\o":
		s.setOutput(console, arg)
	default:
		if !s.runDiagMeta(console, cmd, arg) {
			fmt.Fprintf(console, "unknown command %q (try .help)\n", cmd)
		}
	}
	return false
}

// setFormat switches the serialization format, or reports it when no arg given.
func (s *session) setFormat(console io.Writer, arg string) {
	if arg == "" {
		fmt.Fprintf(console, "format: %s\n", s.format)
		return
	}
	f, err := ParseFormat(arg)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		return
	}
	s.format = f
	fmt.Fprintf(console, "format: %s\n", s.format)
}

// setOutput redirects query output to a file, or back to the console with
// `.output` / `.output stdout`.
func (s *session) setOutput(console io.Writer, arg string) {
	s.closeOutput()
	if arg == "" || strings.EqualFold(arg, "stdout") || strings.EqualFold(arg, "-") {
		s.out = s.base
		fmt.Fprintln(console, "output: stdout")
		return
	}
	path := stripQuotes(arg)
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		s.out = s.base
		return
	}
	s.outFile = f
	s.outPath = path
	s.out = f
	fmt.Fprintf(console, "output: %s\n", path)
}

// closeOutput closes any open output file and resets to the console.
func (s *session) closeOutput() {
	if s.outFile != nil {
		_ = s.outFile.Close()
		s.outFile = nil
		s.outPath = ""
	}
	s.out = s.base
}

func stripQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (s[0] == '\'' || s[0] == '"') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1]
	}
	return s
}

const helpText = `htcondordb SQL-like shell. Each table is a ClassAd collection (no joins);
a row's primary key lives in the "Key" attribute.

  CREATE TABLE machines;
  CREATE VALUE INDEX ON machines (Cpus);
  SELECT * FROM machines WHERE Cpus >= 8 ORDER BY Cpus DESC LIMIT 10;
  SELECT DISTINCT Owner FROM jobs ORDER BY Owner;
  SELECT Owner, COUNT(*), AVG(Cpus) FROM jobs GROUP BY Owner ORDER BY COUNT(*) DESC;
  INSERT INTO machines (Key, Name, Cpus) VALUES ('slot1', 'slot1@ep', 8);
  UPDATE jobs SET JobStatus = 2 WHERE Owner == "alice";
  DELETE FROM jobs WHERE JobStatus == 4;
  DROP INDEX ON machines (Cpus);   DROP TABLE machines;

  MATCH jobs TO machines WHERE Owner == "alice" WHERE TARGET Arch == "X86_64" LIMIT 10;
  MATCH jobs TO machines USING (RequestCpus, RequestMemory, Requirements, Rank) LIMIT 100;
  MATCH KEY '1.0' IN jobs TO machines;

Notes:
  - WHERE is a ClassAd expression (==, =?=, =!=, undefined, regexp(), ...),
    evaluated by the store; string literals use double quotes.
  - Aggregates: COUNT, SUM, AVG, MIN, MAX, with GROUP BY over one+ columns
    (evaluated server-side); DISTINCT and ORDER BY (ASC/DESC) are supported.
  - JOIN and subqueries are not supported; matchmaking is MATCH, not a join.
  - CREATE INDEX kind is VALUE (numeric+range) or CATEGORICAL (string eq).
  - MATCH <requests> TO <resources>: greedy assignment, one resource per request.
    Each request (in table order) takes its best-ranked bilaterally-matching
    resource not already claimed; the resource is then removed from the pool.
    LIMIT bounds the number of REQUESTS assigned (the first N), not resources per
    request; one row per request (Resource blank when it could not be placed).
    Bare WHERE filters requests; WHERE TARGET filters resources (pushed down).
    USING (attrs) autoclusters identical requests (rank once, reuse; still consumed).
    NOPREEMPT excludes already-claimed resources (State =!= "Claimed").

Meta-commands:
  .help                 show this help
  .tables               list tables (* marks the current one)
  .use <table>          set the current table for the commands below
  .format <mode>        table (default) | json | classad | classad-new
  .output <file>        send query output to a file; .output stdout to restore
  .quit                 exit

Diagnostics (current table, or an explicit one where noted):
  .stats [table]        storage stats (ads, segments, bytes)
  .indexes [table]      configured indexes (+ demand-based suggestions)
  .hot [table]          hot attributes (front-loaded in each ad)
  .suggest [table]      index add/drop suggestions from observed demand
  .suggest -i [table]   review suggestions interactively and apply the accepted ones
  .explain <expr>       how the current table's planner would run a constraint
  .explain MATCH KEY '<k>' IN <req> TO <res>   how matchmaking one request would run

Management (needs WRITE):
  .addindex value|categorical <attr>[, ...]   create an index
  .dropindex <attr>[, ...]                     drop an index
  .reindex                                     rebuild indexes
  .addhot <attr>[, ...]                        pin hot attributes
  .refreshhot [<sampleMax> <topN>]             recompute the hot set
  .rewrite                                     re-encode all ads with the hot set
  .compact                                     reclaim dead space
`
