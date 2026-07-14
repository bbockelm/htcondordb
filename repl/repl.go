package repl

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
)

// Run reads statements from in and writes results to out until EOF, ctx is
// cancelled, or a quit meta-command. Each non-empty line is one statement
// (an optional trailing ';' is allowed). Lines beginning with '.' or '\' are
// meta-commands (.help, .quit). Errors are printed and do not stop the loop.
//
// prompt, if non-empty, is written before each read when interactive.
func Run(ctx context.Context, e *Executor, in io.Reader, out io.Writer, prompt string) error {
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if prompt != "" {
			fmt.Fprint(out, prompt)
		}
		if !sc.Scan() {
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if isMeta(line) {
			if quit := runMeta(out, line); quit {
				return nil
			}
			continue
		}
		res, err := e.ExecString(line)
		if err != nil {
			fmt.Fprintf(out, "error: %v\n", err)
			continue
		}
		FormatResult(out, res)
	}
	return sc.Err()
}

func isMeta(line string) bool {
	return strings.HasPrefix(line, ".") || strings.HasPrefix(line, "\\")
}

// runMeta handles a meta-command; it returns true if the loop should quit.
func runMeta(out io.Writer, line string) bool {
	cmd := strings.ToLower(strings.Fields(line)[0])
	switch cmd {
	case ".quit", ".q", "\\q", ".exit":
		return true
	case ".help", "\\h", ".h", "\\?":
		fmt.Fprint(out, helpText)
	default:
		fmt.Fprintf(out, "unknown command %q (try .help)\n", cmd)
	}
	return false
}

const helpText = `htcondordb SQL-like shell. The store is a single ClassAd collection
(no tables to join). Each row's primary key lives in the "Key" attribute.

  SELECT * FROM ads WHERE Cpus >= 8 LIMIT 10;
  SELECT Owner, JobPrio FROM ads WHERE Owner = 'alice';
  SELECT COUNT(*), AVG(Cpus), MAX(Memory) FROM ads WHERE JobStatus = 2;
  INSERT INTO ads (Key, Owner, Cpus) VALUES ('1.0', 'alice', 4);
  UPDATE ads SET JobStatus = 2 WHERE Owner = 'alice';
  DELETE FROM ads WHERE JobStatus = 4;

Notes:
  - WHERE is a ClassAd expression; '=' means equality, AND/OR/NOT work.
  - Aggregates: COUNT, SUM, AVG, MIN, MAX (no GROUP BY).
  - JOIN, GROUP BY, ORDER BY, and subqueries are not supported.

Meta-commands:
  .help    show this help
  .quit    exit
`
