package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/arrowstream"
	"github.com/bbockelm/htcondordb/arrowtype"
)

// `arrow`: write a table out as an Arrow IPC stream.
//
// This is a TABLE export, not a query export, and the distinction is deliberate. The Arrow
// schema is a declaration about a table -- derived from a sample of the whole of it -- so that
// a consumer can cache it. Deriving it from one query's matches instead would make the schema
// vary by query and the cache worthless. A projection or an aggregate is a different shape and
// belongs to the SQL surface, not here.
//
// The output is what DuckDB reads with read_arrow(), and what pyarrow reads with
// ipc.open_stream -- from there pandas, polars and Parquet are a step away.

// knownTimeAttrs are the HTCondor attributes stored as unix-epoch seconds. Declaring them
// makes those columns timestamps, so a consumer's date functions work on them.
//
// It is a DECLARATION, seeded here and overridable, not a guess from the values: an integer
// column is not a timestamp merely because its values happen to look like one. The list
// matches the one the Grafana plugin carries, which is the same knowledge written down twice
// -- worth unifying once there is a stored type map to hold it.
var knownTimeAttrs = map[string]bool{
	"qdate": true, "jobstartdate": true, "jobcurrentstartdate": true,
	"enteredcurrentstatus": true, "completiondate": true, "lastheardfrom": true,
	"daemonstarttime": true, "lastmatchtime": true, "joblaststartdate": true,
	"shadowbday": true, "enteredcurrentactivity": true, "mycurrenttime": true,
	"lastbenchmark": true,
}

func runArrow(ctx context.Context, cfg *config.Config, addr string, fs *flags) error {
	dbc, closeConn, err := connectDB(ctx, cfg, addr)
	if err != nil {
		return err
	}
	defer closeConn()

	table := fs.loadTable
	if table == "" {
		table = dbrpc.DefaultTable
	}

	out := io.Writer(os.Stdout)
	if fs.arrowOut != "" {
		f, err := os.Create(fs.arrowOut)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	} else if isTerminal(os.Stdout) {
		// Arrow IPC is binary. Dumping it into a terminal corrupts the display and
		// teaches nothing, so say what to do instead.
		return fmt.Errorf("arrow output is binary: redirect it or pass -o <file>")
	}

	opts := arrowstream.Options{
		Table:      table,
		Constraint: fs.arrowWhere,
		Limit:      fs.arrowLimit,
		Derive:     arrowtype.Options{TimeAttrs: knownTimeAttrs},
	}
	res, err := arrowstream.Export(ctx, dbc, out, opts)
	if err != nil {
		return err
	}
	// The report goes to stderr so it cannot corrupt a stream on stdout.
	writeArrowReport(os.Stderr, table, res)
	return nil
}

// writeArrowReport says what the export produced and, more usefully, what it could not. An
// export that silently dropped a third of a table's attributes looks exactly like one that did
// not, so the attributes that got no column are named rather than counted, and any value that
// had to be altered or nulled is reported per column.
func writeArrowReport(w io.Writer, table string, res *arrowstream.Result) {
	fmt.Fprintf(w, "%s: %d rows in %d batches, %d columns\n",
		table, res.Rows, res.Batches, res.Schema.NumFields())
	if res.Unparsed > 0 {
		fmt.Fprintf(w, "  WARNING: %d rows could not be parsed and are missing from the stream\n", res.Unparsed)
	}

	if untyped := res.Untyped(); len(untyped) > 0 {
		byReason := map[string][]string{}
		for name, reason := range untyped {
			byReason[reason] = append(byReason[reason], name)
		}
		reasons := make([]string, 0, len(byReason))
		for r := range byReason {
			reasons = append(reasons, r)
		}
		sort.Strings(reasons)
		fmt.Fprintf(w, "  %d attributes have no column:\n", len(untyped))
		for _, r := range reasons {
			names := byReason[r]
			sort.Strings(names)
			fmt.Fprintf(w, "    %-20s %s\n", r, strings.Join(names, " "))
		}
	}

	for _, st := range res.Stats {
		lossy := st.CoercedLossy
		var nulled int
		for cause, n := range st.Nulls {
			if cause == arrowtype.CauseUncoercible {
				nulled += n
			}
		}
		if lossy == 0 && nulled == 0 {
			continue
		}
		fmt.Fprintf(w, "  %s:", st.Name)
		if lossy > 0 {
			fmt.Fprintf(w, " %d values altered to fit", lossy)
		}
		if nulled > 0 {
			fmt.Fprintf(w, " %d values nulled (wrong kind for the column)", nulled)
		}
		fmt.Fprintln(w)
	}
}

// isTerminal reports whether f is a character device, so a binary stream is not dumped into a
// terminal. A stat failure is treated as "not a terminal": refusing to write when we cannot
// tell would be worse than writing to a pipe we misjudged.
func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
