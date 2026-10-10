package main

// The fsck subcommand: inspect a database's files on disk and report what is intact,
// what is damaged, and what a repair could recover.
//
// Unlike every other subcommand this one does NOT talk to a daemon. It reads the
// files directly, because the situation it exists for is the one where the daemon
// will not start -- and because opening a damaged store rewrites it as it recovers
// (rebuilding directories, pruning dictionaries, reindexing), which destroys the
// evidence an operator needs to decide what to do.
//
// That makes it the operator's responsibility to run it against a database nothing
// else has open. A running daemon is appending, so a report taken underneath one
// describes a moment that has already passed; the command says so rather than trying
// to detect it.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/PelicanPlatform/classad/collections"

	"github.com/bbockelm/htcondordb/dbdir"
)

// errDamageFound makes `fsck` exit non-zero when a table is damaged, without the
// message reading like the command failed to run.
var errDamageFound = errors.New("fsck: damage found")

// fsckTable is one table's census.
type fsckTable struct {
	Name   string // "jobs", "archives/history", ...
	Dir    string
	Report *collections.FsckReport
	Err    error
}

// runFsck inspects every table under the database directory, or just the one named.
// It returns the number of tables found to be damaged, so the caller can exit
// non-zero and make this usable from monitoring.
func runFsck(out io.Writer, dir, only string) (damaged int, err error) {
	if strings.TrimSpace(dir) == "" {
		return 0, fmt.Errorf("no database directory: pass -dir, or set HTCONDORDB_DIR or SPOOL")
	}
	tables, err := findTables(dir, only)
	if err != nil {
		return 0, err
	}
	if len(tables) == 0 {
		if only != "" {
			return 0, fmt.Errorf("no table named %q under %s", only, dir)
		}
		return 0, fmt.Errorf("no tables found under %s (is that the database directory?)", dir)
	}

	fmt.Fprintf(out, "fsck %s\n%d table(s)\n\n", dir, len(tables))
	var totalRecords, totalLost, totalStrays int
	for i := range tables {
		t := &tables[i]
		t.Report, t.Err = collections.Fsck(t.Dir)
		if t.Err != nil {
			fmt.Fprintf(out, "%s: CANNOT READ: %v\n\n", t.Name, t.Err)
			damaged++
			continue
		}
		rec, runs, lost := t.Report.Totals()
		totalRecords += rec
		totalLost += lost
		totalStrays += len(t.Report.Strays)

		bad := runs > 0 || lost > 0 || len(t.Report.Strays) > 0 || len(t.Report.DictsMissing) > 0
		if bad {
			damaged++
		}
		status := "ok"
		if bad {
			status = "DAMAGED"
		}
		fmt.Fprintf(out, "%s: %s -- %d segment(s), %d record(s) readable\n",
			t.Name, status, len(t.Report.Segments), rec)
		if bad {
			// Indent the detailed report under the table it belongs to.
			for _, line := range strings.Split(strings.TrimRight(t.Report.String(), "\n"), "\n") {
				// Skip the two lines the per-table header above already carries.
				if strings.HasPrefix(line, "fsck ") || strings.Contains(line, "record(s) readable") {
					continue
				}
				fmt.Fprintf(out, "  %s\n", line)
			}
		}
		fmt.Fprintln(out)
	}

	fmt.Fprintf(out, "summary: %d record(s) readable across %d table(s)\n", totalRecords, len(tables))
	switch {
	case damaged == 0:
		fmt.Fprintln(out, "no damage found")
	default:
		fmt.Fprintf(out, "%d table(s) damaged: %d unreadable record(s), %d ignored file(s)\n",
			damaged, totalLost, totalStrays)
		fmt.Fprintln(out, "nothing was modified; fsck only reads")
	}
	return damaged, nil
}

// findTables lists the collection directories under a database directory: one per
// table, plus one per append-only archive. An archive is reported with its subdirectory
// in the name so "history" under archives/ is never confused with a table of that name.
func findTables(dir, only string) ([]fsckTable, error) {
	var out []fsckTable
	for _, sub := range []string{dbdir.Tables, dbdir.Archives} {
		root := filepath.Join(dir, sub)
		entries, err := os.ReadDir(root)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", root, err)
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			qualified := name
			if sub == dbdir.Archives {
				qualified = dbdir.Archives + "/" + name
			}
			if only != "" && only != name && only != qualified {
				continue
			}
			out = append(out, fsckTable{Name: qualified, Dir: filepath.Join(root, name)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
