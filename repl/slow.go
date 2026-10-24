package repl

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// slowDefaultTop is how many shapes `.slow` prints when the operator names no count.
const slowDefaultTop = 10

// slow renders the daemon's aggregated query report -- which query SHAPES have cost the
// most, and which actual queries to reproduce:
//
//	.slow [table] [n]     the n costliest shapes (default all tables, slowDefaultTop)
//	.slow reset [table]   clear the window and print what it held (needs DAEMON)
//
// Unlike the other diagnostics this defaults to EVERY table rather than the current one:
// "what are my slowest queries?" is a question about the daemon, and answering it for
// whichever table the session happened to .use would hide the answer about as often as
// not.
//
// Shapes are ordered by TOTAL time, not max. The query worth an operator's attention is
// usually a cheap one run constantly rather than the single slowest observation, and
// ranking by max reliably surfaces the wrong one.
func (s *session) slow(console io.Writer, arg string) {
	toks := strings.Fields(arg)
	reset := false
	if len(toks) > 0 && strings.EqualFold(toks[0], "reset") {
		reset, toks = true, toks[1:]
	}
	table := ""
	if len(toks) > 0 && s.exec.tableExists(toks[0]) {
		table, toks = toks[0], toks[1:]
	}
	top := slowDefaultTop
	if len(toks) > 0 {
		n, err := strconv.Atoi(toks[0])
		if err != nil || n <= 0 {
			// The argument was neither a known table nor a count. Naming it matters: the
			// most likely cause is a table that does not exist, and a bare usage line
			// would leave that looking like a syntax mistake.
			fmt.Fprintf(console, "%q is not a known table or a positive count\n", toks[0])
			fmt.Fprintln(console, "usage: .slow [table] [n]   |   .slow reset [table]")
			return
		}
		top = n
	}

	var rep *dbrpc.QueryStatsReport
	var err error
	if reset {
		rep, err = s.exec.ResetQueryStats(table)
	} else {
		rep, err = s.exec.QueryStats(table)
	}
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		return
	}
	showQueryStats(console, rep, table, top)
	if reset {
		fmt.Fprintln(console, "\nwindow cleared; aggregation restarts from now")
	}
}

// showQueryStats prints one report.
func showQueryStats(w io.Writer, rep *dbrpc.QueryStatsReport, table string, top int) {
	if !rep.Enabled {
		fmt.Fprintln(w, "query aggregation is off on this daemon (HTCONDORDB_QUERY_STATS = false)")
		return
	}
	scope := "all tables"
	if table != "" {
		scope = table
	}
	fmt.Fprintf(w, "query shapes (%s): %d quer%s aggregated over %s, %d shape(s) tracked\n",
		scope, rep.Recorded, plural(rep.Recorded), fmtDur(time.Since(rep.Since)), rep.Tracked)
	if rep.Evicted > 0 {
		// Not a warning: the map is deliberately bounded. But an operator chasing a shape
		// that is no longer listed needs to know it may have been dropped rather than
		// never run.
		fmt.Fprintf(w, "  %d shape(s) dropped to stay inside the cap (the cheapest go first)\n", rep.Evicted)
	}
	if len(rep.Shapes) == 0 {
		fmt.Fprintln(w, "  (no queries recorded yet)")
		return
	}
	fmt.Fprintln(w, "  ordered by TOTAL time -- a cheap query run constantly outranks one slow outlier")
	if !rep.Exemplars {
		// Say why, rather than printing shapes with no examples and leaving the operator
		// to conclude none were kept.
		fmt.Fprintln(w, "  exemplars (the actual constraints and who ran them) need DAEMON authorization")
	}

	for i, sh := range rep.Shapes {
		if i >= top {
			fmt.Fprintf(w, "\n(%d more shape(s); .slow %s %d to see them)\n",
				len(rep.Shapes)-top, table, len(rep.Shapes))
			break
		}
		fmt.Fprintf(w, "\n#%d  %s %s   count %d   total %s   mean %s   max %s   rows %d\n",
			i+1, sh.Op, sh.Table, sh.Count, fmtDur(sh.Total), fmtDur(sh.Mean()), fmtDur(sh.Max), sh.Rows)
		fmt.Fprintf(w, "    where: %s%s\n", sh.Shape, limitNote(sh.Limited))
		fmt.Fprintf(w, "    %s\n", projectionLine(sh))
		fmt.Fprintf(w, "    %s\n", scanLine(sh))
		for _, ex := range sh.Exemplars {
			fmt.Fprintf(w, "    slowest: %s  rows %d%s%s  at %s\n",
				fmtDur(ex.Duration), ex.Rows, exemplarWho(ex), exemplarScan(ex),
				ex.At.Local().Format("15:04:05"))
			fmt.Fprintf(w, "      %s\n", oneLine(ex.Constraint))
		}
	}
	fmt.Fprintln(w, "\nreproduce a shape with the constraint from its exemplar: .explain <constraint>")
}

// projectionLine reports how much of each ad the clients asked for. An unprojected read
// is the chatty pattern worth naming: every attribute of every matched ad crossed the
// wire whether the client needed it or not.
func projectionLine(sh dbrpc.QueryShapeStat) string {
	switch {
	case sh.Unprojected == sh.Count:
		return "projection: none -- every attribute of every matched ad crossed the wire"
	case sh.Unprojected > 0:
		return fmt.Sprintf("projection: up to %d attribute(s); %d of %d instance(s) asked for the whole ad",
			sh.ProjectionMax, sh.Unprojected, sh.Count)
	default:
		return fmt.Sprintf("projection: up to %d attribute(s)", sh.ProjectionMax)
	}
}

// scanLine reports how the scan broke down between segments it had to read and segments
// the zone maps let it skip. That ratio is what separates "slow because the window is
// big" from "slow because nothing pruned"; the read paths that do not thread it report
// nothing rather than reporting zero work, which would read as "pruned everything".
func scanLine(sh dbrpc.QueryShapeStat) string {
	if sh.ScanSamples == 0 {
		return "segments: not measured on this read path"
	}
	total := sh.SegmentsScanned + sh.SegmentsPruned
	pct := 0.0
	if total > 0 {
		pct = 100 * float64(sh.SegmentsPruned) / float64(total)
	}
	// Reported as counts and a ratio, with no verdict attached: whether 0% skipped is a
	// problem depends on whether the table has zone maps on the filtered attribute at
	// all, which this report cannot see. The ratio next to the duration is the signal.
	return fmt.Sprintf("segments: %d scanned, %d pruned (%.0f%% skipped) over %d measured instance(s)",
		sh.SegmentsScanned, sh.SegmentsPruned, pct, sh.ScanSamples)
}

func exemplarWho(ex dbrpc.QueryExemplar) string {
	if ex.User == "" {
		return ""
	}
	return "  user " + ex.User
}

func exemplarScan(ex dbrpc.QueryExemplar) string {
	if ex.Scan.SegmentsTotal == 0 {
		return ""
	}
	return fmt.Sprintf("  segments %d/%d scanned", ex.Scan.SegmentsScanned, ex.Scan.SegmentsTotal)
}

func limitNote(limited bool) string {
	if limited {
		return "   [with LIMIT]"
	}
	return ""
}

// oneLine collapses a constraint's internal whitespace so a multi-line constraint does
// not break the report's indentation.
func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func plural(n int64) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}
