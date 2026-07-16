package repl

import (
	"fmt"
	"io"
	"strings"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// runDiagMeta handles the diagnostic and index/hot-set management meta-commands.
// It returns false if cmd is not one of them (so the caller can report "unknown").
func (s *session) runDiagMeta(console io.Writer, cmd, arg string) bool {
	switch cmd {
	case ".tables":
		s.showTables(console)
	case ".use":
		s.useTable(console, arg)
	case ".stats":
		s.withDiag(console, s.tableArg(arg), s.showStats)
	case ".indexes", ".index":
		s.withDiag(console, s.tableArg(arg), s.showIndexes)
	case ".hot":
		s.withDiag(console, s.tableArg(arg), s.showHot)
	case ".suggest":
		s.suggest(console, arg)
	case ".explain":
		s.explain(console, arg)
	case ".addindex":
		s.addIndex(console, arg)
	case ".dropindex":
		s.admin(console, "index.drop", splitAttrs(arg)...)
	case ".reindex":
		s.admin(console, "index.reindex")
	case ".addhot":
		s.admin(console, "hot.add", splitAttrs(arg)...)
	case ".refreshhot":
		s.refreshHot(console, arg)
	case ".compact":
		s.admin(console, "compact")
	case ".rewrite":
		s.admin(console, "rewrite")
	default:
		return false
	}
	return true
}

// tableArg returns arg as the target table if given, else the current table.
func (s *session) tableArg(arg string) string {
	if t := strings.TrimSpace(arg); t != "" {
		return t
	}
	return s.table
}

func (s *session) showTables(console io.Writer) {
	names, err := s.exec.Tables()
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		return
	}
	for _, n := range names {
		marker := "  "
		if n == s.table {
			marker = "* " // the current table
		}
		fmt.Fprintf(console, "%s%s\n", marker, n)
	}
}

func (s *session) useTable(console io.Writer, arg string) {
	t := strings.TrimSpace(arg)
	if t == "" {
		fmt.Fprintf(console, "current table: %s\n", s.table)
		return
	}
	s.table = t
	fmt.Fprintf(console, "using table: %s\n", s.table)
}

// withDiag fetches a table's diagnostics and hands them to fn (reporting errors).
func (s *session) withDiag(console io.Writer, table string, fn func(io.Writer, *dbrpc.Diagnostics)) {
	d, err := s.exec.Diagnostics(table)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		return
	}
	fn(console, d)
}

func (s *session) showStats(w io.Writer, d *dbrpc.Diagnostics) {
	st := d.Stats
	fmt.Fprintf(w, "ads:        %d\n", st.Ads)
	fmt.Fprintf(w, "segments:   %d\n", st.Segments)
	fmt.Fprintf(w, "arena:      %s (reserved)\n", humanBytes(st.ArenaBytes))
	fmt.Fprintf(w, "used:       %s\n", humanBytes(st.UsedBytes))
	fmt.Fprintf(w, "live:       %s\n", humanBytes(st.LiveBytes()))
	fmt.Fprintf(w, "dead:       %s (reclaimable by compaction)\n", humanBytes(st.DeadBytes))
}

func (s *session) showIndexes(w io.Writer, d *dbrpc.Diagnostics) {
	fmt.Fprintf(w, "categorical (string eq/membership): %s\n", orNone(d.CategoricalIndexes))
	fmt.Fprintf(w, "value       (numeric + range):      %s\n", orNone(d.ValueIndexes))
	sz := d.IndexSizes
	if len(sz.PerIndex) > 0 {
		fmt.Fprintf(w, "index memory: %s total, %.1f%% of %s live data\n",
			humanBytes(sz.TotalBytes), sz.Frac*100, humanBytes(sz.DataBytes))
		fmt.Fprintln(w, "  attribute                 kind         owner  size        pct-data")
		for _, s := range sz.PerIndex {
			owner := "human"
			if s.Auto {
				owner = "auto"
			}
			fmt.Fprintf(w, "  %-25s %-12s %-6s %-11s %.1f%%\n",
				s.Attr, s.Kind, owner, humanBytes(s.Bytes), s.Frac*100)
		}
	}
	if len(d.Suggestions) > 0 {
		fmt.Fprintln(w, "suggested indexes (by observed demand):")
		printSuggestions(w, d.Suggestions)
	}
}

func (s *session) showHot(w io.Writer, d *dbrpc.Diagnostics) {
	if len(d.Hot) == 0 {
		fmt.Fprintln(w, "hot attributes: (none; run .refreshhot to compute the hot set from a sample)")
		return
	}
	fmt.Fprintf(w, "hot attributes: %s\n", orNone(d.Hot))
}

// suggest shows index suggestions, or -- with a leading -i / apply -- reviews them
// interactively, prompting to accept each and applying the accepted ones. Usage:
// `.suggest [table]` or `.suggest -i [table]`.
func (s *session) suggest(console io.Writer, arg string) {
	fields := strings.Fields(arg)
	interactive := false
	if len(fields) > 0 && (fields[0] == "-i" || strings.EqualFold(fields[0], "apply")) {
		interactive = true
		fields = fields[1:]
	}
	table := s.table
	if len(fields) > 0 {
		table = fields[0]
	}
	if !interactive {
		s.withDiag(console, table, s.showSuggest)
		return
	}
	s.suggestInteractive(console, table)
}

// suggestInteractive prompts to accept each add/drop suggestion and applies the ones
// accepted (via the same index management the daemon exposes). It needs an input
// source; without one it falls back to just printing.
func (s *session) suggestInteractive(console io.Writer, table string) {
	d, err := s.exec.Diagnostics(table)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		return
	}
	if len(d.Suggestions) == 0 && len(d.DropSuggestions) == 0 {
		fmt.Fprintln(console, "no index suggestions (need observed query demand)")
		return
	}
	if s.readLine == nil {
		s.showSuggest(console, d) // no input to prompt on: just show them
		return
	}
	fmt.Fprintf(console, "reviewing suggestions for %q: [y]es apply / [N]o skip / [a]ll / [q]uit\n", table)
	applied, all := 0, false
	for _, sg := range d.Suggestions {
		if !all {
			fmt.Fprintf(console, "  add %s index on %s? (eq=%d range=%d distinct=%d) [y/N/a/q] ",
				sg.Kind, sg.Attr, sg.QueriesEq, sg.QueriesRange, sg.DistinctValues)
			ans, err := s.readLine()
			if err != nil {
				return
			}
			switch strings.ToLower(strings.TrimSpace(ans)) {
			case "q", "quit":
				fmt.Fprintf(console, "%d index(es) applied.\n", applied)
				return
			case "a", "all":
				all = true
			case "y", "yes":
			default:
				continue
			}
		}
		action := "index.add.value"
		if sg.Kind == "categorical" {
			action = "index.add.categorical"
		}
		if msg, err := s.exec.Admin(table, action, sg.Attr); err != nil {
			fmt.Fprintf(console, "    error: %v\n", err)
		} else {
			applied++
			fmt.Fprintf(console, "    %s\n", msg)
		}
	}
	for _, ds := range d.DropSuggestions {
		fmt.Fprintf(console, "  drop the %s index on %s (%s)? [y/N/q] ", ds.Kind, ds.Attr, ds.Reason)
		ans, err := s.readLine()
		if err != nil {
			return
		}
		switch strings.ToLower(strings.TrimSpace(ans)) {
		case "q", "quit":
			fmt.Fprintf(console, "%d change(s) applied.\n", applied)
			return
		case "y", "yes":
			if msg, err := s.exec.Admin(table, "index.drop", ds.Attr); err != nil {
				fmt.Fprintf(console, "    error: %v\n", err)
			} else {
				applied++
				fmt.Fprintf(console, "    %s\n", msg)
			}
		}
	}
	fmt.Fprintf(console, "%d change(s) applied.\n", applied)
}

func (s *session) showSuggest(w io.Writer, d *dbrpc.Diagnostics) {
	if len(d.Suggestions) == 0 {
		fmt.Fprintln(w, "no index suggestions (need observed query demand)")
	} else {
		fmt.Fprintln(w, "add indexes:")
		printSuggestions(w, d.Suggestions)
	}
	for _, ds := range d.DropSuggestions {
		fmt.Fprintf(w, "drop %-20s %-11s  reason: %s\n", ds.Attr, "("+ds.Kind+")", ds.Reason)
	}
}

func printSuggestions(w io.Writer, sugs []db.IndexSuggestion) {
	for _, sg := range sugs {
		fmt.Fprintf(w, "  %-20s %-11s  eq=%d range=%d distinct=%d\n",
			sg.Attr, "("+sg.Kind+")", sg.QueriesEq, sg.QueriesRange, sg.DistinctValues)
	}
}

func (s *session) explain(console io.Writer, arg string) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		fmt.Fprintln(console, "usage: .explain <ClassAd constraint>")
		fmt.Fprintln(console, "       .explain MATCH KEY '<key>' IN <requests> TO <resources>")
		return
	}
	// A matchmaking form: .explain MATCH KEY '1.0' IN jobs TO machines
	if fields := strings.Fields(arg); len(fields) > 0 && strings.EqualFold(fields[0], "MATCH") {
		s.explainMatch(console, arg)
		return
	}
	ex, err := s.exec.Explain(s.table, arg)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		return
	}
	fmt.Fprintf(console, "table:        %s\n", s.table)
	fmt.Fprintf(console, "plan:         %s\n", ex.Plan)
	fmt.Fprintf(console, "wire-native:  %v\n", ex.Native)
	fmt.Fprintf(console, "index-usable: %d of %d probe(s)\n", ex.IndexUsable, len(ex.Probes))
	fmt.Fprintf(console, "parallelism:  %d worker(s) over %d shard(s)\n", ex.Parallelism, ex.Shards)
	fmt.Fprintf(console, "ads:          %d\n", ex.TotalAds)
	if len(ex.Probes) > 0 {
		fmt.Fprintln(console, "probes:")
		for _, p := range ex.Probes {
			kind := p.Kind
			if kind == "" {
				kind = "not indexed"
			}
			state := "scan"
			if p.Indexed {
				state = "INDEX"
			}
			sel := ""
			if p.HasSelectivity {
				// estimated fraction of ads the index visits (lower = more selective)
				sel = fmt.Sprintf("  est ~%.1f%% (~%d of %d)",
					p.Selectivity*100, p.EstCandidates, ex.TotalAds)
			}
			fmt.Fprintf(console, "  %-20s %-4s %-6s (%s)%s\n", p.Attr, p.Op, state, kind, sel)
		}
	}
}

// explainMatch handles `.explain MATCH ... TO ...`: it parses the MATCH statement and
// reports how matchmaking the identified request against the resource table would run
// -- the job's Requirements rewritten over the slot (constants baked in) and which
// probes prune via a resource-table index.
func (s *session) explainMatch(console io.Writer, arg string) {
	st, err := Parse(arg)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		return
	}
	if st.Kind != StmtMatch {
		fmt.Fprintln(console, "usage: .explain MATCH KEY '<key>' IN <requests> TO <resources>")
		return
	}
	ex, err := s.exec.MatchExplain(st)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		return
	}
	fmt.Fprintf(console, "request:       %s\n", st.Table)
	fmt.Fprintf(console, "resource:      %s\n", st.MatchResource)
	if !ex.HasRequirements {
		fmt.Fprintln(console, "the request has no Requirements: every resource is a candidate (full scan)")
	} else {
		fmt.Fprintf(console, "slot predicate: %s\n", ex.SlotPredicate)
	}
	fmt.Fprintf(console, "plan:          %s\n", ex.Plan)
	fmt.Fprintf(console, "index-usable:  %d of %d probe(s)\n", ex.IndexUsable, len(ex.Probes))
	fmt.Fprintf(console, "parallelism:   %d worker(s) over %d shard(s)\n", ex.Parallelism, ex.Shards)
	fmt.Fprintf(console, "resources:     %d\n", ex.TotalResources)
	if len(ex.Probes) > 0 {
		fmt.Fprintln(console, "probes (job Requirements over the slot):")
		for _, p := range ex.Probes {
			kind := p.Kind
			if kind == "" {
				kind = "not indexed"
			}
			state := "scan"
			if p.Indexed {
				state = "INDEX"
			}
			sel := ""
			if p.HasSelectivity {
				sel = fmt.Sprintf("  est ~%.1f%% (~%d of %d)",
					p.Selectivity*100, p.EstCandidates, ex.TotalResources)
			}
			fmt.Fprintf(console, "  %-20s %-4s %-6s (%s)%s\n", p.Attr, p.Op, state, kind, sel)
		}
	}
	if len(ex.EvalOrder) > 0 {
		fmt.Fprintln(console, "evaluation order (bilateral re-verify, short-circuits left to right):")
		for _, ce := range ex.EvalOrder {
			role := "re-check"
			switch {
			case ce.Probed:
				role = "PROBE" // prunes candidates (job Requirements or pushed-down WHERE TARGET)
			case ce.ResourceSide:
				role = "filter" // WHERE TARGET / NOPREEMPT that no index covers: re-checked only
			case ce.Indexed:
				role = "re-check (indexed)" // estimable but not a pushdown probe (e.g. a bool flag)
			}
			sel := ""
			if ce.HasSelectivity {
				sel = fmt.Sprintf("  ~%.1f%% true", ce.TrueFrac*100)
			}
			src := ""
			if ce.ResourceSide {
				src = "  [WHERE TARGET]" // from NOPREEMPT / WHERE TARGET, applied as a post-filter
			}
			fmt.Fprintf(console, "  %-18s %s%s%s\n", role, ce.Text, sel, src)
		}
	}
}

func (s *session) addIndex(console io.Writer, arg string) {
	fields := strings.Fields(arg)
	if len(fields) < 2 {
		fmt.Fprintln(console, "usage: .addindex value|categorical <attr>[, <attr>...]")
		return
	}
	var action string
	switch strings.ToLower(fields[0]) {
	case "value", "val", "v":
		action = "index.add.value"
	case "categorical", "cat", "c":
		action = "index.add.categorical"
	default:
		fmt.Fprintf(console, "unknown index kind %q (want value or categorical)\n", fields[0])
		return
	}
	attrs := splitAttrs(strings.TrimSpace(strings.TrimPrefix(arg, fields[0])))
	s.admin(console, action, attrs...)
}

func (s *session) refreshHot(console io.Writer, arg string) {
	sampleMax, topN := "2000", "32"
	if f := strings.Fields(arg); len(f) == 2 {
		sampleMax, topN = f[0], f[1]
	} else if len(f) != 0 {
		fmt.Fprintln(console, "usage: .refreshhot [<sampleMax> <topN>]")
		return
	}
	s.admin(console, "hot.refresh", sampleMax, topN)
}

// admin runs a management action on the current table and prints the result.
func (s *session) admin(console io.Writer, action string, args ...string) {
	msg, err := s.exec.Admin(s.table, action, args...)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		if h := HintFor(err); h != "" {
			fmt.Fprintf(console, "  hint: %s\n", h)
		}
		return
	}
	fmt.Fprintln(console, msg)
}

// splitAttrs splits a comma/space-separated attribute list, dropping empties.
func splitAttrs(s string) []string {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t'
	})
	out := fields[:0]
	for _, f := range fields {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

func orNone(ss []string) string {
	if len(ss) == 0 {
		return "(none)"
	}
	return strings.Join(ss, ", ")
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
