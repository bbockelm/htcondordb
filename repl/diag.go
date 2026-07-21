package repl

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/collections"
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
		s.maintenance(console, arg, "index.reindex")
	case ".addhot":
		s.admin(console, "hot.add", splitAttrs(arg)...)
	case ".refreshhot":
		s.refreshHot(console, arg)
	case ".compact":
		s.maintenance(console, arg, "compact")
	case ".rewrite":
		s.maintenance(console, arg, "rewrite")
	case ".retrain":
		s.maintenance(console, arg, "codec.retrain")
	case ".memory":
		s.convertToMemory(console, arg)
	case ".views":
		s.showViews(console)
	case ".export":
		s.exportViews(console, arg)
	case ".timetravel":
		s.timeTravel(console, arg)
	default:
		return false
	}
	return true
}

// timeTravel handles ".timetravel on <window> [checkpoint] | off" for the current
// table, dispatching to the DAEMON-gated timetravel.* admin actions. Durations are Go
// duration strings (e.g. 168h, 1m); enabling is not retroactive.
func (s *session) timeTravel(console io.Writer, arg string) {
	fields := strings.Fields(arg)
	if len(fields) == 0 {
		fmt.Fprintln(console, "usage: .timetravel on <window> [checkpoint] | off   (e.g. .timetravel on 168h 1m)")
		return
	}
	secs := func(d time.Duration) string { return fmt.Sprintf("%d", int(d/time.Second)) }
	switch strings.ToLower(fields[0]) {
	case "off", "disable":
		s.admin(console, "timetravel.disable")
	case "on", "enable":
		if len(fields) < 2 {
			fmt.Fprintln(console, "usage: .timetravel on <window> [checkpoint]  (e.g. .timetravel on 168h 1m)")
			return
		}
		maxD, err := time.ParseDuration(fields[1])
		if err != nil || maxD <= 0 {
			fmt.Fprintf(console, "error: bad window %q (use a duration like 168h)\n", fields[1])
			return
		}
		args := []string{secs(maxD)}
		if len(fields) > 2 {
			ckpt, err := time.ParseDuration(fields[2])
			if err != nil || ckpt < 0 {
				fmt.Fprintf(console, "error: bad checkpoint interval %q\n", fields[2])
				return
			}
			args = append(args, secs(ckpt))
		}
		s.admin(console, "timetravel.enable", args...)
	default:
		fmt.Fprintf(console, "unknown .timetravel subcommand %q (use: on <window> [checkpoint] | off)\n", fields[0])
	}
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
	cs := d.Codec
	retrain := "never (compression not retrained this run)"
	if !cs.LastRetrain.IsZero() {
		retrain = cs.LastRetrain.Format("2006-01-02 15:04:05")
	}
	fmt.Fprintf(w, "codec:      %s", cs.Codec)
	if cs.DictBytes > 0 {
		fmt.Fprintf(w, " (dict %s)", humanBytes(cs.DictBytes))
	}
	if cs.SampleRecords > 0 {
		fmt.Fprintf(w, ", %.2fx compression (sampled %d recs)", cs.Ratio, cs.SampleRecords)
	}
	fmt.Fprintf(w, "\nretrained:  %s\n", retrain)
	if cs.Codec == "identity" {
		fmt.Fprintln(w, "  (no compression configured; enable ZSTD or run .retrain to train a dictionary)")
	}
	showOpStats(w, d.OpStats)
}

// showOpStats prints the cumulative operational timing counters -- where the store
// spent time blocked in, or holding, each stall point -- so an operator can see what
// is "blocking the world" (long shard-write holds, slow syncs, expensive maintenance).
func showOpStats(w io.Writer, o db.OpStats) {
	rows := []struct {
		label string
		s     db.OpStat
	}{
		{"shard write wait", o.ShardWriteWait},
		{"shard write hold", o.ShardWriteHold},
		{"segment alloc", o.SegmentAlloc},
		{"sync (msync)", o.Sync},
		{"compact", o.Compact},
		{"retrain", o.Retrain},
		{"reindex", o.Reindex},
		{"snapshot lock", o.SnapshotLock},
	}
	fmt.Fprintln(w, "operational timings (cumulative):")
	for _, r := range rows {
		mean := time.Duration(0)
		if r.s.Count > 0 {
			mean = time.Duration(r.s.Nanos / r.s.Count)
		}
		fmt.Fprintf(w, "  %-17s n=%-8d total=%-11s mean=%-10s max=%s\n",
			r.label, r.s.Count, time.Duration(r.s.Nanos), mean, time.Duration(r.s.MaxNanos))
		// The mean hides tail stalls (a slow msync or a long retrain averages out to
		// milliseconds while its worst case is seconds). Show the latency histogram when
		// the tail is non-trivial, so the distribution -- not just the average -- is visible.
		if r.s.MaxNanos > int64(100*time.Millisecond) {
			if line := histLine(r.s.Buckets); line != "" {
				fmt.Fprintf(w, "  %17s %s\n", "", line)
			}
		}
	}
}

// histLine renders a latency histogram as "<=1ms:60000 <=1s:80 >30s:8", showing only
// non-empty buckets. buckets[i] counts occurrences <= bound[i]; the final entry counts
// occurrences above the last bound.
func histLine(buckets []int64) string {
	if len(buckets) == 0 {
		return ""
	}
	bounds := collections.LatencyBucketBoundsNanos()
	var parts []string
	for i, c := range buckets {
		if c == 0 {
			continue
		}
		var label string
		if i < len(bounds) {
			label = "<=" + time.Duration(bounds[i]).String()
		} else if len(bounds) > 0 {
			label = ">" + time.Duration(bounds[len(bounds)-1]).String()
		}
		parts = append(parts, fmt.Sprintf("%s:%d", label, c))
	}
	return strings.Join(parts, " ")
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
		fmt.Fprintln(console, "evaluation order (index probes prune first, most-selective; then per-candidate re-checks):")
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
	fields := strings.Fields(arg)
	all := false
	if len(fields) > 0 && (fields[0] == "-all" || strings.EqualFold(fields[0], "all")) {
		all, fields = true, fields[1:]
	}
	sampleMax, topN := "2000", "32"
	if len(fields) == 2 {
		sampleMax, topN = fields[0], fields[1]
	} else if len(fields) != 0 {
		fmt.Fprintln(console, "usage: .refreshhot [-all] [<sampleMax> <topN>]")
		return
	}
	if all {
		tables, err := s.exec.Tables()
		if err != nil {
			fmt.Fprintf(console, "error: %v\n", err)
			return
		}
		for _, tbl := range tables {
			fmt.Fprintf(console, "[%s] ", tbl)
			s.adminTable(console, tbl, "hot.refresh", sampleMax, topN)
		}
		return
	}
	s.admin(console, "hot.refresh", sampleMax, topN)
}

// admin runs a management action on the current table and prints the result.
func (s *session) admin(console io.Writer, action string, args ...string) {
	s.adminTable(console, s.table, action, args...)
}

// convertToMemory drops a table's on-disk backing, keeping its data in RAM only. It targets
// the argument table, or the current table if none is given. Requires DAEMON authorization
// at the daemon; best run during low write activity (a racing write can be lost).
func (s *session) convertToMemory(console io.Writer, arg string) {
	table := s.tableArg(arg)
	if err := s.exec.ConvertTableToMemory(table); err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		if h := HintFor(err); h != "" {
			fmt.Fprintf(console, "  hint: %s\n", h)
		}
		return
	}
	fmt.Fprintf(console, "table %q is now memory-only (on-disk backing dropped; data gone after a daemon restart)\n", table)
}

// adminTable runs a management action on a named table and prints the result.
func (s *session) adminTable(console io.Writer, table, action string, args ...string) {
	msg, err := s.exec.Admin(table, action, args...)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		if h := HintFor(err); h != "" {
			fmt.Fprintf(console, "  hint: %s\n", h)
		}
		return
	}
	fmt.Fprintln(console, msg)
}

// maintenance runs a maintenance action on the current table, or on every table when the
// argument leads with "-all" (or "all"); the remaining tokens are passed as action args.
// It is used by .reindex/.retrain/.compact/.rewrite so a whole store can be maintained
// without enumerating tables by hand.
func (s *session) maintenance(console io.Writer, arg, action string) {
	fields := strings.Fields(arg)
	if len(fields) > 0 && (fields[0] == "-all" || strings.EqualFold(fields[0], "all")) {
		tables, err := s.exec.Tables()
		if err != nil {
			fmt.Fprintf(console, "error: %v\n", err)
			return
		}
		if len(tables) == 0 {
			fmt.Fprintln(console, "no tables")
			return
		}
		for _, tbl := range tables {
			fmt.Fprintf(console, "[%s] ", tbl)
			s.adminTable(console, tbl, action, fields[1:]...)
		}
		return
	}
	s.admin(console, action, fields...)
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

// Prometheus column-alias prefixes: a projected column named label_<x> becomes a Prometheus
// label <x>; metric_<y> becomes a sample of the metric <view>_<y>. Columns without either
// prefix are ignored by the exporter.
const (
	viewLabelPrefix  = "label_"
	viewMetricPrefix = "metric_"
)

// showViews lists the materialized view names.
func (s *session) showViews(console io.Writer) {
	names, err := s.exec.ListViews()
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		return
	}
	if len(names) == 0 {
		fmt.Fprintln(console, "no materialized views")
		return
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(console, "  %s\n", n)
	}
}

// exportViews writes the Prometheus text exposition for one view (arg) or, if arg is empty,
// every view. Each view's rows are rendered from its label_*/metric_* columns.
func (s *session) exportViews(console io.Writer, arg string) {
	var names []string
	if arg = strings.TrimSpace(arg); arg != "" {
		names = []string{arg}
	} else {
		var err error
		names, err = s.exec.ListViews()
		if err != nil {
			fmt.Fprintf(console, "error: %v\n", err)
			return
		}
		sort.Strings(names)
	}
	for _, name := range names {
		if err := s.exportView(console, name); err != nil {
			fmt.Fprintf(console, "# error exporting %s: %v\n", name, err)
		}
	}
}

func (s *session) exportView(console io.Writer, name string) error {
	rows, err := s.exec.ViewRows(name)
	if err != nil {
		return err
	}
	type promSample struct {
		labels string // pre-rendered `{k="v",...}` (or "" when the row has no labels)
		value  float64
	}
	samples := map[string][]promSample{}
	var metricOrder []string
	for _, ad := range rows {
		attrs := ad.GetAttributes()
		sort.Strings(attrs)

		// Build this row's label set from its label_* columns.
		var lbl strings.Builder
		nlabels := 0
		for _, a := range attrs {
			if !strings.HasPrefix(a, viewLabelPrefix) {
				continue
			}
			key := strings.TrimPrefix(a, viewLabelPrefix)
			val, ok := ad.EvaluateAttrString(a)
			if !ok {
				val = ad.EvaluateAttr(a).String()
			}
			if nlabels == 0 {
				lbl.WriteByte('{')
			} else {
				lbl.WriteByte(',')
			}
			fmt.Fprintf(&lbl, "%s=%q", key, promEscape(val))
			nlabels++
		}
		if nlabels > 0 {
			lbl.WriteByte('}')
		}
		labels := lbl.String()

		// Emit one sample per metric_* column.
		for _, a := range attrs {
			if !strings.HasPrefix(a, viewMetricPrefix) {
				continue
			}
			metric := name + "_" + strings.TrimPrefix(a, viewMetricPrefix)
			v, ok := ad.EvaluateAttrNumber(a)
			if !ok {
				continue
			}
			if _, seen := samples[metric]; !seen {
				metricOrder = append(metricOrder, metric)
			}
			samples[metric] = append(samples[metric], promSample{labels: labels, value: v})
		}
	}
	sort.Strings(metricOrder)
	for _, metric := range metricOrder {
		fmt.Fprintf(console, "# HELP %s Materialized view %q metric.\n", metric, name)
		fmt.Fprintf(console, "# TYPE %s gauge\n", metric)
		for _, smp := range samples[metric] {
			fmt.Fprintf(console, "%s%s %s\n", metric, smp.labels, strconv.FormatFloat(smp.value, 'g', -1, 64))
		}
	}
	return nil
}

// promEscape escapes a Prometheus label value (backslash, double-quote, newline).
func promEscape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}
