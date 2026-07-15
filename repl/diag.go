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
	case ".stats":
		s.withDiag(console, s.showStats)
	case ".indexes", ".index":
		s.withDiag(console, s.showIndexes)
	case ".hot":
		s.withDiag(console, s.showHot)
	case ".suggest":
		s.withDiag(console, s.showSuggest)
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

// withDiag fetches diagnostics once and hands them to fn (reporting a fetch error).
func (s *session) withDiag(console io.Writer, fn func(io.Writer, *dbrpc.Diagnostics)) {
	d, err := s.exec.Diagnostics()
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
	if len(d.Suggestions) > 0 {
		fmt.Fprintln(w, "suggested indexes (by observed demand):")
		printSuggestions(w, d.Suggestions)
	}
}

func (s *session) showHot(w io.Writer, d *dbrpc.Diagnostics) {
	fmt.Fprintf(w, "hot attributes: %s\n", orNone(d.Hot))
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
		return
	}
	ex, err := s.exec.Explain(arg)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		return
	}
	fmt.Fprintf(console, "plan:         %s\n", ex.Plan)
	fmt.Fprintf(console, "wire-native:  %v\n", ex.Native)
	fmt.Fprintf(console, "index-usable: %d of %d probe(s)\n", ex.IndexUsable, len(ex.Probes))
	fmt.Fprintf(console, "parallelism:  %d worker(s) over %d shard(s)\n", ex.Parallelism, ex.Shards)
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
			fmt.Fprintf(console, "  %-20s %-4s %-6s (%s)\n", p.Attr, p.Op, state, kind)
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

// admin runs a management action and prints the server's message (or error).
func (s *session) admin(console io.Writer, action string, args ...string) {
	msg, err := s.exec.Admin(action, args...)
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
