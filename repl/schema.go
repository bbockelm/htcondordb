package repl

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// The `.schema` command: see the derived schema, judge whether it still fits, rebuild it.
//
// The columnar accelerator's schema is recovered by SAMPLING, once, and then held stable -- every
// segment's block is built against a particular schema. So it can be enabled over a shape nobody
// expected, and it can drift as the workload changes, and neither shows up in the coverage counts
// `.stats` prints. These three views are how an operator sees that and acts on it.

// schemaCmd dispatches `.schema`, `.schema fit`, `.schema groups`, and `.schema rebuild`.
//
//	.schema [table]                     the derived schema, field by field
//	.schema fit [table] [sampleMax]     per-field escape rates against a fresh sample
//	.schema groups [table] [sampleMax]  candidate secondary schemas (co-occurring attributes)
//	.schema rebuild [table] [max topN]  re-derive the schema and rebuild every columnar block
func (s *session) schemaCmd(console io.Writer, arg string) {
	fields := strings.Fields(arg)
	switch {
	case len(fields) > 0 && strings.EqualFold(fields[0], "fit"):
		s.schemaFit(console, fields[1:])
	case len(fields) > 0 && strings.EqualFold(fields[0], "groups"):
		s.schemaGroups(console, fields[1:])
	case len(fields) > 0 && strings.EqualFold(fields[0], "rebuild"):
		s.schemaRebuild(console, fields[1:])
	default:
		s.withDiag(console, s.tableArg(arg), s.showSchema)
	}
}

// schemaGroups reports the candidate secondary (group) schemas: clusters of attributes that
// co-occur outside the base schema, derived from a fresh server-side sample. The admin action
// returns a preformatted report (or a plain message when the accelerator is off), so print it as-is.
func (s *session) schemaGroups(console io.Writer, args []string) {
	table, rest := s.tableAndArgs(args)
	msg, err := s.exec.Admin(table, "schema.groups", rest...)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		if h := HintFor(err); h != "" {
			fmt.Fprintf(console, "  hint: %s\n", h)
		}
		return
	}
	fmt.Fprintln(console, msg)
}

// showSchema renders the derived schema: what the sampler decided the ads look like.
func (s *session) showSchema(w io.Writer, d *dbrpc.Diagnostics) {
	ss := d.SchemaScan
	if !ss.Enabled {
		fmt.Fprintln(w, "columnar accelerator: off — no derived schema")
		fmt.Fprintln(w, "  a mutable table builds one during maintenance (or .analyze); an archive")
		fmt.Fprintln(w, "  only when the server is configured to (ArchiveSchemaScanHotTopN)")
		return
	}
	fmt.Fprintf(w, "columnar accelerator: on — %d/%d sealed segments covered, %d schema field(s)\n",
		ss.CoveredSegments, ss.SealedSegments, ss.SchemaFields)
	if len(ss.Schema) == 0 {
		// An older server reports the counts but not the schema itself.
		fmt.Fprintln(w, "  (this server does not report the schema's fields; hot columns: "+
			orNone(ss.HotFields)+")")
		return
	}
	fmt.Fprintln(w, "  attribute                 kind     width  tier")
	for _, f := range ss.Schema {
		width := "-"
		if f.Width > 0 {
			width = fmt.Sprintf("%d", f.Width)
		}
		kind := f.Kind
		if f.Unsigned {
			kind += " (u)"
		}
		tier := "cold"
		if f.Hot {
			tier = "HOT"
		}
		fmt.Fprintf(w, "  %-25s %-8s %5s  %s\n", f.Name, kind, width, tier)
	}
	// Secondary (group) schemas capture clusters of co-occurring attributes that fall outside the
	// base schema. They are committed only after the same cluster recurs across several maintenance
	// derivations, so a table can be freshly accelerated and still have none; `.schema groups` shows
	// the candidates from a fresh sample even before any is committed.
	if ss.GroupSchemas > 0 {
		fmt.Fprintf(w, "  %d secondary schema(s), %d field(s) total — see `.schema groups`\n",
			ss.GroupSchemas, ss.GroupSchemaFields)
	} else {
		fmt.Fprintln(w, "  no secondary schemas yet; `.schema groups` shows co-occurring-attribute candidates")
	}
	fmt.Fprintln(w, "  run `.schema fit` to see whether the schema still matches the data")
}

// schemaFit reports how well the schema still fits, from the server's fresh sample.
func (s *session) schemaFit(console io.Writer, args []string) {
	table, rest := s.tableAndArgs(args)
	msg, err := s.exec.Admin(table, "schema.fit", rest...)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		if h := HintFor(err); h != "" {
			fmt.Fprintf(console, "  hint: %s\n", h)
		}
		return
	}
	// The action returns either a plain message (accelerator off) or the JSON report.
	var rep struct {
		Sampled int                 `json:"sampled"`
		Fields  []db.SchemaFieldFit `json:"fields"`
	}
	if err := json.Unmarshal([]byte(msg), &rep); err != nil || len(rep.Fields) == 0 {
		fmt.Fprintln(console, msg)
		return
	}
	fmt.Fprintf(console, "schema fit over %d sampled record(s)\n", rep.Sampled)
	fmt.Fprintln(console, "  attribute                 kind     tier  escaped  missing  unstorable")
	drift := false
	for _, f := range rep.Fields {
		tier := "cold"
		if f.Hot {
			tier = "HOT"
		}
		unstorable := f.Escaped - f.Missing
		if f.Hot && unstorable > 0.01 {
			drift = true
		}
		fmt.Fprintf(console, "  %-25s %-8s %-4s %7.1f%% %8.1f%% %11.1f%%\n",
			f.Name, f.Kind, tier, f.Escaped*100, f.Missing*100, unstorable*100)
	}
	fmt.Fprintln(console, "  escaped   = value was not in its fixed slot (the slow path)")
	fmt.Fprintln(console, "  missing   = attribute absent; no schema change would fix it")
	fmt.Fprintln(console, "  unstorable = present but wrong kind or too wide -- what a rebuild recovers")
	if drift {
		fmt.Fprintln(console, "a HOT column is escaping: `.schema rebuild` would re-derive the schema "+
			"(cost: one columnar block rebuilt per sealed segment)")
	}
}

// schemaRebuild re-derives the schema and rebuilds every sealed segment's columnar block.
func (s *session) schemaRebuild(console io.Writer, args []string) {
	table, rest := s.tableAndArgs(args)
	if len(rest) != 0 && len(rest) != 2 {
		fmt.Fprintln(console, "usage: .schema rebuild [table] [sampleMax topN]")
		return
	}
	s.adminTable(console, table, "schema.rebuild", rest...)
}

// tableAndArgs peels an optional leading table name off a command's arguments, the way the
// maintenance commands do, so `.schema fit`, `.schema fit history` and `.schema fit 5000` all
// mean what they look like.
func (s *session) tableAndArgs(args []string) (string, []string) {
	if len(args) > 0 && !isNumericArg(args[0]) {
		return args[0], args[1:]
	}
	return s.table, args
}

// isNumericArg reports whether a token is all digits, i.e. a count rather than a table name.
func isNumericArg(tok string) bool {
	if tok == "" {
		return false
	}
	for _, r := range tok {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
