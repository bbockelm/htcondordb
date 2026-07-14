package repl

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
)

// Format is a result serialization mode, switchable in the REPL with `.format`.
type Format int

const (
	// FormatTable is the aligned column table (default).
	FormatTable Format = iota
	// FormatJSON emits one JSON object per line (JSONL).
	FormatJSON
	// FormatClassAdOld emits each ad in old (newline-separated) ClassAd format.
	FormatClassAdOld
	// FormatClassAdNew emits each ad in new (bracketed) ClassAd format.
	FormatClassAdNew
)

// ParseFormat parses a `.format` argument.
func ParseFormat(s string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "table", "":
		return FormatTable, nil
	case "json", "jsonl":
		return FormatJSON, nil
	case "classad", "classad-old", "old":
		return FormatClassAdOld, nil
	case "classad-new", "new":
		return FormatClassAdNew, nil
	default:
		return FormatTable, fmt.Errorf("unknown format %q (want table, json, classad, or classad-new)", s)
	}
}

func (f Format) String() string {
	switch f {
	case FormatJSON:
		return "json"
	case FormatClassAdOld:
		return "classad"
	case FormatClassAdNew:
		return "classad-new"
	default:
		return "table"
	}
}

// FormatResult writes r to w in the given format. Non-SELECT results always print
// their summary line. The ads-carrying JSON/ClassAd formats serialize whole ads
// (projection is a table-mode feature); an aggregate result, which has no ads,
// falls back to serializing its computed rows.
func FormatResult(w io.Writer, r *Result, f Format) {
	if !r.IsSelect {
		fmt.Fprintln(w, r.Note)
		return
	}
	switch f {
	case FormatJSON:
		formatJSON(w, r)
	case FormatClassAdOld:
		formatClassAd(w, r, false)
	case FormatClassAdNew:
		formatClassAd(w, r, true)
	default:
		formatTable(w, r)
	}
}

// formatJSON emits JSONL: whole ads when present, else one object per group row.
func formatJSON(w io.Writer, r *Result) {
	if r.Ads != nil {
		for _, ad := range r.Ads {
			b, err := ad.MarshalJSONWithPrivate()
			if err != nil {
				fmt.Fprintf(w, "{\"error\":%q}\n", err.Error())
				continue
			}
			w.Write(b)
			fmt.Fprintln(w)
		}
		return
	}
	for _, row := range r.Rows {
		obj := make(map[string]string, len(r.Columns))
		for i, col := range r.Columns {
			if i < len(row) {
				obj[col] = row[i]
			}
		}
		b, _ := json.Marshal(obj)
		w.Write(b)
		fmt.Fprintln(w)
	}
}

// formatClassAd emits each ad (or each aggregate row as a synthesized ad) in old
// or new ClassAd format, blank-line separated.
func formatClassAd(w io.Writer, r *Result, newFormat bool) {
	render := func(ad *classad.ClassAd) string {
		if newFormat {
			return ad.StringWithPrivate()
		}
		return ad.MarshalOldWithPrivate()
	}
	if r.Ads != nil {
		for _, ad := range r.Ads {
			fmt.Fprintln(w, render(ad))
			if !newFormat {
				fmt.Fprintln(w) // old-format ads are blank-line separated
			}
		}
		return
	}
	// Aggregate rows: synthesize a ClassAd per group row.
	for _, row := range r.Rows {
		ad := classad.New()
		for i, col := range r.Columns {
			if i < len(row) {
				ad.InsertAttrString(col, row[i])
			}
		}
		fmt.Fprintln(w, render(ad))
		if !newFormat {
			fmt.Fprintln(w)
		}
	}
}

// formatTable writes the aligned column table.
func formatTable(w io.Writer, r *Result) {
	if len(r.Columns) == 0 {
		fmt.Fprintln(w, "(no columns)")
		return
	}
	widths := make([]int, len(r.Columns))
	for i, c := range r.Columns {
		widths[i] = len(c)
	}
	for _, row := range r.Rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	writeRow := func(cells []string) {
		parts := make([]string, len(cells))
		for i, c := range cells {
			parts[i] = pad(c, widths[i])
		}
		fmt.Fprintln(w, strings.Join(parts, " | "))
	}
	writeRow(r.Columns)
	seps := make([]string, len(r.Columns))
	for i := range seps {
		seps[i] = strings.Repeat("-", widths[i])
	}
	fmt.Fprintln(w, strings.Join(seps, "-+-"))
	for _, row := range r.Rows {
		writeRow(row)
	}
	n := len(r.Rows)
	unit := "rows"
	if n == 1 {
		unit = "row"
	}
	fmt.Fprintf(w, "(%d %s)\n", n, unit)
}

func pad(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-len(s))
}
