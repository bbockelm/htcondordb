package repl

import (
	"fmt"
	"io"
	"strings"
)

// FormatResult writes a human-readable rendering of a Result to w.
func FormatResult(w io.Writer, r *Result) {
	if !r.IsSelect {
		fmt.Fprintln(w, r.Note)
		return
	}
	if len(r.Columns) == 0 {
		fmt.Fprintln(w, "(no columns)")
		return
	}
	// Column widths.
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
	// Separator.
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
