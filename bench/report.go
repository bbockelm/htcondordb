package bench

import (
	"fmt"
	"strings"
	"text/tabwriter"
)

// Printing a report so the numbers in it cannot be read as more comparable than they are.
//
// The rates in one sweep span two PROJECTION FORMS that differ by roughly 10x, and the gap is not
// a property of the server: the stored form ships an ad carrying the expression it was written
// with, the evaluated form ships the number that expression computes to, and a consumer needs one
// or the other rather than whichever is faster. A flat table sorted by MB/s would invite exactly
// the wrong conclusion, so the output groups by form, says what each form is, and prints the
// store state and its warnings ABOVE the numbers rather than in a footnote.

// String renders the report as a plain-text table.
func (r Report) String() string {
	var b strings.Builder
	rd := r.Readiness
	fmt.Fprintf(&b, "table %s", rd.Table)
	if rd.Records > 0 {
		fmt.Fprintf(&b, "  records=%d", rd.Records)
	}
	if rd.Plan != "" {
		fmt.Fprintf(&b, "  plan=%s", rd.Plan)
	}
	fmt.Fprintf(&b, "\ncolumnar=%v", rd.ColumnarEnabled)
	if rd.SealedSegments > 0 {
		fmt.Fprintf(&b, " (%d/%d sealed segments covered)", rd.CoveredSegments, rd.SealedSegments)
	}
	if len(rd.ValueIndexes) > 0 || len(rd.CategoricalIdx) > 0 {
		fmt.Fprintf(&b, "  indexes: value=%v categorical=%v", rd.ValueIndexes, rd.CategoricalIdx)
	} else {
		b.WriteString("  indexes: none")
	}
	if rd.Encryption {
		b.WriteString("  encryption=on")
	}
	if rd.Escape.Measured {
		fmt.Fprintf(&b, "\nsampled read: %.0f%% of %d records rebuilt from the arena",
			100*rd.Escape.Rate(), rd.Escape.Visited)
	} else {
		b.WriteString("\nsampled read: no server-side breakdown -- the server may not be aggregating query stats,\n              or the read ran on a path that does not report them (a table with no sealed\n              segments reports none). NOT the same as nothing having been rebuilt.")
	}
	b.WriteString("\n")
	for _, w := range rd.Warnings {
		fmt.Fprintf(&b, "WARNING: %s\n", w)
	}

	samples := append([]Sample(nil), r.Samples...)
	SortSamples(samples)
	for _, grp := range []struct {
		evaluated bool
		title     string
	}{
		{false, "STORED form -- the ad carries the expression it was written with"},
		{true, "EVALUATED form -- typed values, what a consumer with a schema (Arrow, SQL) needs"},
	} {
		var in []Sample
		for _, s := range samples {
			if s.Transport.Evaluated() == grp.evaluated {
				in = append(in, s)
			}
		}
		if len(in) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\n%s\n", grp.title)
		tw := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "  transport\tlimit\tconc\trows\tMB/s\trows/s\tB/row\tnote")
		for _, s := range in {
			note := s.Note
			if s.Err != nil && note == "" {
				note = s.Err.Error()
			}
			lim := "all"
			if s.Limit > 0 {
				lim = fmt.Sprintf("%d", s.Limit)
			}
			_, _ = fmt.Fprintf(tw, "  %s\t%s\t%d\t%d\t%.1f\t%.0f\t%.1f\t%s\n",
				s.Transport, lim, s.Concurrency, s.Rows, s.MBPerSec(), s.RowsPerSec(), s.BytesPerRow(), note)
		}
		_ = tw.Flush()
	}
	b.WriteString("\nThe two forms are different products, not two speeds of one: a rate from one says\n")
	b.WriteString("nothing about the other, and only rates within a group are comparable.\n")
	return b.String()
}
