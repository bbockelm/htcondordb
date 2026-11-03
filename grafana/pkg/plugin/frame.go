package plugin

import (
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/bbockelm/htcondordb/arrowtype"
	"github.com/bbockelm/htcondordb/repl"
)

// knownTimeAttrs are the attributes stored as unix-epoch seconds; a column with one of these
// names renders as a Grafana time field so it graphs correctly.
//
// The HTCondor ones come from arrowtype, which is the same list the daemon's Flight endpoint
// advertises as timestamp columns. They used to be written out here as well, and the two copies
// had already drifted -- so an attribute could graph on a time axis in one place and as a large
// integer in the other, for no reason either side could see.
//
// One entry is this plugin's own and does not belong upstream: a column literally named "time"
// is the conventional alias for a time_bucket(...) series, which is a Grafana convention rather
// than anything about HTCondor.
var knownTimeAttrs = func() map[string]bool {
	m := arrowtype.DefaultTimeAttrs()
	m["time"] = true
	return m
}()

// resultToFrame converts a repl SELECT result into a Grafana data frame.
//
// A column's type comes from DECLARED, when the caller could derive one: the same decision the
// Flight endpoint publishes for that column, so a panel and an ADBC client reading the same
// table agree about what it holds. Otherwise it falls back to inferring from the cells, which
// is what this did for every column before and is still what a shape with no declaration gets.
//
// THE FALLBACK IS THE BUG THIS REPLACES, and it is worth naming because it looks reasonable.
// numberFieldFrom bails on the FIRST non-numeric cell, so a column that is a number in 999 rows
// and holds one `undefined`-looking string in the thousandth becomes a STRING field -- and a
// Grafana graph of a string field is an empty panel. One cell silently changes the type of the
// whole column, and which cell depends on the time range the panel happens to be showing.
//
// Cells are nullable either way, so a missing or undefined value is a gap rather than a zero.
// timeField (from the builder) still forces that column to a time field: an explicit choice in
// the panel outranks both the declaration and the guess.
func resultToFrame(refID string, res *repl.Result, decisions []arrowtype.Decision, timeField, format string) *data.Frame {
	frame := data.NewFrame(refID)
	n := len(res.Rows)
	byName := make(map[string]arrowtype.Decision, len(decisions))
	for _, d := range decisions {
		byName[strings.ToLower(d.Name)] = d
	}
	for c, col := range res.Columns {
		cells := make([]string, n)
		for r := 0; r < n; r++ {
			if c < len(res.Rows[r]) {
				cells[r] = res.Rows[r][c]
			}
		}
		if d, ok := byName[strings.ToLower(col)]; ok && d.Verdict == arrowtype.Typed &&
			!isTimeColumn(col, timeField) {
			if f, ok := declaredField(col, d, cells); ok {
				frame.Fields = append(frame.Fields, f)
				continue
			}
		}
		frame.Fields = append(frame.Fields, inferField(col, cells, timeField))
	}
	if strings.EqualFold(format, "timeseries") {
		frame.Meta = &data.FrameMeta{PreferredVisualization: data.VisTypeGraph}
	}
	return frame
}

func inferField(name string, cells []string, timeField string) *data.Field {
	if isTimeColumn(name, timeField) {
		if f, ok := timeFieldFrom(name, cells); ok {
			return f
		}
	}
	if f, ok := numberFieldFrom(name, cells); ok {
		return f
	}
	return stringFieldFrom(name, cells)
}

func isTimeColumn(name, timeField string) bool {
	if timeField != "" && strings.EqualFold(name, timeField) {
		return true
	}
	return knownTimeAttrs[strings.ToLower(name)]
}

// timeFieldFrom parses cells as unix-epoch seconds (integer or fractional). It
// bails (ok=false) if any non-empty cell is not numeric, so a mislabeled column
// falls back to number/string rather than producing garbage timestamps.
func timeFieldFrom(name string, cells []string) (*data.Field, bool) {
	vals := make([]*time.Time, len(cells))
	any := false
	for i, s := range cells {
		s = strings.TrimSpace(s)
		if s == "" || strings.EqualFold(s, "undefined") {
			continue
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, false
		}
		sec := int64(f)
		nsec := int64((f - float64(sec)) * 1e9)
		t := time.Unix(sec, nsec).UTC()
		vals[i] = &t
		any = true
	}
	if !any {
		return nil, false
	}
	return data.NewField(name, nil, vals), true
}

// numberFieldFrom parses cells as float64. It bails if any non-empty cell is not
// numeric, so mixed columns become string fields.
func numberFieldFrom(name string, cells []string) (*data.Field, bool) {
	vals := make([]*float64, len(cells))
	any := false
	for i, s := range cells {
		s = strings.TrimSpace(s)
		if s == "" || strings.EqualFold(s, "undefined") {
			continue
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, false
		}
		v := f
		vals[i] = &v
		any = true
	}
	if !any {
		return nil, false
	}
	return data.NewField(name, nil, vals), true
}

func stringFieldFrom(name string, cells []string) *data.Field {
	vals := make([]*string, len(cells))
	for i, s := range cells {
		v := s
		vals[i] = &v
	}
	return data.NewField(name, nil, vals)
}

// declaredField builds a field of the type the schema DECLARED for this column, reading each
// cell as that type or as null.
//
// Null rather than a retype is the whole difference from inferring. A cell that does not parse
// is one bad cell; under inference it was a reason to change what the column IS, which is how a
// numeric series turns into text and a graph turns into an empty panel.
//
// Reports false for a declared type with no Grafana equivalent, so the caller falls back rather
// than inventing one.
func declaredField(name string, d arrowtype.Decision, cells []string) (*data.Field, bool) {
	switch d.Field.Type.ID() {
	case arrow.TIMESTAMP:
		vals := make([]*time.Time, len(cells))
		for i, s := range cells {
			if f, ok := epochSeconds(s); ok {
				sec := int64(f)
				t := time.Unix(sec, int64((f-float64(sec))*1e9)).UTC()
				vals[i] = &t
			}
		}
		return data.NewField(name, nil, vals), true
	case arrow.INT64:
		vals := make([]*int64, len(cells))
		for i, s := range cells {
			if v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
				n := v
				vals[i] = &n
			}
		}
		return data.NewField(name, nil, vals), true
	case arrow.FLOAT64:
		vals := make([]*float64, len(cells))
		for i, s := range cells {
			if v, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil {
				f := v
				vals[i] = &f
			}
		}
		return data.NewField(name, nil, vals), true
	case arrow.BOOL:
		vals := make([]*bool, len(cells))
		for i, s := range cells {
			switch strings.TrimSpace(s) {
			case "true":
				v := true
				vals[i] = &v
			case "false":
				v := false
				vals[i] = &v
			}
		}
		return data.NewField(name, nil, vals), true
	case arrow.STRING:
		return stringFieldFrom(name, cells), true
	}
	return nil, false
}

// epochSeconds reads a cell as unix seconds, integral or fractional.
func epochSeconds(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "undefined") {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil
}
