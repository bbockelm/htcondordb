package plugin

import (
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/bbockelm/htcondordb/repl"
)

// knownTimeAttrs are HTCondor attributes stored as unix-epoch seconds; a column
// with one of these names renders as a Grafana time field so it graphs correctly.
var knownTimeAttrs = map[string]bool{
	"qdate": true, "jobstartdate": true, "jobcurrentstartdate": true,
	"enteredcurrentstatus": true, "completiondate": true, "lastheardfrom": true,
	"daemonstarttime": true, "lastmatchtime": true, "joblaststartdate": true,
	"shadowbday": true, "enteredcurrentactivity": true, "mycurrenttime": true,
	"lastbenchmark": true,
	// A column literally named "time" holds unix-epoch seconds -- the conventional
	// alias for a time_bucket(...) series, so it graphs as the time axis.
	"time": true,
}

// resultToFrame converts a repl SELECT result into a Grafana data frame, inferring
// each column's type from its values (and name): unix-epoch time, numeric, or
// string. Cells are nullable so a missing/undefined value becomes a gap rather
// than a zero. timeField (from the builder) forces that column to a time field.
func resultToFrame(refID string, res *repl.Result, timeField, format string) *data.Frame {
	frame := data.NewFrame(refID)
	n := len(res.Rows)
	for c, col := range res.Columns {
		cells := make([]string, n)
		for r := 0; r < n; r++ {
			if c < len(res.Rows[r]) {
				cells[r] = res.Rows[r][c]
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
