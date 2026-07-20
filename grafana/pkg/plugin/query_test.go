package plugin

import (
	"testing"
	"time"
)

func testRange() timeRange {
	// 2021-01-01T00:00:00Z .. +1h, fixed so expectations are stable.
	from := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	return newTimeRange(from, from.Add(time.Hour))
}

func TestToSQL_Builder(t *testing.T) {
	tr := testRange() // from=1609459200 to=1609462800
	cases := []struct {
		name string
		q    queryModel
		want string
	}{
		{
			name: "count group by with filter",
			q: queryModel{
				Table:   "jobs",
				Metrics: []metricDef{{Func: "COUNT", Attr: "*"}},
				GroupBy: []string{"Owner"},
				Filters: []filterDef{{Attr: "JobStatus", Op: "==", Value: "2"}},
				OrderBy: "COUNT(*)", OrderDesc: true, Limit: 10,
			},
			want: `SELECT Owner, COUNT(*) FROM jobs WHERE JobStatus == 2 GROUP BY Owner ORDER BY COUNT(*) DESC LIMIT 10`,
		},
		{
			name: "string filter is quoted; time field applies range",
			q: queryModel{
				Table:     "machines",
				Columns:   []string{"Name", "State"},
				Filters:   []filterDef{{Attr: "State", Op: "==", Value: "Unclaimed"}},
				TimeField: "EnteredCurrentStatus",
			},
			want: `SELECT Name, State FROM machines WHERE State == "Unclaimed" && (EnteredCurrentStatus >= 1609459200 && EnteredCurrentStatus <= 1609462800)`,
		},
		{
			name: "no projection defaults to star",
			q:    queryModel{Table: "jobs"},
			want: `SELECT * FROM jobs`,
		},
		{
			name: "avg metric with attr",
			q: queryModel{
				Table:   "machines",
				Metrics: []metricDef{{Func: "AVG", Attr: "Cpus"}},
			},
			want: `SELECT AVG(Cpus) FROM machines`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.q.toSQL(tr)
			if err != nil {
				t.Fatalf("toSQL: %v", err)
			}
			if got != tc.want {
				t.Errorf("toSQL mismatch:\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

func TestToSQL_NoTableError(t *testing.T) {
	if _, err := (&queryModel{}).toSQL(testRange()); err == nil {
		t.Error("expected an error when no table is selected")
	}
}

func TestToSQL_CodeModeMacros(t *testing.T) {
	tr := testRange()
	q := queryModel{
		EditorMode: "code",
		RawSQL:     "SELECT Owner FROM jobs WHERE $__timeFilter(QDate) && QDate > $__unixEpochFrom() LIMIT $__timeTo()",
	}
	got, err := q.toSQL(tr)
	if err != nil {
		t.Fatalf("toSQL: %v", err)
	}
	want := `SELECT Owner FROM jobs WHERE (QDate >= 1609459200 && QDate <= 1609462800) && QDate > 1609459200 LIMIT 1609462800`
	if got != want {
		t.Errorf("macro expansion mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestToSQL_CodeModeEmpty(t *testing.T) {
	if _, err := (&queryModel{EditorMode: "code", RawSQL: "   "}).toSQL(testRange()); err == nil {
		t.Error("expected an error for empty SQL in code mode")
	}
}

func TestQuoteValue(t *testing.T) {
	cases := map[string]string{
		"Unclaimed": `"Unclaimed"`,
		"42":        "42",
		"3.14":      "3.14",
		"true":      "true",
		"undefined": "undefined",
		`"already"`: `"already"`,
		"":          `""`,
	}
	for in, want := range cases {
		if got := quoteValue(in); got != want {
			t.Errorf("quoteValue(%q) = %q, want %q", in, got, want)
		}
	}
}
