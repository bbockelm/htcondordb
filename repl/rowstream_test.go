package repl

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// Which shapes can produce a row from one ad, and which have to see the whole result first.
// Getting this list wrong is not a crash but silently wrong output -- a header that grows, or
// COUNT(*) evaluated against an ad that never held it -- so it is pinned statement by statement.
func TestRowStreamerDeclines(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		stream bool
		why    string
	}{
		{`SELECT Owner FROM jobs`, true, "plain column"},
		{`SELECT Owner, RequestMemory FROM jobs WHERE JobStatus = 2`, true, "several columns"},
		{`SELECT Owner AS who FROM jobs`, true, "alias"},
		{`SELECT RequestMemory * 2 AS doubled FROM jobs`, true, "expression column"},
		{`SELECT Owner FROM jobs ORDER BY Owner`, true, "ORDER BY: rows still come from ads"},
		{`SELECT DISTINCT Owner FROM jobs`, true, "DISTINCT: rows still come from ads"},
		{`SELECT Owner FROM jobs LIMIT 10`, true, "LIMIT"},

		{`SELECT * FROM jobs`, false, "star: the header is the union of every ad's attributes"},
		{`SELECT COUNT(*) FROM jobs`, false, "aggregate: rows are synthesized from groups"},
		{`SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner`, false, "GROUP BY"},
		{`SELECT Owner FROM jobs GROUP BY Owner HAVING COUNT(*) > 3`, false, "HAVING"},
		{`SELECT 2 * COUNT(*) AS twice FROM jobs`, false, "aggregate inside an expression"},
		{`SELECT Owner, ROW_NUMBER() OVER (ORDER BY QDate) AS n FROM jobs`, false,
			"window: the value ranks a row against the others"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			st, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			e := &Executor{}
			columns, row, err := e.RowStreamer(st)
			if err != nil {
				t.Fatalf("RowStreamer: %v", err)
			}
			if got := row != nil; got != tc.stream {
				t.Errorf("streams = %v, want %v (%s)", got, tc.stream, tc.why)
			}
			if tc.stream && len(columns) == 0 {
				t.Error("streaming statement produced no columns")
			}
			if !tc.stream && columns != nil {
				t.Errorf("declined statement returned columns %v", columns)
			}
		})
	}
}

// A non-SELECT has no rows to stream, and saying so is the caller's cue to run it instead.
func TestRowStreamerRejectsNonSelect(t *testing.T) {
	st, err := Parse(`UPDATE jobs SET JobStatus = 3 WHERE Owner = 'alice'`)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := (&Executor{}).RowStreamer(st); err == nil {
		t.Error("RowStreamer accepted an UPDATE")
	}
}

// The per-row function has to produce exactly what the materializing path would: same columns,
// same order, and values -- not display strings -- so a caller keeps ClassAd types.
func TestRowStreamerValues(t *testing.T) {
	st, err := Parse(`SELECT Owner, RequestMemory, RequestMemory * 2 AS doubled, Missing FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	columns, row, err := (&Executor{}).RowStreamer(st)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Owner", "RequestMemory", "doubled", "Missing"}
	for i, w := range want {
		if columns[i] != w {
			t.Errorf("columns[%d] = %q, want %q", i, columns[i], w)
		}
	}

	ad, err := classad.Parse(`[ Owner = "alice"; RequestMemory = 2048 ]`)
	if err != nil {
		t.Fatal(err)
	}
	values := row(ad)
	if s, _ := values[0].StringValue(); s != "alice" {
		t.Errorf("Owner = %v, want alice", values[0])
	}
	if n, _ := values[1].IntValue(); n != 2048 {
		t.Errorf("RequestMemory = %v, want 2048", values[1])
	}
	if n, _ := values[2].IntValue(); n != 4096 {
		t.Errorf("doubled = %v, want 4096", values[2])
	}
	// An attribute the ad does not carry is undefined, not an error and not a zero.
	if !values[3].IsUndefined() {
		t.Errorf("Missing = %v, want undefined", values[3])
	}
}
