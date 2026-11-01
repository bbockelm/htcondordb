package repl

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// The wire-cost benchmark's arms are only comparable if they return the same rows, and it
// checks that before timing -- but CI runs no benchmarks, so that check never executes and
// the fixture it depends on could rot unnoticed. This runs the same agreement at a size
// small enough to belong in the test suite.
//
// It is also a differential test in its own right: reading a side through the dbrpc server
// and reading it in-process are two separate code paths, and a projected read is a third.
// Nothing else here asserts they agree on the same data.
func TestJoinFetchArmsAgree(t *testing.T) {
	const clusters = 24
	fix := newJoinBenchFixture(t, clusters)
	defer fix.cleanup()

	// paths are compared across arms, and must name only attributes the statement
	// PROJECTS: a whole-ad arm legitimately carries attributes a projected arm dropped, so
	// comparing an unprojected one reports a difference that is not a disagreement.
	cases := []struct {
		name  string
		sql   string
		paths []string
		want  int
	}{
		{
			name:  "projected",
			sql:   `SELECT h.ClusterId, h.Owner, e.RunInstanceId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`,
			paths: []string{"h.ClusterId", "h.Owner", "e.RunInstanceId", "e.ExitCode"},
			want:  2 * clusters,
		},
		{
			// SELECT * projects nothing, so every arm reads whole ads and any attribute
			// may be compared.
			name:  "star",
			sql:   `SELECT * FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`,
			paths: []string{"h.ClusterId", "h.Owner", "h.Cmd", "e.RunInstanceId", "e.ExitCode", "e.TerminationReason"},
			want:  2 * clusters,
		},
		{
			// Few enough keys that the key-batch constraint is built, so the membership
			// constraint is on the path the arms must agree across.
			name:  "key-batched",
			sql:   `SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId WHERE h.ClusterId <= 8`,
			paths: []string{"h.ClusterId", "e.ExitCode"},
			want:  16,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := Parse(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			arms := []struct {
				name  string
				fetch joinFetch
			}{
				{"wire", fix.fetchWire},
				{"inproc-whole", fix.fetchWhole},
				{"inproc-projected", fix.fetchProjected},
			}
			var first []string
			for _, arm := range arms {
				got, rerr := renderBenchJoin(st, arm.fetch, tc.paths)
				if rerr != nil {
					t.Fatalf("%s: %v", arm.name, rerr)
				}
				if len(got) != tc.want {
					t.Errorf("%s: joined %d rows, want %d", arm.name, len(got), tc.want)
				}
				if first == nil {
					first = got
					continue
				}
				if len(got) != len(first) {
					t.Fatalf("%s: %d rows, but %s produced %d", arm.name, len(got), arms[0].name, len(first))
				}
				for i := range got {
					if got[i] != first[i] {
						t.Errorf("%s row %d = %q, but %s has %q", arm.name, i, got[i], arms[0].name, first[i])
					}
				}
			}
		})
	}
}

// renderBenchJoin runs the join and renders each composite row to a comparable string over
// the named paths, so the arms are compared on the values they produced rather than on a row
// count that two different wrong answers could share.
func renderBenchJoin(st *Statement, fetch joinFetch, paths []string) ([]string, error) {
	return renderJoinWith(st, fetch, paths, true)
}

// renderBenchJoinBatch renders a two-column join with key batching forced on or off, for the
// tests that must show the decision changes only the work and not the answer.
func renderBenchJoinBatch(st *Statement, fetch joinFetch, batching bool) ([]string, error) {
	return renderJoinWith(st, fetch, []string{"h.ClusterId", "e.ExitCode"}, batching)
}

// renderJoinWith is the shared core: plan, fetch both sides (optionally narrowing the right
// one to the left's keys), pair them, and render the named paths.
func renderJoinWith(st *Statement, fetch joinFetch, paths []string, batching bool) ([]string, error) {
	p, err := planJoin(st)
	if err != nil {
		return nil, err
	}
	leftAds, err := fetch(st, p.sides[0], 0)
	if err != nil {
		return nil, err
	}
	right := p.sides[1]
	if batching {
		if batch := p.steps[0].keyBatchConstraint(p.wrapFirst(leftAds)); batch != "" {
			right.where = andText(right.where, batch)
		}
	}
	rightAds, err := fetch(st, right, 1)
	if err != nil {
		return nil, err
	}
	rows, err := joinTwoSides(p, leftAds, rightAds)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		cells := make([]string, len(paths))
		for i, path := range paths {
			cells[i] = fmt.Sprintf("%v", evalPath(row, path))
		}
		out = append(out, strings.Join(cells, "|"))
	}
	sort.Strings(out)
	return out, nil
}
