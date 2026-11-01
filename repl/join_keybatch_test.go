package repl

import (
	"testing"
)

// Narrowing the right side's scan to the left side's keys is worth the constraint's cost
// only when the scan it avoids is big enough. These assert the decision, not the timing:
// which plan was chosen, and on what information.
func TestKeyBatchDecision(t *testing.T) {
	// 256 clusters in history, two epoch rows each, so the right side holds 512 rows and
	// an unfiltered left side offers 256 keys -- two rows per key, far under the threshold.
	const clusters = 256
	fix := newJoinBenchFixture(t, clusters)
	defer fix.cleanup()

	cases := []struct {
		name      string
		sql       string
		wantBatch bool
		wantKeys  int
		wantKnown bool
		why       string
	}{
		{
			name:      "too few right rows per key",
			sql:       `SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`,
			wantBatch: false,
			wantKeys:  clusters,
			wantKnown: true,
			why:       "512 right rows against 256 keys is 2 per key; the constraint costs more than the scan",
		},
		{
			name:      "selective left side",
			sql:       `SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId WHERE h.ClusterId <= 4`,
			wantBatch: true,
			wantKeys:  4,
			wantKnown: true,
			why:       "512 right rows against 4 keys is 128 per key, well past the threshold",
		},
		{
			// An ON conjunct naming only the right side pushes to it, so its whole-table
			// count would overstate what the unbatched scan reads. The count is then not
			// asked for at all and the pre-measurement behavior stands.
			name:      "right side filters itself",
			sql:       `SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId && e.ExitCode == 0`,
			wantBatch: true,
			wantKeys:  clusters,
			wantKnown: false,
			why:       "a constrained right side is not described by its whole-table count",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := Parse(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			p, err := planJoin(st)
			if err != nil {
				t.Fatal(err)
			}
			leftAds, err := fix.exec.fetchJoinSide(st, p.left, 0)
			if err != nil {
				t.Fatal(err)
			}
			d := fix.exec.decideKeyBatch(p, leftAds)
			if got := d.constraint != ""; got != tc.wantBatch {
				t.Errorf("batched = %v, want %v (%s)", got, tc.wantBatch, tc.why)
			}
			if d.keys != tc.wantKeys {
				t.Errorf("keys = %d, want %d", d.keys, tc.wantKeys)
			}
			if d.knownRows != tc.wantKnown {
				t.Errorf("knownRows = %v, want %v (%s)", d.knownRows, tc.wantKnown, tc.why)
			}
			if tc.wantKnown && d.rightRows != 2*clusters {
				t.Errorf("rightRows = %d, want %d", d.rightRows, 2*clusters)
			}
		})
	}
}

// Whichever way the decision goes, the answer must not move. The constraint is a pure
// narrowing -- it removes right rows that could not have matched -- so declining it is a
// performance choice and never a correctness one. If that ever stops holding, the decision
// becomes unsafe rather than merely mispriced, so it is worth asserting separately from the
// decision itself.
func TestKeyBatchDoesNotChangeTheAnswer(t *testing.T) {
	const clusters = 64
	fix := newJoinBenchFixture(t, clusters)
	defer fix.cleanup()

	for _, sql := range []string{
		`SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`,
		`SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId WHERE h.ClusterId <= 4`,
		`SELECT h.ClusterId, e.ExitCode FROM history h LEFT JOIN epoch e ON h.ClusterId == e.ClusterId WHERE h.ClusterId <= 4`,
	} {
		st, err := Parse(sql)
		if err != nil {
			t.Fatal(err)
		}
		batched, err := renderBenchJoinBatch(st, fix.fetchWire, true)
		if err != nil {
			t.Fatalf("%s batched: %v", sql, err)
		}
		unbatched, err := renderBenchJoinBatch(st, fix.fetchWire, false)
		if err != nil {
			t.Fatalf("%s unbatched: %v", sql, err)
		}
		if len(batched) != len(unbatched) {
			t.Errorf("%s: batched %d rows, unbatched %d", sql, len(batched), len(unbatched))
			continue
		}
		for i := range batched {
			if batched[i] != unbatched[i] {
				t.Errorf("%s row %d: batched %q, unbatched %q", sql, i, batched[i], unbatched[i])
			}
		}
	}
}

// The threshold is a tunable, so a test can show that it -- and not some incidental property
// of the fixture -- is what decides.
func TestKeyBatchThresholdGoverns(t *testing.T) {
	const clusters = 64 // 128 right rows, 64 keys unfiltered: 2 per key
	fix := newJoinBenchFixture(t, clusters)
	defer fix.cleanup()

	st, err := Parse(`SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	leftAds, err := fix.exec.fetchJoinSide(st, p.left, 0)
	if err != nil {
		t.Fatal(err)
	}

	restore := minRightRowsPerKey
	defer func() { minRightRowsPerKey = restore }()

	minRightRowsPerKey = 20
	if d := fix.exec.decideKeyBatch(p, leftAds); d.constraint != "" {
		t.Errorf("at a threshold of 20 rows per key, 2 per key should not batch")
	}
	minRightRowsPerKey = 1
	if d := fix.exec.decideKeyBatch(p, leftAds); d.constraint == "" {
		t.Errorf("at a threshold of 1 row per key, 2 per key should batch")
	}
}
