package repl

import (
	"fmt"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/db"
)

// The nine cells the key-batch sweeps actually measured, and which arm won each.
//
// Two sweeps, one variable each: the first holds a 16,384-row right side and grows the key
// count, the second holds 400 keys and grows the right side. Together they are the only
// evidence there is about where break-even lies, so the rule's one real check is that it picks
// the faster arm in every one of them -- the same check the placement model is held to.
//
// rightRows is the fixture's epoch table, which carries two rows per cluster, so the number of
// distinct key values in it is half its rows. That is stated here rather than computed in the
// rule, because it is a property of the fixture the numbers came from.
var keyBatchSweep = []struct {
	keys      int
	rightRows int
	batchWon  bool
	note      string
}{
	{32, 16384, true, "1.7 ms batched against 87.4 unbatched"},
	{128, 16384, true, "12.0 ms against 88.4"},
	{256, 16384, true, "26.8 ms against 89.8"},
	{400, 16384, true, "47.0 ms against 94.5"},
	{511, 16384, true, "89.6 ms against 93.3 -- break-even, batching marginally ahead"},
	{400, 1024, false, "42.4 ms batched against 10.6 unbatched: 4.0x worse"},
	{400, 4096, false, "41.3 ms against 26.8: 1.5x worse"},
	{400, 16384, true, "46.4 ms against 92.7: 2.0x better"},
	{400, 65536, true, "46.5 ms against 374: 8.0x better"},
}

// Does the rule pick the arm the clock picked?
//
// Run twice over the same cells: once with NDV, once without. Both have to agree with the
// measurement, and for opposite reasons. With NDV the rule is using its full form, and the
// sweep is what calibrated it. Without NDV it collapses to the rule that shipped before NDV
// existed, and the sweep is what calibrated THAT -- so a disagreement in the second arm would
// mean the collapse is not a collapse and absence has quietly changed the decision.
func TestKeyBatchModelAgreesWithTheMeasuredSweep(t *testing.T) {
	for _, withNDV := range []bool{true, false} {
		name := "without-ndv"
		if withNDV {
			name = "with-ndv"
		}
		t.Run(name, func(t *testing.T) {
			for _, c := range keyBatchSweep {
				ndv := 0.0
				if withNDV {
					ndv = float64(c.rightRows) / 2 // the fixture's two rows per key
				}
				probe := probeRows(float64(c.rightRows), c.keys, ndv)
				got := float64(c.rightRows)-probe >= keyProbeCostRows(c.keys)
				if got != c.batchWon {
					t.Errorf("keys=%d right=%d: rule says batch=%v, the clock says %v (%s)",
						c.keys, c.rightRows, got, c.batchWon, c.note)
				}
			}
		})
	}
}

// The decision has to move in the direction the cost curve says on BOTH sides of break-even,
// and it has to be the right side's size that moves it. One cell either side is not a curve;
// what makes this a test of the rule rather than of one number is that the two cells differ
// only in the right table.
func TestKeyBatchTurnsOverOnTheRightSidesSize(t *testing.T) {
	const keys = 400
	// Break-even at 400 keys is near 8,000 right-side rows. A table well under it must be
	// scanned whole and one well over it must be narrowed.
	below := float64(2048)
	above := float64(65536)
	if below-probeRows(below, keys, below/2) >= keyProbeCostRows(keys) {
		t.Errorf("a %0.f-row right side at %d keys should be scanned whole", below, keys)
	}
	if above-probeRows(above, keys, above/2) < keyProbeCostRows(keys) {
		t.Errorf("a %0.f-row right side at %d keys should be narrowed", above, keys)
	}
}

// Past the key count the sweep reached, the constraint has to be charged more than a line
// through the origin charges it, or removing the old unconditional cap would license a plan
// nothing has measured.
//
// The sweep's own cost per key climbs from about 53 microseconds at 32 keys to 175 at 511,
// which is what a linear model cannot produce; this asserts the model is superlinear where it
// matters and linear where the sweep anchored it, so the regime that shipped does not move.
func TestKeyProbeCostIsLinearToTheAnchorAndSuperlinearPastIt(t *testing.T) {
	for _, n := range []int{1, 32, 400, keyProbeLinearKeys} {
		want := float64(minRightRowsPerKey * n)
		if got := keyProbeCostRows(n); got != want {
			t.Errorf("at %d keys the cost is %.0f rows, want the linear %.0f", n, got, want)
		}
	}
	// Doubling the keys past the anchor must cost MORE than twice as much.
	at := keyProbeCostRows(keyProbeLinearKeys)
	twice := keyProbeCostRows(2 * keyProbeLinearKeys)
	if twice <= 2*at {
		t.Errorf("doubling %d keys to %d costs %.0f, which is not more than twice %.0f",
			keyProbeLinearKeys, 2*keyProbeLinearKeys, twice, at)
	}
	// And the band above the anchor is charged at what it was measured to cost, not at what
	// the shipped figure extrapolates to. The two disagree by two to three times, so a rule
	// that quietly used the smaller one would open a plan that is worse than the refusal it
	// replaces -- which is the whole hazard of raising a cap on a model alone.
	if got := keyProbeCostRows(keyProbeMeasuredKeys); got != keyProbeMeasuredRows {
		t.Errorf("at the measured point the cost is %.0f rows, want the measured %.0f",
			got, keyProbeMeasuredRows)
	}
	if extrapolated := float64(minRightRowsPerKey * keyProbeMeasuredKeys); keyProbeMeasuredRows <= extrapolated {
		t.Errorf("the measured charge (%.0f) is not above what the linear figure extrapolates "+
			"to (%.0f), so there is nothing for the step to protect",
			keyProbeMeasuredRows, extrapolated)
	}
}

// The old cap refused outright past 512 keys. The rule may now batch there -- but only when
// the right table is large enough to pay for a constraint that long, which is the whole point
// of pricing it instead of capping it.
//
// The two sizes straddle the measured break-even at this key count: the arms cost the same
// against about 80,000 rows, so a 65,536-row right side must still be scanned whole and a far
// larger one must be narrowed. That is the cell the measurement actually covers, rather than a
// pair of sizes chosen to be obviously on either side.
func TestKeyBatchBeyondTheOldCapNeedsALargeRightSide(t *testing.T) {
	const keys = 1024 // twice the 512 the old constant refused at
	small := float64(65536)
	large := float64(4_000_000)
	if small-probeRows(small, keys, small/2) >= keyProbeCostRows(keys) {
		t.Errorf("%d keys against a %.0f-row right side should still be scanned whole: "+
			"a constraint that long costs about %.0f rows' worth",
			keys, small, keyProbeCostRows(keys))
	}
	if large-probeRows(large, keys, large/2) < keyProbeCostRows(keys) {
		t.Errorf("%d keys against a %.0f-row right side should be narrowed: that is the win "+
			"the old cap forbade", keys, large)
	}
}

// THE CASE NDV EXISTS FOR. Two right sides of identical size and an identical key count, where
// the only difference is how many distinct values the join key takes. The ratio of the table
// sizes is the same in both, so a rule without NDV decides them identically -- and one of the
// two answers is wrong, because against a key with ten distinct values the narrowed scan reads
// the whole table and pays the constraint on top.
func TestKeyBatchRefusesALowNDVKey(t *testing.T) {
	const keys = 400
	const right = 16384

	good := float64(right) - probeRows(right, keys, right/2)
	if good < keyProbeCostRows(keys) {
		t.Fatalf("a key with %d distinct values should be narrowed; the sweep measured it 2.0x better", right/2)
	}
	low := float64(right) - probeRows(right, keys, 10)
	if low >= keyProbeCostRows(keys) {
		t.Errorf("a key with 10 distinct values should be scanned whole: narrowing it skips "+
			"about %.0f of %d rows and still pays the constraint", low, right)
	}
	// And the degenerate key, which is the one the hash join refuses at run time: every row
	// in one bucket, so the constraint excludes nothing at all.
	if float64(right)-probeRows(right, keys, 1) != 0 {
		t.Errorf("a key with one distinct value excludes nothing, so there is nothing to weigh")
	}
}

// Each accuracy class, and absence, reduced to the lower bound the rule is allowed to use.
//
// The directions are not symmetric and must not be treated as though they were: a reading that
// can only be LOW is already a lower bound on a quantity sitting in a denominator, and one that
// can be HIGH is not a bound at all until it is brought down.
func TestNDVLowerBoundByAccuracyClass(t *testing.T) {
	cases := []struct {
		name string
		est  collections.NDVEstimate
		want float64
		use  ndvUse
	}{
		{
			name: "exact is taken as it stands",
			est:  collections.NDVEstimate{Distinct: 8192, Err: collections.NDVErrNone, Accuracy: collections.NDVExact},
			want: 8192, use: ndvTaken,
		},
		{
			name: "a bound that can only read low is already a lower bound",
			est:  collections.NDVEstimate{Distinct: 8192, Err: collections.NDVErrLow, Accuracy: collections.NDVBound},
			want: 8192, use: ndvTaken,
		},
		{
			name: "a bound that can read high is brought under the truth first",
			est:  collections.NDVEstimate{Distinct: 8192, Err: collections.NDVErrHigh, Accuracy: collections.NDVBound},
			want: 4096, use: ndvDeflated,
		},
		{
			name: "an estimate bounded in neither direction is brought down the same way",
			est: collections.NDVEstimate{Distinct: 8192, Err: collections.NDVErrEither,
				Accuracy: collections.NDVEstimated, RelErr: 0.0325},
			want: 4096, use: ndvDeflated,
		},
		{
			name: "one distinct value stays one, deflated or not",
			est:  collections.NDVEstimate{Distinct: 1, Err: collections.NDVErrEither, Accuracy: collections.NDVEstimated},
			want: 1, use: ndvDeflated,
		},
		{
			name: "a count of zero is no keyable value, not a denominator",
			est:  collections.NDVEstimate{Distinct: 0, Err: collections.NDVErrNone, Accuracy: collections.NDVExact},
			want: 0, use: ndvNoKeyableValue,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, use := ndvLowerBound(tc.est)
			if got != tc.want || use != tc.use {
				t.Errorf("ndvLowerBound = (%.0f, %v), want (%.0f, %v)", got, use, tc.want, tc.use)
			}
		})
	}
}

// "Nothing indexes this attribute" and "this attribute has one distinct value" are opposite
// findings, and a planner that cannot tell them apart makes exactly the wrong decision: one
// distinct value is the key whose every row matches, where narrowing is worthless, and an
// unknown is the case for leaving the term out and deciding on what IS known.
func TestKeyBatchDistinguishesUnknownNDVFromOneValue(t *testing.T) {
	const keys = 400
	const right = 16384

	unknown := float64(right) - probeRows(right, keys, 0)
	one := float64(right) - probeRows(right, keys, 1)
	if unknown == one {
		t.Fatalf("an unknown NDV and an NDV of one must not price the same")
	}
	if unknown < keyProbeCostRows(keys) {
		t.Errorf("with NDV unknown the decision must stand on the key count and the row count, "+
			"which at %d keys and %d rows is a narrowing", keys, right)
	}
	if one >= keyProbeCostRows(keys) {
		t.Errorf("with one distinct value the narrowing excludes nothing and must be declined")
	}
}

// An absent statistics source has to leave the decision exactly where it was, not merely
// somewhere defensible. This pins the collapse: with no NDV, the rule IS `rightRows >= keys *
// minRightRowsPerKey` for every key count the old cap allowed.
func TestKeyBatchWithoutNDVIsThePreNDVRule(t *testing.T) {
	for _, keys := range []int{1, 7, 32, 128, 256, 400, 511, keyProbeLinearKeys} {
		// The sizes either side of this key count's own threshold are what make this a test
		// of the rule rather than of a handful of sizes: a collapse that is off by one row
		// is still a change of behaviour, and only a case that straddles the boundary sees
		// it.
		edge := keys * minRightRowsPerKey
		for _, right := range []int{0, 1, 100, 1024, 8000, 16384, 65536, 1 << 20,
			edge - 1, edge, edge + 1} {
			if right < 0 {
				continue
			}
			old := right >= keys*minRightRowsPerKey
			got := float64(right)-probeRows(float64(right), keys, 0) >= keyProbeCostRows(keys)
			if got != old {
				t.Fatalf("keys=%d right=%d: without NDV the rule says %v where the pre-NDV rule says %v",
					keys, right, got, old)
			}
		}
	}
}

// The NDV term can only ever subtract from the benefit, so consulting it can decline a
// narrowing the pre-NDV rule would have made and can never make one it would have declined.
// That is what makes an estimate safe to use at all: being wrong costs a missed win, not the
// 4x pessimization the sweep found at the small end.
func TestNDVCanOnlyDeclineNeverBatch(t *testing.T) {
	for _, keys := range []int{1, 32, 400, 512, 4096} {
		for _, right := range []int{0, 1024, 16384, 1 << 20} {
			for _, ndv := range []float64{1, 10, 1000, 1e6, 1e12} {
				without := float64(right)-probeRows(float64(right), keys, 0) >= keyProbeCostRows(keys)
				with := float64(right)-probeRows(float64(right), keys, ndv) >= keyProbeCostRows(keys)
				if with && !without {
					t.Fatalf("keys=%d right=%d ndv=%.0f: NDV turned a refusal into a narrowing",
						keys, right, ndv)
				}
			}
		}
	}
}

// The rule against a real store, with a real NDV read out of real index metadata: the number
// the model is fed has to be one the store will actually produce, not one a test invented.
func TestKeyBatchReadsNDVFromTheStore(t *testing.T) {
	const clusters = 256
	fix := newJoinBenchFixture(t, clusters)
	defer fix.cleanup()
	fix.exec.attrNDV = catalogNDV(fix.cat)

	st, err := Parse(`SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId WHERE h.ClusterId <= 4`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	leftAds, err := fix.exec.fetchJoinSide(st, p.sides[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	d := fix.exec.decideKeyBatch(&p.steps[0], p.sides[1], p.wrapFirst(leftAds))
	if !d.batching() {
		t.Errorf("4 keys against %d right rows should be narrowed: %s", 2*clusters, d.why)
	}
	if d.use != ndvTaken && d.use != ndvDeflated {
		t.Fatalf("the store reported no usable NDV for epoch.ClusterId (use=%v): %s", d.use, d.why)
	}
	// The epoch table holds two rows per cluster, so the key takes `clusters` distinct
	// values. An exact or low-bounded reading may not exceed that; a deflated one may not
	// exceed half of it.
	if d.ndv > float64(clusters) {
		t.Errorf("NDV read as %.0f, which is more than the %d distinct ClusterIds the table holds",
			d.ndv, clusters)
	}
	if d.ndv <= 0 {
		t.Errorf("NDV read as %.0f, which is not a usable denominator", d.ndv)
	}
}

// A low-NDV key end to end: the same question against a store whose epoch rows all share a
// handful of ClusterIds. The pre-NDV rule narrows it -- many rows, few keys, a good ratio --
// and that narrowing excludes nothing, which is precisely what the ratio cannot see.
func TestKeyBatchDeclinesALowNDVKeyAgainstTheStore(t *testing.T) {
	const distinct = 8
	fix := newLowNDVJoinFixture(t, 4096, distinct)
	defer fix.cleanup()

	st, err := Parse(`SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId WHERE h.ClusterId <= 8`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	leftAds, err := fix.exec.fetchJoinSide(st, p.sides[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	rows := p.wrapFirst(leftAds)

	// Without a statistics source this is the shape the pre-NDV rule likes best: a few keys
	// against a large table. It narrows, and the narrowing is worthless.
	fix.exec.attrNDV = nil
	blind := fix.exec.decideKeyBatch(&p.steps[0], p.sides[1], rows)
	if !blind.batching() {
		t.Fatalf("without NDV, %d keys against %d rows should narrow -- otherwise this test is "+
			"not showing what NDV adds: %s", blind.keys, blind.rightRows, blind.why)
	}

	fix.exec.attrNDV = catalogNDV(fix.cat)
	seeing := fix.exec.decideKeyBatch(&p.steps[0], p.sides[1], rows)
	if seeing.batching() {
		t.Errorf("with NDV, a key the right side repeats should be scanned whole (%s): %s",
			fix.exec.mustNDV(t, "epoch", "ClusterId"), seeing.why)
	}
	if !strings.Contains(seeing.why, "distinct value") {
		t.Errorf("the reason does not say what it rested on: %q", seeing.why)
	}
}

// EXPLAIN has to say which way the decision went, on what numbers, and how good the estimate
// among them was. A plan that silently used a bad estimate reads exactly like one that used a
// good one.
func TestExplainReportsTheKeyBatchDecision(t *testing.T) {
	const clusters = 256
	fix := newJoinBenchFixture(t, clusters)
	defer fix.cleanup()
	fix.exec.attrNDV = catalogNDV(fix.cat)

	plan := explainLines(t, fix.exec,
		`EXPLAIN SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId WHERE h.ClusterId <= 4`)
	// Plain EXPLAIN has read no rows, so it states the rule and the one input it CAN have
	// without reading any: the distinct-value count and how accurate the store called it.
	//
	// The class is read back from the store rather than written down here, because which one
	// this fixture produces is a property of how its segments sealed and not of the rule. A
	// multi-segment archive with an index that has not caught up answers "estimate", which is
	// the class the deflation exists for; asserting a literal would make this a test of the
	// fixture's segment count.
	est, ok := fix.cat.AttrNDV("epoch", "ClusterId")
	if !ok {
		t.Fatal("the fixture's epoch.ClusterId is indexed, so the store should report its NDV")
	}
	for _, want := range []string{"key batching:", "distinct e.ClusterId values:",
		string(est.Accuracy), fmt.Sprintf("%d", est.Distinct)} {
		if !strings.Contains(plan, want) {
			t.Errorf("plain EXPLAIN does not mention %q:\n%s", want, plan)
		}
	}

	analyzed := explainLines(t, fix.exec,
		`EXPLAIN ANALYZE SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId WHERE h.ClusterId <= 4`)
	// ANALYZE has read the left side, so it reports the decision itself and the arithmetic
	// under it: how many rows the narrowing skips, out of how many, against what the
	// constraint costs.
	for _, want := range []string{"is narrowed to 4 key(s)", "skips about", "rows' worth", "distinct value(s)"} {
		if !strings.Contains(analyzed, want) {
			t.Errorf("EXPLAIN ANALYZE does not mention %q:\n%s", want, analyzed)
		}
	}
}

// A session with no statistics source must say so rather than print a number it did not have.
func TestExplainSaysWhenNDVWasNotAsked(t *testing.T) {
	const clusters = 64
	fix := newJoinBenchFixture(t, clusters)
	defer fix.cleanup()

	plan := explainLines(t, fix.exec,
		`EXPLAIN SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`)
	if !strings.Contains(plan, "not asked") {
		t.Errorf("a session without a statistics source should say so:\n%s", plan)
	}
}

// An attribute nothing indexes answers "not known", which the plan must report as an unknown
// rather than as a count -- including not reporting it as one.
func TestExplainSaysWhenNothingIndexesTheKey(t *testing.T) {
	const clusters = 64
	fix := newJoinBenchFixture(t, clusters)
	defer fix.cleanup()
	fix.exec.attrNDV = catalogNDV(fix.cat)

	// ProcId is carried by both tables and indexed by neither, so it is a join key the store
	// has no statistic for.
	plan := explainLines(t, fix.exec,
		`EXPLAIN SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ProcId == e.ProcId`)
	if !strings.Contains(plan, "unknown -- no index reports them") {
		t.Errorf("an unindexed join key should report an unknown NDV:\n%s", plan)
	}
	if strings.Contains(plan, "distinct e.ProcId values: 1") {
		t.Errorf("an unknown NDV must not be reported as one distinct value:\n%s", plan)
	}
}

// catalogNDV is the statistics source a session that holds the catalog in its own process can
// supply. There is no dbrpc opcode for this, so a session reaching a store over a connection
// has none and takes the absent path; the tests wire it directly, which is also what makes
// them a test of the real metadata rather than of a hand-written number.
func catalogNDV(cat *db.Catalog) ndvSource {
	return func(table, attr string) (collections.NDVEstimate, bool) { return cat.AttrNDV(table, attr) }
}

// mustNDV reports what the session's source says, for a failure message that names the number
// the decision was taken on.
func (e *Executor) mustNDV(t *testing.T, table, attr string) string {
	t.Helper()
	if e.attrNDV == nil {
		return "no statistics source"
	}
	est, ok := e.attrNDV(table, attr)
	if !ok {
		return "not indexed"
	}
	return fmt.Sprintf("%d distinct (%s)", est.Distinct, est.Accuracy)
}

// explainLines runs one EXPLAIN and joins its rows, so a test can assert on the whole report.
func explainLines(t *testing.T, e *Executor, sql string) string {
	t.Helper()
	st, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Exec(st)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, r := range res.Rows {
		b.WriteString(r[0])
		b.WriteByte('\n')
	}
	return b.String()
}

// newLowNDVJoinFixture is newJoinBenchFixture with the right side's join key repeated instead
// of unique: n epoch rows spread over `distinct` ClusterIds, against a history table that
// still has one row per cluster.
//
// This is the shape neither sweep contains and the one the ratio of two table sizes cannot
// tell from a good key: the right side is large, the key count is small, and narrowing the
// scan to those keys excludes nothing because every row carries one of them.
func newLowNDVJoinFixture(t testing.TB, n, distinct int) *joinBenchFixture {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := func(zone []string) db.ArchiveConfig {
		return db.ArchiveConfig{SegmentSize: 1 << 16, ValueAttrs: []string{"ClusterId"}, ZoneAttrs: zone}
	}
	hist, err := cat.CreateArchiveTable("history", cfg([]string{"CompletionDate"}))
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := cat.CreateArchiveTable("epoch", cfg(nil))
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= distinct; i++ {
		if aerr := hist.Append(benchHistoryAd(i)); aerr != nil {
			t.Fatal(aerr)
		}
	}
	for i := 0; i < n; i++ {
		if aerr := epoch.Append(benchEpochAd(1+i%distinct, i%2)); aerr != nil {
			t.Fatal(aerr)
		}
	}
	return serveJoinFixture(t, cat)
}

// Past the old cap, with a right table big enough to pay for the constraint. The cap refused
// this outright; the rule now weighs it, so the key count alone no longer decides.
//
// The right side's size and its distinct-value count are both supplied rather than built,
// because the point is the ARITHMETIC above 512 keys and building a fifty-million-row table to
// reach it would be measuring the fixture. The decision reads them through exactly the paths a
// real session reads them through: the memoized cheap count, and the session's statistics
// source.
func TestKeyBatchNarrowsPastTheOldCapWhenItPays(t *testing.T) {
	const clusters = 2048
	const keys = 600 // past the 512 the old cap refused at
	fix := newJoinBenchFixture(t, clusters)
	defer fix.cleanup()

	const rightRows = 50_000_000
	fix.exec.rowCounts = map[string]int{"epoch": rightRows}
	fix.exec.attrNDV = func(string, string) (collections.NDVEstimate, bool) {
		// A key with a distinct value per row: the best a join key can be, and the case the
		// old cap forbade a win on.
		return collections.NDVEstimate{Distinct: rightRows, Err: collections.NDVErrNone,
			Accuracy: collections.NDVExact}, true
	}

	st, err := Parse(fmt.Sprintf(
		`SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId WHERE h.ClusterId <= %d`, keys))
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	leftAds, err := fix.exec.fetchJoinSide(st, p.sides[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	d := fix.exec.decideKeyBatch(&p.steps[0], p.sides[1], p.wrapFirst(leftAds))
	if d.keys != keys {
		t.Fatalf("keys = %d, want %d -- the constraint is no longer being built past the old cap", d.keys, keys)
	}
	if !d.batching() {
		t.Errorf("%d keys against %d rows should be narrowed: %s", keys, rightRows, d.why)
	}
}

// A row count that cannot be had cheaply leaves the decision where it was before there was
// anything to weigh: narrow, as this did for as long as the key count was the only input.
//
// The guard this pins had no test behind it, which a mutation of it found by surviving. It
// matters because it is the compatibility path -- a store too old to answer a count from its
// counters reaches exactly here -- and the failure mode of getting it wrong is silent: the
// join simply stops narrowing anything against every older server.
//
// The count is made unavailable by asking about a table that is not there, which is the
// contract cheapRowCount states: any error at all reports not-known. Nothing is fetched
// through this side, so the name only has to reach the count.
func TestKeyBatchNarrowsWhenTheRowCountIsUnknown(t *testing.T) {
	const clusters = 64
	fix := newJoinBenchFixture(t, clusters)
	defer fix.cleanup()
	fix.exec.attrNDV = catalogNDV(fix.cat)

	st, err := Parse(`SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	leftAds, err := fix.exec.fetchJoinSide(st, p.sides[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	// With the count available this shape is declined: 128 right rows against 64 keys is two
	// per key, far under the threshold. So if it narrows below, the missing count is the only
	// thing that can have changed it.
	if d := fix.exec.decideKeyBatch(&p.steps[0], p.sides[1], p.wrapFirst(leftAds)); d.batching() {
		t.Fatalf("with the count in hand, 2 rows per key should be scanned whole: %s", d.why)
	}

	side := p.sides[1]
	side.ref.Table = "no_such_table"
	d := fix.exec.decideKeyBatch(&p.steps[0], side, p.wrapFirst(leftAds))
	if d.knownRows {
		t.Fatalf("the count should not have been available for a table that is not there")
	}
	if !d.batching() {
		t.Errorf("with no second number to weigh against, the decision must stay where it was "+
			"-- narrowing: %s", d.why)
	}
}

// THE CAP IS A CORRECTNESS BOUND, SO THIS IS A CORRECTNESS TEST.
//
// The membership constraint joins its terms with `||`, so n values nest n deep, and the
// evaluator stops recursing at a fixed depth. Past it the predicate does not fail -- it
// matches NOTHING, and the query returns an empty result with no error anywhere. A join that
// built such a constraint would answer "no rows" to a statement with thousands of them, which
// is the worst shape a bug can take: a confident wrong answer to a question nobody would think
// to re-ask.
//
// Two things are pinned. First, where the boundary actually is, measured rather than taken
// from a constant in a dependency, so a release that moves it is noticed here rather than in a
// result. Second, that a constraint at maxJoinKeyBatch -- ANDed with another predicate, as a
// side that filters itself would be -- is comfortably inside it and answers correctly.
func TestKeyBatchStaysInsideTheEvaluatorsDepthLimit(t *testing.T) {
	const clusters = 2048
	fix := newJoinBenchFixture(t, clusters)
	defer fix.cleanup()

	chain := func(n int) string {
		terms := make([]string, n)
		for i := range terms {
			terms[i] = fmt.Sprintf("ClusterId == %d", i+1)
		}
		return "(" + strings.Join(terms, " || ") + ")"
	}
	count := func(where string) int {
		ads, err := fix.exec.queryAds("epoch", where, 0)
		if err != nil {
			t.Fatalf("%d-byte constraint: %v", len(where), err)
		}
		return len(ads)
	}

	// The cap's own constraint, and the cap's constraint wrapped the way a self-filtering
	// side wraps it. Both must answer, and answer correctly: two epoch rows per cluster.
	if got, want := count(chain(maxJoinKeyBatch)), 2*maxJoinKeyBatch; got != want {
		t.Errorf("a constraint at the cap (%d values) matched %d rows, want %d -- the cap is "+
			"past the depth the evaluator will recurse to", maxJoinKeyBatch, got, want)
	}
	wrapped := andText("ExitCode >= 0", chain(maxJoinKeyBatch))
	if got, want := count(wrapped), 2*maxJoinKeyBatch; got != want {
		t.Errorf("a constraint at the cap ANDed with another matched %d rows, want %d -- the "+
			"cap leaves no room for what the plan wraps around it", got, want)
	}

	// And where the boundary is, so a dependency that moves it is caught here. The cap must
	// stay well under it; the margin is what the plan's own nesting is allowed to use.
	deep := 0
	for _, n := range []int{1997, 1998} {
		if count(chain(n)) > 0 {
			deep = n
		}
	}
	if deep == 0 {
		t.Fatalf("no constraint near the measured boundary matched anything; the limit has moved down")
	}
	// The margin is stated in levels of nesting rather than as a ratio, because what uses it
	// is nesting: the conjuncts a plan wraps around the constraint, which are a handful and
	// not a proportion of the key count. 512 spare levels is orders of magnitude more than
	// any statement wraps, and it is what makes the cap safe rather than merely lucky.
	const depthMargin = 512
	if maxJoinKeyBatch+depthMargin > deep {
		t.Errorf("the cap is %d values against a measured limit of %d, which leaves %d levels "+
			"for what the plan nests around it; %d is the margin this is willing to run on",
			maxJoinKeyBatch, deep, deep-maxJoinKeyBatch, depthMargin)
	}
}
