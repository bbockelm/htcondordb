package repl

// WHETHER TO NARROW THE RIGHT SIDE'S SCAN TO THE LEFT SIDE'S KEYS.
//
// Key batching folds the key values the rows so far actually hold into one membership
// constraint on the table about to be read, so that scan reads a slice of the table instead
// of all of it. Both arms return the same rows, so the choice is entirely a cost one, and the
// cost is a trade between two quantities that move independently:
//
//	the constraint COSTS   something that grows with the KEY COUNT
//	the constraint SAVES   the part of the right table it excludes
//
// Neither is a property of the other, so no constant in one of them can decide it. A key count
// that pays against a large table loses against a small one, and a key count that loses against
// a unique key wins against a key the right side repeats.
//
// WHAT THE MEASUREMENTS SAY, AND WHAT THEY LEAVE OUT. Against a fixed 16,384-row right side the
// batched arm cost 1.7 ms at 32 keys, 12.0 at 128, 26.8 at 256, 47.0 at 400 and 89.6 at 511,
// while the unbatched arm sat near 90 ms however many keys there were. Holding the key count at
// 400 and growing the right side instead, the batched arm stayed flat near 45 ms while the
// unbatched arm went 10.6 ms at 1,024 rows, 26.8 at 4,096, 92.7 at 16,384 and 374 at 65,536.
// So the unbatched arm is linear in the right table and the batched arm is not, and break-even
// is a curve in both -- near 8,000 right-side rows at 400 keys, which is the 20 rows per key
// minRightRowsPerKey carries.
//
// Both of those sweeps hold one thing fixed that is not fixed in general: how much of the right
// table the keys select. The fixture's right side repeats each key twice, so 400 keys select 800
// rows out of however many there are, and the batched arm really does read a slice. A right side
// whose join key has FEW DISTINCT VALUES behaves the other way round -- 400 keys against an
// attribute with 10 distinct values select the whole table -- and then the batched arm reads
// everything the unbatched arm reads AND pays the constraint on top. The ratio of the two tables'
// sizes cannot see that; it is the same ratio either way. The number that sees it is NDV, the
// count of distinct values of the right side's join attribute, which the store can report from
// index metadata without reading a record.
//
// SO THE RULE IS THE SAME TRADE WITH THE SELECTED FRACTION PUT BACK IN:
//
//	rows the probe reads   ~  rightRows * min(1, keys/ndv)      (uniform over the distinct values)
//	rows the scan avoids   =  rightRows - rows the probe reads
//	the constraint costs      keyProbeCostRows(keys)            rows of scan, by the sweep above
//
// and batching pays when what it avoids covers what it costs. With no NDV the probe's read is
// taken as zero, which collapses the rule to `rightRows >= keys * minRightRowsPerKey` -- exactly
// the decision this made before NDV existed, down to the comparison. That is deliberate: a
// statistic that is absent must leave the decision where it was, not move it somewhere new and
// equally unjustified.
//
// WHY THE NDV TERM CAN ONLY SUBTRACT. It appears on the benefit side and nowhere else, so every
// NDV this consults can make the rule decline where it would have batched and can never make it
// batch where it would have declined. A wrong NDV therefore costs at most the batching win it
// talked the planner out of; it cannot produce the 4x pessimization the sweep found at the small
// end. That asymmetry is why an estimate is usable here at all, and it is what decides which
// accuracy classes ndvLowerBound accepts.

import (
	"fmt"
	"math"

	"github.com/PelicanPlatform/classad/collections"
)

// minRightRowsPerKey is how many rows the right side must hold per left-side key before
// narrowing its scan to those keys is worth the constraint's own cost.
//
// Measured at 400 keys, batching cost 4.0x MORE against a 1,024-row right table and 1.5x more at
// 4,096 rows, broke even around 8,000, and was 2.0x better at 16,384 and 8.0x better at 65,536 --
// break-even near 20 rows per key.
//
// A var rather than a const so a test can move it, and so a site whose data disagrees with the
// fixture this was measured on has somewhere to change it. The figure is one shape's break-even,
// not a law.
var minRightRowsPerKey = 20

// Beyond the key count the first sweeps reached, the constraint's cost stops being linear, and
// is charged at what it was MEASURED to cost rather than extrapolated from the figure above.
//
// The 20-rows-per-key figure is one point on a curve, taken at 400 keys against a 16,384-row
// right side, and reading it as a slope through the origin understates a much longer
// constraint: cost per key climbs from about 53 microseconds at 32 keys to 175 at 511, which a
// line through the origin cannot produce. Both sweeps agree the growth is a power law with an
// exponent near 1.45 -- 1.43 fitted on the first, 1.47 on the one below.
//
// They do NOT agree on where break-even sits. Against a 131,072-row right side the arms cost
// the same at about 10,500 rows at 256 keys, 29,000 at 512 and 80,000 at 1,024, where the
// linear figure asks for 5,120, 10,240 and 27,000 -- it under-charges by two to three times.
// The reason is visible in the sweeps themselves: an unbatched scan costs about 5.7
// microseconds per row at 16,384 rows and about 1.5 at 131,072, because a larger scan amortizes
// better, and break-even IN ROWS scales inversely with that rate. So the rows-per-key figure is
// not a constant of the data at all; it is a function of the scan it is being compared against.
//
// That is a finding about the shipped constant, and it is recorded rather than acted on: moving
// a number the shipped measurements were taken against, on the strength of a second fixture, is
// how a document ends up with two numbers and no reading. What it DOES decide is the band this
// change opens. Up to keyProbeLinearKeys the rule is the one that shipped, untouched. Above it
// -- key counts the old cap refused outright, so nothing can regress -- the charge is the
// measured curve, because a newly reachable plan that is three times worse than scanning would
// be worse than the refusal it replaces, and the measurement is in hand.
//
// The step between the two at keyProbeLinearKeys is the two measurements disagreeing. It is
// left visible instead of smoothed, because smoothing it would mean picking one of them.
var (
	// keyProbeLinearKeys is the key count up to which the constraint's cost is the linear
	// figure above -- the range the shipped decision was taken over.
	keyProbeLinearKeys = 512
	// keyProbeMeasuredKeys and keyProbeMeasuredRows are one measured point on the curve past
	// that range: at 1,024 keys the arms cost the same against a right side of about 80,000
	// rows.
	keyProbeMeasuredKeys = 1024
	keyProbeMeasuredRows = 80000.0
	// keyProbeCostExp is how the constraint's cost grows with the key count, fitted on both
	// sweeps independently and agreeing to within 0.04.
	keyProbeCostExp = 1.47
)

// ndvLowerBoundFactor deflates an NDV that bounds the truth from ABOVE, or bounds it in neither
// direction, into one that can be used as a lower bound.
//
// The rule needs a lower bound on NDV, because NDV is in a denominator: understating it
// overstates how much of the right table the probe reads, which is the direction that declines
// batching, and overstating it does the opposite. So an answer that can only be too low is
// usable as it stands, and an answer that can be too high is not usable until it is brought
// down.
//
// 0.5 is what the two measured worst cases ask for, and they ask for the same thing from both
// directions. An index that has not caught up with the writes reads about 50% low, so halving a
// reading that is already a floor costs a factor this never needed -- but an index still posting
// superseded record versions reads about 96% high, and 1/1.96 is 0.51. One factor covers both,
// and it costs almost nothing where it is not needed: against a key with a distinct value per
// row, halving NDV doubles an estimate of the probe's read that is a rounding error against the
// table anyway. Where it IS needed -- a key with a handful of distinct values -- it only makes
// the refusal firmer.
var ndvLowerBoundFactor = 0.5

// keyProbeCostRows is what the membership constraint costs, expressed in rows of the scan it is
// being weighed against, so the two sides of the trade are in one unit.
func keyProbeCostRows(keys int) float64 {
	if keys <= keyProbeLinearKeys || keyProbeLinearKeys <= 0 {
		return float64(minRightRowsPerKey) * float64(keys)
	}
	return keyProbeMeasuredRows * math.Pow(float64(keys)/float64(keyProbeMeasuredKeys), keyProbeCostExp)
}

// ndvSource is where a session learns an attribute's distinct-value count. See
// ExecConfig.AttrNDV: it is nil for a session that has no cheap way to ask, and absence is one
// of the cases the rule is written to handle rather than an error.
type ndvSource func(table, attr string) (collections.NDVEstimate, bool)

// ndvUse says what the rule did with an NDV reading, which is not the same question as what the
// reading said. A planner that printed only the number would read identically whether it
// trusted it, deflated it or threw it away.
type ndvUse int

const (
	// ndvUnavailable: the session has no way to ask, so nothing was looked up.
	ndvUnavailable ndvUse = iota
	// ndvNotIndexed: asked, and no segment indexes the attribute. This is NOT a count of
	// one -- a count of one is the degenerate key whose every row matches every key, and
	// acting on "we do not know" as though it were that would refuse batching exactly where
	// it pays most.
	ndvNotIndexed
	// ndvNoKeyableValue: indexed, but no record posted a value the index could key, so the
	// join is on values no statistic here has seen.
	ndvNoKeyableValue
	// ndvTaken: used as reported, because it cannot be too high.
	ndvTaken
	// ndvDeflated: used after ndvLowerBoundFactor, because it could be too high.
	ndvDeflated
)

// ndvLowerBound turns one reading into the lower bound on NDV this rule can use, and says how.
//
// The classes are not decoration and they do not all lead to the same place:
//
//   - exact, and a bound that can only be LOW, are already lower bounds. Taken as they are.
//   - a bound that can only be HIGH, and an estimate bounded in neither direction, are not.
//     Both are brought down by ndvLowerBoundFactor rather than refused, because the deflated
//     figure is still a real constraint on the degenerate case and the deflation is cheap
//     where the key is good.
//   - a reported count of ZERO is not a usable denominator and means the attribute's values
//     are all of a kind the index cannot key, so there is nothing to reason from.
//
// RelErr is deliberately NOT used to size the deflation. It is the sketch's relative standard
// error and it describes only the sketch: a reading that is Either BECAUSE a sketch merged
// several segments AND because the index lags AND because superseded versions are still posted
// carries the sketch's 3% and says nothing about the other two, which are the large ones. A
// number that does not bound what it is being asked to bound is worse than no number.
func ndvLowerBound(est collections.NDVEstimate) (float64, ndvUse) {
	if est.Distinct == 0 {
		return 0, ndvNoKeyableValue
	}
	switch est.Err {
	case collections.NDVErrNone, collections.NDVErrLow:
		return float64(est.Distinct), ndvTaken
	default:
		// NDVErrHigh and NDVErrEither alike: usable only once brought below the truth.
		d := float64(est.Distinct) * ndvLowerBoundFactor
		if d < 1 {
			d = 1 // one distinct value is the floor of the quantity, not a reason to give up
		}
		return d, ndvDeflated
	}
}

// keyBatchDecision is why the join did or did not narrow the right side's scan, so EXPLAIN can
// say which and on what basis.
type keyBatchDecision struct {
	constraint string // "" = not batching
	keys       int
	rightRows  int
	knownRows  bool // false = the right side's size could not be learned cheaply

	// What the NDV lookup contributed. ndv is the lower bound actually used and is 0 when
	// none was; reported is what the store said, kept so EXPLAIN can show a deflation rather
	// than only its result.
	use      ndvUse
	ndv      float64
	reported collections.NDVEstimate

	// probeRows is the estimated rows the narrowed scan reads, and costRows what the
	// constraint costs in the same unit. Kept so EXPLAIN prints the arithmetic rather than
	// the verdict.
	probeRows float64
	costRows  float64

	// why is one sentence for a reader of EXPLAIN, in the voice the placement decision uses:
	// a narrowing that was not applied for a reason reads exactly like one that was never
	// available, unless the reason is printed.
	why string
}

// batching reports whether the decision narrowed the scan.
func (d keyBatchDecision) batching() bool { return d.constraint != "" }

// decideKeyBatch chooses whether to narrow the right side's scan to the left side's keys.
//
// THE KEY COUNT IS MEASURED, NOT ESTIMATED. The left side has already been read by the time this
// runs, so how many distinct values it actually holds is a fact and not a guess -- and a fact in
// hand beats an estimate of the same quantity every time. The two halves this cannot measure are
// the right side's size and the right side's distinct-value count, and both are asked of the
// store, which is where they are cheap.
//
// They are asked only when the answer can change the decision: there is a single bare-attribute
// equality to batch on, and the right side carries no constraint of its own, which would make a
// whole-table count an overestimate of what the unbatched scan actually reads. When the count
// cannot be had cheaply the decision falls back to the key count alone, so an older server
// behaves exactly as before; neither helper ever falls back to reading rows, because a decision
// that scans a whole history table to decide whether to scan a whole history table is worse than
// not deciding.
func (e *Executor) decideKeyBatch(step *joinStep, side joinSide, rows []joinTuple) keyBatchDecision {
	lits := step.keyLiterals(rows)
	d := keyBatchDecision{keys: len(lits)}
	if len(lits) == 0 {
		d.why = "there is no single bare-attribute equality whose key values could narrow this scan"
		if step.batchAttr != "" && len(rows) > 0 {
			d.why = fmt.Sprintf("the rows so far hold more than %d distinct %s value(s), which is "+
				"more than one constraint should carry", maxJoinKeyBatch, step.batchAttr)
		}
		return d
	}
	if side.where != "" {
		// This side already filters itself, so its whole-table count would overstate the scan
		// being avoided. Keep the pre-measurement behavior rather than decide on a number that
		// does not describe this plan.
		d.constraint = step.batchConstraintFor(lits)
		d.why = fmt.Sprintf("narrowed %s to %d key(s) without pricing it: the side carries its own "+
			"constraint, so a whole-table count would overstate the scan being avoided",
			side.ref.Alias, len(lits))
		return d
	}
	n, ok := e.cheapRowCount(side.ref.Table)
	d.rightRows, d.knownRows = n, ok
	if !ok {
		d.constraint = step.batchConstraintFor(lits)
		d.why = fmt.Sprintf("narrowed %s to %d key(s) without pricing it: its row count could not "+
			"be had cheaply, so there is no second number to weigh the constraint against",
			side.ref.Alias, len(lits))
		return d
	}

	d.ndv, d.use, d.reported = e.rightKeyNDV(side, step)
	d.probeRows = probeRows(float64(n), len(lits), d.ndv)
	d.costRows = keyProbeCostRows(len(lits))
	avoided := float64(n) - d.probeRows
	if avoided < d.costRows {
		d.why = fmt.Sprintf("%s is scanned whole: narrowing it to %d key(s) would skip about %.0f "+
			"of its %d row(s), and the constraint costs about %.0f rows' worth%s",
			side.ref.Alias, len(lits), avoided, n, d.costRows, d.ndvPhrase())
		return d
	}
	d.constraint = step.batchConstraintFor(lits)
	d.why = fmt.Sprintf("%s is narrowed to %d key(s): that skips about %.0f of its %d row(s), "+
		"against a constraint costing about %.0f rows' worth%s",
		side.ref.Alias, len(lits), avoided, n, d.costRows, d.ndvPhrase())
	return d
}

// probeRows estimates how many of the right side's rows the narrowed scan reads.
//
// Uniformly over the distinct values: each of the keys selects one value's worth of the table,
// so keys/ndv of it. Without an NDV it is taken as ZERO rather than as anything else, which is
// what makes the rule collapse to the one that shipped before NDV existed -- an unknown is a
// term that is not added, not a guess that is.
func probeRows(rightRows float64, keys int, ndv float64) float64 {
	if ndv <= 0 {
		return 0
	}
	frac := float64(keys) / ndv
	if frac > 1 {
		frac = 1
	}
	return rightRows * frac
}

// rightKeyNDV asks the session's statistics source what the right side's join attribute's
// distinct-value count is, and reduces the answer to the lower bound the rule can use.
func (e *Executor) rightKeyNDV(side joinSide, step *joinStep) (float64, ndvUse, collections.NDVEstimate) {
	if e.attrNDV == nil || step.batchAttr == "" {
		return 0, ndvUnavailable, collections.NDVEstimate{}
	}
	est, ok := e.attrNDV(side.ref.Table, step.batchAttr)
	if !ok {
		return 0, ndvNotIndexed, collections.NDVEstimate{}
	}
	ndv, use := ndvLowerBound(est)
	if use == ndvNoKeyableValue {
		return 0, use, est
	}
	return ndv, use, est
}

// ndvPhrase is the clause EXPLAIN appends to the decision naming what the probe's share of the
// table rested on -- the number, how accurate the store said it was, and whether this trusted it
// as reported or brought it down first.
func (d keyBatchDecision) ndvPhrase() string {
	switch d.use {
	case ndvUnavailable:
		return ", counting the narrowed scan as free because this session cannot ask how many " +
			"distinct values the key has (the pre-NDV rule)"
	case ndvNotIndexed:
		return ", counting the narrowed scan as free because no index reports how many distinct " +
			"values the key has -- which is not the same as its having one"
	case ndvNoKeyableValue:
		return ", counting the narrowed scan as free because the index has posted no keyable " +
			"value for the key at all"
	case ndvTaken:
		return fmt.Sprintf(", reading about %.0f of them because the key has %.0f distinct value(s) "+
			"(%s, cannot read high)", d.probeRows, d.ndv, d.reported.Accuracy)
	case ndvDeflated:
		return fmt.Sprintf(", reading about %.0f of them because the key has at least %.0f distinct "+
			"value(s) (%s reported %d, which may read high, taken at %.0f%% of it)",
			d.probeRows, d.ndv, d.reported.Accuracy, d.reported.Distinct, ndvLowerBoundFactor*100)
	}
	return ""
}

// describeKeyNDV is what a plain EXPLAIN can say about the statistic the key-batch rule will
// use, before any row has been read.
//
// The key COUNT is not available here -- it is a fact about rows this has not fetched -- but NDV
// is, because it comes from index metadata rather than from records. So the half of the trade
// that is an estimate can be reported, and labelled, at plan time; the half that is a
// measurement waits for ANALYZE. Printing the accuracy class matters more here than anywhere
// else in the plan, because a reading that cannot be too high and a reading that can be are
// used differently and would otherwise print the same.
func (e *Executor) describeKeyNDV(side joinSide, step *joinStep) string {
	ndv, use, est := e.rightKeyNDV(side, step)
	switch use {
	case ndvUnavailable:
		return fmt.Sprintf("distinct %s.%s values: not asked -- this session has no statistics "+
			"source, so the rule weighs the key count against the row count alone",
			side.ref.Alias, step.batchAttr)
	case ndvNotIndexed:
		return fmt.Sprintf("distinct %s.%s values: unknown -- no index reports them, which is "+
			"not the same as there being one, so they are left out of the rule rather than "+
			"guessed at", side.ref.Alias, step.batchAttr)
	case ndvNoKeyableValue:
		return fmt.Sprintf("distinct %s.%s values: none the index can key (%d record(s) carry it "+
			"as something it cannot), so no statistic here has seen the values this joins on",
			side.ref.Alias, step.batchAttr, est.Exceptions)
	case ndvTaken:
		return fmt.Sprintf("distinct %s.%s values: %d (%s, error %s: %s)",
			side.ref.Alias, step.batchAttr, est.Distinct, est.Accuracy, est.Err, est.Source)
	default:
		return fmt.Sprintf("distinct %s.%s values: %d (%s, error %s: %s), used as at least %.0f "+
			"because a reading that can be high has to be brought under the truth before it can "+
			"stand in a denominator", side.ref.Alias, step.batchAttr, est.Distinct, est.Accuracy,
			est.Err, est.Source, ndv)
	}
}
