package repl

// WHAT THE STORE DID WITH A JOIN IT RAN.
//
// A join placed in the store leaves this process with nothing to measure. The client-side
// pipeline reports its own key-batching decision because it took it; the store's join takes the
// same kind of decision -- narrow this side's scan to the keys in hand, or read it whole -- and
// takes it where no EXPLAIN here can see. So the cost rule that chooses the store arm is the one
// whose outcome is least observable from the side that invokes it.
//
// The store reports it as a trailer after the last row, and dbrpc.JoinStatsReport is what
// arrived. Its three booleans are the whole reason it is a type rather than a db.JoinStats: a
// zero JoinStats is what a store that said nothing leaves behind AND what a join that narrowed
// on nothing would leave behind, so a report rendered without reading them first would print
// "narrowed on nothing" for a store that never spoke. Four answers, four sentences:
//
//	Unsupported         this store does not report join statistics
//	!Reported           they were asked for and the join was cut short before they arrived
//	Reported !Complete  the access path is real, the counts stopped where the join did
//	Reported Complete   all of it
//
// Collapsing any two of those is the failure this exists to prevent, because "nobody told me"
// and "it scanned" lead an operator to opposite actions.

import (
	"fmt"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// joinStatsReport is where a store-side join puts what the store says about running it, and nil
// when nothing is collecting it.
//
// Nil rather than a discarded report: a nil report is what tells the client not to set the
// request byte at all, so a session not running EXPLAIN ANALYZE costs the store nothing, and one
// that is costs it a single frame for the whole query rather than anything per row.
func (e *Executor) joinStatsReport() *dbrpc.JoinStatsReport {
	if e.explain == nil {
		return nil
	}
	return &e.explain.joinStats
}

// joinStatsLines renders what the store said about a join it ran, for EXPLAIN ANALYZE.
//
// It reports nothing at all when no join ran in the store -- a statement that is not a join, or
// one that fell back to the pipeline here. The plan above already says where the join ran, and a
// statistics block under a join that this process executed would attribute the pipeline's work
// to the store.
func joinStatsLines(tr *explainTrace) []string {
	if !tr.joinStatsAsked {
		return nil
	}
	rep := tr.joinStats
	switch {
	case rep.Unsupported:
		// A property of the peer, not of this query. Said as such, because an operator who
		// reads it as a fact about the join will go looking for an index that is not the
		// problem.
		return []string{"store join statistics: this store does not report them, so none were " +
			"asked for and none came back -- which says nothing about how the join ran"}
	case !rep.Reported:
		// The counts in hand are a zero value nothing ever wrote. Printing them here is
		// exactly the mistake the report's booleans exist to prevent.
		return []string{"store join statistics: asked for, and the join was cut short before they " +
			"arrived (the row stream ended early, or reading it stopped), so nothing is known " +
			"about how the store ran it"}
	}

	s := rep.Stats
	out := []string{"store join statistics:"}
	if !rep.Complete {
		// The access path is still worth printing -- which attribute a step narrowed on does
		// not change because the join stopped -- but every count below it describes the part
		// that ran, and a reader who takes them for a finished join's underestimates the
		// work the whole join would do.
		out = append(out, "  PARTIAL: the join did not run to its end, so the access path below is "+
			"what the store used but the counts stopped where the join did -- they are a floor, "+
			"not a finished join's totals")
	}
	if rep.Err != "" {
		out = append(out, "  the join stopped on: "+rep.Err)
	}
	out = append(out, fmt.Sprintf("  work: hashed=%d row(s)  streamed=%d row(s)  pairs examined=%d  rows produced=%d",
		s.HashRows, s.ScanRows, s.Pairs, s.Rows))
	if s.Groups > 0 {
		out = append(out, fmt.Sprintf("  groups returned: %d (the store reduced the rows before sending them)", s.Groups))
	}
	out = append(out, joinProbeLines(s)...)
	out = append(out, joinOrientationLines(s)...)
	return out
}

// joinProbeLines renders one line per step saying whether that step narrowed the side it folds
// in to the keys in hand, and -- when it did not -- why not.
//
// The why is the actionable half. A step that had no index to narrow on and a step that had one
// and declined it produce the same outcome, and they want different fixes: build the index, or
// look at what the decision priced it against. A line that states only the outcome sends a
// reader to the wrong one.
func joinProbeLines(s db.JoinStats) []string {
	steps := s.ProbeSteps
	if len(steps) == 0 {
		// A store that fills only the single-step fields, which for a two-table join are the
		// whole story. All three empty is a store that reported its work but not its access
		// path, and that is its own answer rather than a step that narrowed on nothing.
		if s.ProbeAttr == "" && s.ProbeWhy == "" && s.ProbeKeys == 0 {
			return []string{"  access path: the store reported none -- it did not say whether " +
				"any side's scan was narrowed, or why not"}
		}
		steps = []db.JoinProbeStat{{Attr: s.ProbeAttr, Keys: s.ProbeKeys, Why: s.ProbeWhy}}
	}
	out := make([]string, 0, 2*len(steps)+1)
	out = append(out, "  access path, one step per side folded in:")
	for i := range steps {
		out = append(out, "    "+joinProbeLine(i, steps[i]))
		if tuple := joinProbeTupleLine(steps[i]); tuple != "" {
			out = append(out, "      "+tuple)
		}
	}
	return out
}

// joinProbeLine is one step's decision.
func joinProbeLine(i int, st db.JoinProbeStat) string {
	where := fmt.Sprintf("step %d", i+1)
	if st.Alias != "" {
		where = fmt.Sprintf("step %d (%s)", i+1, st.Alias)
	}
	// The size the decision was taken against. Without it, "narrowing did not pay" and
	// "narrowing was priced against the wrong table size" read identically -- and the second
	// is a bug in the plan while the first is the plan working.
	priced := ""
	if st.EstRecords > 0 {
		priced = fmt.Sprintf("; the decision priced this side's scan at %d record(s)", st.EstRecords)
	}
	switch {
	case st.Keys > 0:
		attr := st.Attr
		if attr == "" {
			attr = "the join key"
		}
		return fmt.Sprintf("%s: scan narrowed on %s to %d key(s)%s", where, attr, st.Keys, priced)
	case st.Why != "":
		return fmt.Sprintf("%s: scan NOT narrowed -- %s%s", where, st.Why, priced)
	}
	return fmt.Sprintf("%s: scan NOT narrowed, and the store gave no reason -- this reads the "+
		"same as a side with no index to narrow on, and the two want different fixes%s", where, priced)
}

// joinProbeTupleLine says what a step did with a multi-attribute join key as a WHOLE.
//
// Two separate facts, neither of which the outcome line can carry. Narrowing per attribute
// admits the cross product of the values rather than the tuples the rows actually hold, so a
// step that narrowed is still reading more than it was asked for. And a step that declined the
// tuple form on cost while narrowing per attribute leaves the outcome line's why empty, which
// without this reads as a step that was never a candidate for the tuple form at all.
func joinProbeTupleLine(st db.JoinProbeStat) string {
	switch {
	case st.Tuple != "" && st.TupleComposite:
		return fmt.Sprintf("narrowed on the tuple %s as a whole: a composite index answers it in one lookup", st.Tuple)
	case st.Tuple != "":
		return fmt.Sprintf("narrowed on the tuple %s as a whole, but one lookup per component, "+
			"whose postings are then intersected", st.Tuple)
	case st.TupleWhy != "":
		return "NOT narrowed on the key tuple as a whole -- " + st.TupleWhy +
			"; the per-attribute narrowing admits the cross product of the values, not the tuples the rows hold"
	}
	return ""
}

// joinOrientationLines render the two decisions that change which rows the store reads FIRST,
// and with them the order the rows come back in.
//
// Both report their reason on either answer, so a plan left as written is distinguishable from
// one that was never looked at. For a chain the FROM side is where every step's probe gets its
// keys, which makes it the decision that says whether the chain reads its tables through an
// index at all.
func joinOrientationLines(s db.JoinStats) []string {
	var out []string
	if s.BuildSideSwapped || s.BuildSideWhy != "" {
		did := "left the sides as written"
		if s.BuildSideSwapped {
			did = "exchanged the sides, so the side written SECOND was streamed and the rows " +
				"come out in its order"
		}
		out = append(out, "  build side: the planner "+did+
			joinWhyClause(s.BuildSideWhy)+joinCostClause(s.BuildSideCost, s.BuildSideSwapCost,
			"as run", "swapped"))
	}
	if s.ChainFromAlias != "" || s.ChainOrderWhy != "" {
		did := "left the chain as written"
		if s.ChainFromAlias != "" {
			did = fmt.Sprintf("re-planned the chain to read FROM %s, which is where every "+
				"step's probe takes its keys", s.ChainFromAlias)
		}
		out = append(out, "  chain order: the planner "+did+
			joinWhyClause(s.ChainOrderWhy)+joinCostClause(s.ChainOrderCost, s.ChainOrderAsWrittenCost,
			"as run", "as written"))
	}
	return out
}

// joinWhyClause appends a reason when the store gave one.
func joinWhyClause(why string) string {
	if why == "" {
		return ""
	}
	return " -- " + why
}

// joinCostClause appends what the two alternatives were priced at, and nothing when neither was
// priced -- a pair of zeros would read as two free plans rather than as a decision taken on
// something other than cost.
func joinCostClause(chosen, other int, chosenName, otherName string) string {
	if chosen == 0 && other == 0 {
		return ""
	}
	return fmt.Sprintf(" (priced at %d record(s) %s against %d %s)", chosen, chosenName, other, otherName)
}
