package repl

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// The join evaluates its condition against ONE composite row reused across a left row's
// candidates, swapping the right-hand member each time. That is only sound if a ClassAd
// gives the same answer when a member is replaced as a freshly built ad would.
//
// It is sound because Insert replaces an existing name in place -- the attribute set does
// not change, so the ad is neither marked dirty nor re-sorted -- and nothing about
// evaluation is memoized against the ad. But "I read the code" is not evidence, and a cache
// added later would break this silently and in a way no join test would localize. So this
// asserts the property directly, against fresh ads as the oracle.
func TestJoinReusedCandidateRowAgreesWithFreshRows(t *testing.T) {
	mk := func(cluster int64, owner string) *classad.ClassAd {
		ad := classad.New()
		ad.InsertAttr("ClusterId", cluster)
		ad.InsertAttrString("Owner", owner)
		ad.InsertAttr("Cpus", cluster*2)
		return ad
	}
	left := mk(7, "alice")

	// Expressions chosen to read both members, to mix types, and to include the shapes most
	// likely to be cached by a future optimization: a select, an arithmetic combination, a
	// string comparison and a function call.
	exprs := []string{
		"j.ClusterId == h.ClusterId",
		"j.Cpus + h.Cpus",
		"j.Owner == h.Owner",
		"strcat(j.Owner, h.Owner)",
		"h.ClusterId > 3 && j.ClusterId > 3",
		"h.Missing",
	}
	compiled := make([]*classad.Expr, len(exprs))
	for i, src := range exprs {
		ex, err := classad.ParseExpr(src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		compiled[i] = ex
	}

	rights := []*classad.ClassAd{
		mk(7, "alice"), mk(3, "bob"), mk(7, "ALICE"), mk(99, "carol"), mk(7, "alice"),
	}

	// The reused row: built once, with the right member swapped per candidate.
	reused := classad.New()
	reused.InsertAttrClassAd("j", left)
	for ci, r := range rights {
		reused.InsertAttrClassAd("h", r)
		for i, ex := range compiled {
			// The oracle: a row built from scratch for this candidate.
			fresh := classad.New()
			fresh.InsertAttrClassAd("j", left)
			fresh.InsertAttrClassAd("h", r)

			got := valueDisplay(ex.Eval(reused))
			want := valueDisplay(ex.Eval(fresh))
			if got != want {
				t.Errorf("candidate %d, %q: reused row gave %q, a fresh row gave %q",
					ci, exprs[i], got, want)
			}
		}
	}
}

// The same property through the join itself: a query whose condition rejects most
// candidates must return what it returns when every candidate gets its own row.
//
// This is the end-to-end form of the test above. It would catch a reuse bug that only
// appears once the scratch row has been handed to the output and rebuilt, which the
// expression-level test cannot reach.
func TestJoinRejectingConditionAgreesAcrossReuse(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	// hist holds clusters 1, 2 and 9. A band that keeps only one pairing per job exercises
	// the reject path heavily while still keeping rows.
	r := mustExec(t, e, `SELECT j.ClusterId, h.ExitCode FROM jobs j
		JOIN hist h ON h.ClusterId >= j.ClusterId && h.ClusterId < j.ClusterId + 1`)
	wantRows(t, r, "1|0", "2|1")

	// And with nothing kept at all, which is the all-reject extreme the reuse path is for.
	r = mustExec(t, e, `SELECT j.ClusterId FROM jobs j
		JOIN hist h ON h.ClusterId == j.ClusterId + 1000`)
	wantRows(t, r)
}

// A LEFT JOIN interleaves surviving and rejected candidates, and its null-extended row is
// the accumulated row itself rather than a scratch. Getting the handover wrong -- keeping a
// scratch that a later candidate then mutates -- would show up here as a row whose right
// side belongs to a different pairing.
func TestLeftJoinRowsKeepTheirOwnRightSide(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	mustExec(t, e, `INSERT INTO hist (Key, ClusterId, ExitCode) VALUES ('h3', 1, 42)`)
	r := mustExec(t, e, `SELECT j.ClusterId, h.ExitCode FROM jobs j
		LEFT JOIN hist h ON j.ClusterId == h.ClusterId`)
	// Cluster 1 now matches two history rows and must keep both exit codes distinctly;
	// cluster 3 still matches nothing.
	wantRows(t, r, "1|0", "1|42", "2|1", "3|undefined")
}

// Every row the join emits must be a distinct ClassAd. Handing the scratch to the output
// without replacing it would alias two output rows to one ad, which every assertion above
// could still pass if the rows happened to be compared after the last mutation.
func TestJoinOutputRowsAreDistinctAds(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	res := mustExec(t, e, `SELECT j.ClusterId, h.ExitCode FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId`)
	seen := map[*classad.ClassAd]int{}
	for i, ad := range res.Ads {
		if prev, dup := seen[ad]; dup {
			t.Fatalf("output rows %d and %d are the same ad", prev, i)
		}
		seen[ad] = i
	}
	if len(res.Ads) < 2 {
		t.Fatalf("the fixture produced %d rows; it cannot detect aliasing", len(res.Ads))
	}
}

// A three-table join reuses a scratch at every step, and a later step reads members an
// earlier step put there. A scratch leaking between steps would show up as a row carrying
// another row's first-table member.
func TestThreeTableJoinRowsKeepEveryMember(t *testing.T) {
	e, cleanup := threeTableFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.ClusterId, h.ExitCode, u.Dept FROM jobs j
		LEFT JOIN hist h ON j.ClusterId == h.ClusterId
		JOIN users u ON u.Owner == j.Owner`)
	wantRows(t, r, "1|0|physics", "3|undefined|physics")

	// Spelled out per row, so a mismatched pairing cannot hide behind a sorted set.
	for i, ad := range r.Ads {
		cluster := valueDisplay(evalPath(ad, "j.ClusterId"))
		dept := valueDisplay(evalPath(ad, "u.Dept"))
		if dept != "physics" {
			t.Errorf("row %d: cluster %s has Dept %q", i, cluster, dept)
		}
		owner := valueDisplay(evalPath(ad, "j.Owner"))
		uowner := valueDisplay(evalPath(ad, "u.Owner"))
		if !strings.EqualFold(owner, uowner) {
			t.Errorf("row %d: joined j.Owner %q to u.Owner %q", i, owner, uowner)
		}
	}

}
