package repl

import (
	"strings"
	"testing"
)

// adItem builds one write item from new-ClassAd text.
func adItem(key, text string) AdItem { return AdItem{Key: key, AdText: text} }

func TestWriteAdsUpserts(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	res, err := e.WriteAds("ads", []AdItem{
		adItem("a", `[Name = "a"; Cpus = 4]`),
		adItem("b", `[Name = "b"; Cpus = 8]`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Written != 2 || len(res.Rejects) != 0 {
		t.Fatalf("written=%d rejects=%v, want 2 and none", res.Written, res.Rejects)
	}
	if got := count(t, e, "ads"); got != "2" {
		t.Errorf("COUNT(*) = %s, want 2", got)
	}

	// Upsert: the same key replaces rather than duplicating.
	if _, err := e.WriteAds("ads", []AdItem{adItem("a", `[Name = "a"; Cpus = 64]`)}); err != nil {
		t.Fatal(err)
	}
	if got := count(t, e, "ads"); got != "2" {
		t.Errorf("after re-writing a key, COUNT(*) = %s, want 2", got)
	}
	r := mustExec(t, e, `SELECT Cpus FROM ads WHERE Name == "a"`)
	if r.Rows[0][0] != "64" {
		t.Errorf("Cpus = %s, want 64 (the write should replace)", r.Rows[0][0])
	}
}

// Replacing means an attribute the new ad omits is gone -- the sharp edge of upsert, and
// the reason writing back a projected read is dangerous.
func TestWriteAdsReplacesRatherThanMerges(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	if _, err := e.WriteAds("ads", []AdItem{adItem("a", `[Name = "a"; Cpus = 4; Extra = "keep"]`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.WriteAds("ads", []AdItem{adItem("a", `[Name = "a"; Cpus = 4]`)}); err != nil {
		t.Fatal(err)
	}
	if got := mustExec(t, e, `SELECT Extra FROM ads`).Rows[0][0]; got != "undefined" {
		t.Errorf("Extra = %s, want undefined (the omitted attribute should be gone)", got)
	}
}

// An expression written through this path stays an expression.
func TestWriteAdsPreservesExpressions(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	if _, err := e.WriteAds("ads", []AdItem{
		adItem("a", `[Name = "a"; Cpus = 8; Req = Cpus > 4]`),
	}); err != nil {
		t.Fatal(err)
	}
	ads := collectStream(t, e, "SELECT * FROM ads")
	expr, ok := ads[0].Lookup("Req")
	if !ok {
		t.Fatal("Req missing")
	}
	if !strings.Contains(expr.String(), "Cpus") {
		t.Errorf("Req stored as %q, want the expression", expr.String())
	}
	if got := mustExec(t, e, "SELECT Cpus, Req FROM ads").Rows[0][1]; got != "true" {
		t.Errorf("Req evaluates to %s, want true", got)
	}
}

// A bad ad is rejected by index and the rest of the batch still applies -- what a bulk
// loader needs, so one malformed record does not lose the file.
func TestWriteAdsPartialRejects(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	res, err := e.WriteAds("ads", []AdItem{
		adItem("good1", `[Name = "good1"]`),
		adItem("newline", `[Name = "newline"; Note = "a\nb"]`),
		adItem("", `[Name = "nokey"]`),
		adItem("unparseable", `[Name = `),
		adItem("good2", `[Name = "good2"]`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Written != 2 {
		t.Errorf("written=%d, want 2", res.Written)
	}
	if len(res.Rejects) != 3 {
		t.Fatalf("rejects=%v, want 3", res.Rejects)
	}
	// Indices are the caller's, not the surviving batch's.
	idx := map[int]string{}
	for _, r := range res.Rejects {
		idx[r.Index] = r.Reason
	}
	for _, want := range []int{1, 2, 3} {
		if _, ok := idx[want]; !ok {
			t.Errorf("index %d not rejected; got %v", want, idx)
		}
	}
	if !strings.Contains(idx[1], "newline") {
		t.Errorf("the newline reject does not say why: %q", idx[1])
	}
	if !strings.Contains(idx[2], "empty key") {
		t.Errorf("the empty-key reject does not say why: %q", idx[2])
	}
	if got := count(t, e, "ads"); got != "2" {
		t.Errorf("COUNT(*) = %s, want 2", got)
	}
}

// A trailing backslash is the other shape old-ClassAd text cannot hold.
func TestWriteAdsRejectsTrailingBackslash(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	res, err := e.WriteAds("ads", []AdItem{adItem("a", `[Name = "a"; Path = "C:\\dir\\"]`)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rejects) != 1 || !strings.Contains(res.Rejects[0].Reason, "backslash") {
		t.Errorf("rejects=%v, want one naming the backslash", res.Rejects)
	}
}

// Inside a transaction the writes stage into it rather than committing on their own.
func TestWriteAdsStagesIntoAnOpenTransaction(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "BEGIN")
	if _, err := e.WriteAds("ads", []AdItem{adItem("a", `[Name = "a"]`)}); err != nil {
		t.Fatal(err)
	}
	// Visible to the transaction...
	if got := count(t, e, "ads"); got != "1" {
		t.Errorf("inside the transaction COUNT(*) = %s, want 1", got)
	}
	mustExec(t, e, "ROLLBACK")
	// ...and gone once it is discarded.
	if got := count(t, e, "ads"); got != "0" {
		t.Errorf("after ROLLBACK COUNT(*) = %s, want 0", got)
	}
}

func TestWriteAdsCannotSpanTablesInATransaction(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()

	mustExec(t, e, "CREATE TABLE other")
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO ads (Key, Name) VALUES ('a', 'a')")
	_, err := e.WriteAds("other", []AdItem{adItem("b", `[Name = "b"]`)})
	if err == nil || !strings.Contains(err.Error(), "cannot span tables") {
		t.Errorf("err = %v, want a cannot-span-tables refusal", err)
	}
	mustExec(t, e, "ROLLBACK")
}

func TestWriteAdsEmptyBatch(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	res, err := e.WriteAds("ads", nil)
	if err != nil || res.Written != 0 {
		t.Errorf("empty batch: res=%v err=%v", res, err)
	}
}
