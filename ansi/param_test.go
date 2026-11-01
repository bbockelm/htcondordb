package ansi

import (
	"errors"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// bindOn lowers a predicate with placeholders, binds the values, and EVALUATES the result
// against an ad -- the same discipline as evalOn, for the same reason: comparing lowered strings
// would test my expectations of the lowering rather than its meaning.
func bindOn(t *testing.T, predicate string, vals []any, adText string) (admitted bool, lowered string) {
	t.Helper()
	tmpl, err := LowerTemplate(predicate, cols)
	if err != nil {
		t.Fatalf("LowerTemplate(%q): %v", predicate, err)
	}
	lowered, err = tmpl.Bind(vals)
	if err != nil {
		t.Fatalf("Bind(%q, %v): %v", predicate, vals, err)
	}
	ad, err := classad.ParseOld(adText)
	if err != nil {
		t.Fatalf("ParseOld(%q): %v", adText, err)
	}
	ex, err := classad.ParseExpr(lowered)
	if err != nil {
		t.Fatalf("the bound expression %q does not parse as ClassAd: %v", lowered, err)
	}
	b, berr := ex.Eval(ad).BoolValue()
	return berr == nil && b, lowered
}

func TestBoundValuesFilter(t *testing.T) {
	cases := []struct {
		q    string
		vals []any
		ad   string
		want bool
	}{
		{`Cpus = ?`, []any{int64(4)}, `Cpus = 4`, true},
		{`Cpus = ?`, []any{int64(4)}, `Cpus = 5`, false},
		{`Cpus > ?`, []any{int64(2)}, `Cpus = 4`, true},
		{`Cpus > ?`, []any{int64(2)}, `Cpus = 1`, false},
		{`Cpus <> ?`, []any{int64(4)}, `Cpus = 5`, true},
		{`Load > ?`, []any{0.5}, `Load = 0.75`, true},
		{`Load > ?`, []any{0.5}, `Load = 0.25`, false},
		{`Completed = ?`, []any{true}, `Completed = true`, true},
		{`Completed = ?`, []any{true}, `Completed = false`, false},
		{`Owner = ?`, []any{"alice"}, `Owner = "alice"`, true},
		{`Owner = ?`, []any{"alice"}, `Owner = "bob"`, false},
		{`QDate > ?`, []any{int64(1700000000)}, `QDate = 1700000001`, true},
		// Several placeholders, bound in source order.
		{`Cpus > ? AND Owner = ?`, []any{int64(2), "alice"},
			`Cpus = 4` + "\n" + `Owner = "alice"`, true},
		{`Cpus > ? AND Owner = ?`, []any{int64(2), "alice"},
			`Cpus = 4` + "\n" + `Owner = "bob"`, false},
		// A placeholder mixed with an inline literal.
		{`Cpus > ? AND Owner = 'alice'`, []any{int64(2)},
			`Cpus = 4` + "\n" + `Owner = "alice"`, true},
		// IN and BETWEEN lists bind element by element.
		{`Cpus IN (?, ?)`, []any{int64(1), int64(4)}, `Cpus = 4`, true},
		{`Cpus IN (?, ?)`, []any{int64(1), int64(2)}, `Cpus = 4`, false},
		{`Cpus BETWEEN ? AND ?`, []any{int64(2), int64(6)}, `Cpus = 4`, true},
		{`Cpus BETWEEN ? AND ?`, []any{int64(2), int64(6)}, `Cpus = 8`, false},
		// An int bound to a real column, which SQL would widen and ClassAd compares
		// numerically.
		{`Load > ?`, []any{int64(0)}, `Load = 0.5`, true},
	}
	for _, c := range cases {
		got, lowered := bindOn(t, c.q, c.vals, c.ad)
		if got != c.want {
			t.Errorf("%q bound %v over %q = %v, want %v (lowered: %s)",
				c.q, c.vals, c.ad, got, c.want, lowered)
		}
	}
}

// TestBoundEqualsInline is the equivalence that matters: binding a value must produce exactly
// what writing it inline would. Anything else means two dialects, and the second one is the one
// nobody tested.
func TestBoundEqualsInline(t *testing.T) {
	cases := []struct {
		param, inline string
		vals          []any
	}{
		{`Cpus = ?`, `Cpus = 4`, []any{int64(4)}},
		{`Cpus >= ?`, `Cpus >= 4`, []any{int64(4)}},
		{`Owner = ?`, `Owner = 'alice'`, []any{"alice"}},
		{`Completed = ?`, `Completed = TRUE`, []any{true}},
		{`Cpus IN (?, ?)`, `Cpus IN (1, 2)`, []any{int64(1), int64(2)}},
		{`Cpus BETWEEN ? AND ?`, `Cpus BETWEEN 2 AND 6`, []any{int64(2), int64(6)}},
		{`Cpus > ? OR Owner = ?`, `Cpus > 2 OR Owner = 'alice'`, []any{int64(2), "alice"}},
		{`NOT Cpus = ?`, `NOT Cpus = 4`, []any{int64(4)}},
		{`Owner = ? AND (Cpus = ? OR Cpus = ?)`, `Owner = 'a' AND (Cpus = 1 OR Cpus = 2)`,
			[]any{"a", int64(1), int64(2)}},
	}
	for _, c := range cases {
		tmpl, err := LowerTemplate(c.param, cols)
		if err != nil {
			t.Errorf("%q: %v", c.param, err)
			continue
		}
		bound, err := tmpl.Bind(c.vals)
		if err != nil {
			t.Errorf("%q: %v", c.param, err)
			continue
		}
		want, err := Lower(c.inline, cols)
		if err != nil {
			t.Errorf("%q: %v", c.inline, err)
			continue
		}
		if bound != want {
			t.Errorf("%q bound %v gave %q; inline %q gives %q", c.param, c.vals, bound, c.inline, want)
		}
	}
}

// TestBoundNullDropsTheRow: a NULL bound value makes the comparison yield NULL, which drops the
// row. It must not compare the column against anything -- see the note in param.go on why the
// slot covers the whole comparison.
func TestBoundNullDropsTheRow(t *testing.T) {
	cases := []struct {
		q    string
		vals []any
		ad   string
	}{
		{`Cpus = ?`, []any{nil}, `Cpus = 4`},
		{`Cpus <> ?`, []any{nil}, `Cpus = 4`},
		{`Cpus > ?`, []any{nil}, `Cpus = 4`},
		{`Owner = ?`, []any{nil}, `Owner = "alice"`},
		{`Completed = ?`, []any{nil}, `Completed = true`},
		{`Cpus BETWEEN ? AND ?`, []any{nil, int64(6)}, `Cpus = 4`},
		{`Cpus IN (?, ?)`, []any{nil, int64(9)}, `Cpus = 4`},
		// A NULL on one side of AND drops the row whatever the other side says.
		{`Cpus = 4 AND Owner = ?`, []any{nil}, `Cpus = 4` + "\n" + `Owner = "alice"`},
	}
	for _, c := range cases {
		got, lowered := bindOn(t, c.q, c.vals, c.ad)
		if got {
			t.Errorf("%q bound %v admitted %q, want dropped (lowered: %s)", c.q, c.vals, c.ad, lowered)
		}
	}
	// But a NULL in an OR or an IN list must not drop a row the other side admits: SQL's
	// `TRUE OR NULL` is TRUE.
	if ok, lowered := bindOn(t, `Cpus IN (?, ?)`, []any{nil, int64(4)}, `Cpus = 4`); !ok {
		t.Errorf("a matching IN element was dropped because another was NULL (lowered: %s)", lowered)
	}
	if ok, lowered := bindOn(t, `Cpus = 4 OR Owner = ?`, []any{nil}, `Cpus = 4`); !ok {
		t.Errorf("TRUE OR NULL dropped the row (lowered: %s)", lowered)
	}
}

// TestBoundStringIsCaseSensitive: the divergence this package exists for, reached through a
// bound value rather than a literal. A bind that quietly folded case would be the same bug
// wearing a different hat.
func TestBoundStringIsCaseSensitive(t *testing.T) {
	if ok, lowered := bindOn(t, `Owner = ?`, []any{"bbockelm"}, `Owner = "bbockelm"`); !ok {
		t.Errorf("exact match rejected (lowered: %s)", lowered)
	}
	if ok, lowered := bindOn(t, `Owner = ?`, []any{"bbockelm"}, `Owner = "BBockelm"`); ok {
		t.Errorf("a differently-cased value matched a bound parameter (lowered: %s)", lowered)
	}
}

// TestBoundStringCannotEscapeItsLiteral is why the lowering carries fragments rather than a
// string with markers. A bound value is the one part of the expression a caller controls, so it
// is the only place an injection could come from: each of these contains something that looks
// like expression syntax, and each must stay inside its literal.
func TestBoundStringCannotEscapeItsLiteral(t *testing.T) {
	hostile := []string{
		`x" || true || "y`,
		`" || true || "`,
		`a"b`,
		`a\b`,
		`a\"b`,
		`" =?= "`,
		"x\ny",
		"x\x00y",
		`${}`,
		`?`,
	}
	for _, v := range hostile {
		// The predicate must admit the ad that holds EXACTLY this value...
		ad := classad.New()
		ad.InsertAttrString("Owner", v)
		tmpl, err := LowerTemplate(`Owner = ?`, cols)
		if err != nil {
			t.Fatal(err)
		}
		lowered, err := tmpl.Bind([]any{v})
		if err != nil {
			t.Errorf("binding %q: %v", v, err)
			continue
		}
		ex, perr := classad.ParseExpr(lowered)
		if perr != nil {
			t.Errorf("binding %q lowered to %q, which does not parse: %v", v, lowered, perr)
			continue
		}
		if b, berr := ex.Eval(ad).BoolValue(); berr != nil || !b {
			t.Errorf("binding %q did not match an ad holding it (lowered: %s)", v, lowered)
		}
		// ...and must NOT admit an ad holding something else. A value that broke out of
		// its literal would most likely make the predicate unconditionally true, which is
		// exactly what this half catches.
		other := classad.New()
		other.InsertAttrString("Owner", "someone-else")
		if b, berr := ex.Eval(other).BoolValue(); berr == nil && b {
			t.Errorf("binding %q admitted an ad holding a different value; the value escaped "+
				"its literal (lowered: %s)", v, lowered)
		}
	}
}

// TestPlaceholderRefusalsMatchInline: every refusal the inline path gives must be given to the
// parameterised path, at PREPARE time. A client that learns `NOT Owner = ?` is unsupported only
// when it binds has been told at the wrong moment, and a parameterised predicate accepted where
// its inline twin is refused is a hole in the dialect guarantee.
func TestPlaceholderRefusalsMatchInline(t *testing.T) {
	for _, c := range []struct {
		q    string
		want string
	}{
		{`NOT Owner = ?`, "negated equality"},
		{`Owner > ?`, "case-insensitively"},
		{`Owner <= ?`, "case-insensitively"},
		{`Owner <> ?`, "three-valued"},
		{`Nope = ?`, "unknown column"},
		{`Owner BETWEEN ? AND ?`, "ordering comparisons"},
		{`? = 4`, "expected a column name"},
		{`Cpus = ? + 1`, ""},
	} {
		_, err := LowerTemplate(c.q, cols)
		if err == nil {
			t.Errorf("%q: lowered, want a refusal at prepare time", c.q)
			continue
		}
		if c.want != "" && !strings.Contains(err.Error(), c.want) {
			t.Errorf("%q: reason %q does not mention %q", c.q, err, c.want)
		}
	}
}

// TestDirectPathRefusesPlaceholders: only a prepared statement can supply a value, so Lower
// must refuse rather than compare against a literal question mark.
func TestDirectPathRefusesPlaceholders(t *testing.T) {
	_, err := Lower(`Cpus = ?`, cols)
	if err == nil {
		t.Fatal("a placeholder was accepted on the direct path")
	}
	var un *Unsupported
	if !errors.As(err, &un) {
		t.Errorf("%T, want *Unsupported", err)
	}
	if !strings.Contains(err.Error(), "prepared statement") {
		t.Errorf("reason %q does not say where placeholders do work", err)
	}
}

func TestBindRejectsBadValues(t *testing.T) {
	for _, c := range []struct {
		q    string
		vals []any
		want string
	}{
		// The wording is asserted, not just the refusal: lowerCompare would refuse these
		// too, with a message about a "literal" that misdirects a client who bound a
		// value. See bound.check.
		{`Cpus = ?`, []any{"four"}, "was bound a non-numeric value"},
		{`Owner = ?`, []any{int64(5)}, "was bound a non-string value"},
		{`Completed = ?`, []any{int64(1)}, "was bound a non-boolean value"},
		{`Cpus = ?`, []any{math.NaN()}, "NaN"},
		{`Cpus = ?`, []any{math.Inf(1)}, "infinite"},
		{`Cpus = ?`, []any{math.Inf(-1)}, "infinite"},
		{`Cpus = ?`, []any{uint64(math.MaxUint64)}, "too large"},
		{`Cpus = ?`, []any{[]string{"x"}}, "cannot be bound"},
		// Count mismatches, both ways.
		{`Cpus = ?`, nil, "takes 1 parameter"},
		{`Cpus = ?`, []any{int64(1), int64(2)}, "takes 1 parameter"},
		{`Cpus = 4`, []any{int64(1)}, "takes 0 parameter"},
	} {
		tmpl, err := LowerTemplate(c.q, cols)
		if err != nil {
			t.Errorf("%q: %v", c.q, err)
			continue
		}
		_, err = tmpl.Bind(c.vals)
		if err == nil {
			t.Errorf("%q bound %v: accepted, want a refusal", c.q, c.vals)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%q bound %v: reason %q does not mention %q", c.q, c.vals, err, c.want)
		}
	}
}

// TestBindAcceptsTheNarrowerWidths: a caller reading an Arrow column should not have to know
// which Go widths this package takes, so the normalisation lives here rather than at every
// caller.
func TestBindAcceptsTheNarrowerWidths(t *testing.T) {
	for _, v := range []any{int(4), int8(4), int16(4), int32(4), int64(4), uint32(4), uint64(4), float32(4), float64(4)} {
		ok, lowered := bindOn(t, `Cpus = ?`, []any{v}, `Cpus = 4`)
		if !ok {
			t.Errorf("a %T holding 4 did not match Cpus = 4 (lowered: %s)", v, lowered)
		}
	}
}

func TestParamsReportColumnAndKind(t *testing.T) {
	tmpl, err := LowerTemplate(`Owner = ? AND Cpus > ? AND Load < ?`, cols)
	if err != nil {
		t.Fatal(err)
	}
	got := tmpl.Params()
	want := []Param{
		{Column: "Owner", Kind: KindString},
		{Column: "Cpus", Kind: KindInt},
		{Column: "Load", Kind: KindReal},
	}
	if len(got) != len(want) {
		t.Fatalf("%d parameters, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("parameter %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	// The slice is a copy: a caller building a parameter schema must not be able to alter
	// the template other fetches are using.
	got[0].Column = "clobbered"
	if again := tmpl.Params(); again[0].Column != "Owner" {
		t.Error("Params() hands out the template's own slice; a caller can rewrite it")
	}
}

// TestTemplateIsReusable: one prepared statement serves many fetches, so a Bind must not consume
// or mutate the template. A template that remembered its last binding would return the previous
// query's rows -- a bug nobody reports, because nobody compares.
func TestTemplateIsReusable(t *testing.T) {
	tmpl, err := LowerTemplate(`Cpus = ?`, cols)
	if err != nil {
		t.Fatal(err)
	}
	first, err := tmpl.Bind([]any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := tmpl.Bind([]any{int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("both binds gave %q", first)
	}
	again, err := tmpl.Bind([]any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Errorf("rebinding the first value gave %q, want %q", again, first)
	}
	// A failed bind must not damage the template either.
	if _, err := tmpl.Bind([]any{"not a number"}); err == nil {
		t.Fatal("a bad value was accepted")
	}
	if after, err := tmpl.Bind([]any{int64(1)}); err != nil || after != first {
		t.Errorf("after a failed bind: %q, %v; want %q", after, err, first)
	}
}

// TestConcurrentBind: the server holds one template per prepared statement and a client may
// fetch on it from several threads. Run under -race, this is the check that a template is
// genuinely immutable rather than only apparently so.
func TestConcurrentBind(t *testing.T) {
	tmpl, err := LowerTemplate(`Cpus = ? AND Owner = ?`, cols)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			want, err := tmpl.Bind([]any{int64(i), "u"})
			if err != nil {
				errs <- err
				return
			}
			for j := 0; j < 50; j++ {
				got, err := tmpl.Bind([]any{int64(i), "u"})
				if err != nil {
					errs <- err
					return
				}
				if got != want {
					errs <- errors.New("a concurrent bind returned " + got + ", want " + want)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
