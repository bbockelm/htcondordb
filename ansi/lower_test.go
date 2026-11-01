package ansi

import (
	"errors"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

var cols = KindsFromNames(map[string]Kind{
	"Owner":     KindString,
	"Site":      KindString,
	"Cpus":      KindInt,
	"Load":      KindReal,
	"Completed": KindBool,
	"QDate":     KindTimestamp,
})

// evalOn lowers a predicate and EVALUATES it against an ad, which is the only check that
// matters. Comparing lowered strings would test my expectations of the lowering; evaluating
// tests whether the lowering means what the SQL meant.
func evalOn(t *testing.T, predicate, adText string) (admitted bool, lowered string) {
	t.Helper()
	lowered, err := Lower(predicate, cols)
	if err != nil {
		t.Fatalf("Lower(%q): %v", predicate, err)
	}
	ad, err := classad.ParseOld(adText)
	if err != nil {
		t.Fatalf("ParseOld(%q): %v", adText, err)
	}
	ex, err := classad.ParseExpr(lowered)
	if err != nil {
		t.Fatalf("the lowered expression %q does not parse as ClassAd: %v", lowered, err)
	}
	v := ex.Eval(ad)
	// A WHERE clause admits a row when the predicate is TRUE. Undefined and error both
	// drop it, exactly as SQL's NULL does -- that is the equivalence being tested, not
	// value equality.
	b, err := v.BoolValue()
	return err == nil && b, lowered
}

// TestStringEqualityIsCaseSensitive is the divergence this package exists for. ClassAd's `==`
// would admit the second ad; SQL's `=` must not.
func TestStringEqualityIsCaseSensitive(t *testing.T) {
	const q = `Owner = 'bbockelm'`

	if ok, lowered := evalOn(t, q, `Owner = "bbockelm"`); !ok {
		t.Errorf("exact match rejected (lowered: %s)", lowered)
	}
	if ok, lowered := evalOn(t, q, `Owner = "BBockelm"`); ok {
		t.Errorf("a differently-cased value matched; ClassAd's == folds case and SQL's = does not (lowered: %s)", lowered)
	}
	// And it must lower to the form the store indexes exactly, not to a function call.
	if _, lowered := evalOn(t, q, `Owner = "bbockelm"`); !strings.Contains(lowered, "=?=") {
		t.Errorf("lowered to %q; string equality must use =?=, which probe.go routes to the exact-case postings", lowered)
	}
}

// TestNullDropsTheRow: SQL's NULL comparison admits nothing, and neither may the lowering --
// for an absent attribute and for an explicitly undefined one alike.
func TestNullDropsTheRow(t *testing.T) {
	for _, ad := range []string{`Cpus = 4`, `Owner = undefined` + "\nCpus = 4"} {
		if ok, lowered := evalOn(t, `Owner = 'alice'`, ad); ok {
			t.Errorf("%q admitted by %q, want dropped", ad, lowered)
		}
	}
	// IS NULL is the form that asks about it, and must admit both.
	for _, ad := range []string{`Cpus = 4`, `Owner = undefined` + "\nCpus = 4"} {
		if ok, lowered := evalOn(t, `Owner IS NULL`, ad); !ok {
			t.Errorf("%q not admitted by %q, want admitted", ad, lowered)
		}
	}
	if ok, _ := evalOn(t, `Owner IS NOT NULL`, `Owner = "alice"`); !ok {
		t.Error("IS NOT NULL rejected a present value")
	}
	if ok, _ := evalOn(t, `Owner IS NOT NULL`, `Cpus = 4`); ok {
		t.Error("IS NOT NULL admitted an absent attribute")
	}
}

func TestNumericComparisons(t *testing.T) {
	cases := []struct {
		q    string
		ad   string
		want bool
	}{
		{`Cpus = 4`, `Cpus = 4`, true},
		{`Cpus = 4`, `Cpus = 5`, false},
		{`Cpus <> 4`, `Cpus = 5`, true},
		{`Cpus <> 4`, `Cpus = 4`, false},
		{`Cpus > 2`, `Cpus = 4`, true},
		{`Cpus > 2`, `Cpus = 1`, false},
		{`Cpus >= 4`, `Cpus = 4`, true},
		{`Cpus < 4`, `Cpus = 2`, true},
		{`Cpus <= 2`, `Cpus = 2`, true},
		{`Load > 0.5`, `Load = 0.75`, true},
		{`Load > 0.5`, `Load = 0.25`, false},
		// A numeric comparison against an absent attribute drops the row, as SQL's NULL
		// comparison does.
		{`Cpus > 2`, `Owner = "x"`, false},
		{`Cpus <> 4`, `Owner = "x"`, false},
		{`Completed = TRUE`, `Completed = true`, true},
		{`Completed = TRUE`, `Completed = false`, false},
		{`QDate > 1700000000`, `QDate = 1700000001`, true},
	}
	for _, c := range cases {
		got, lowered := evalOn(t, c.q, c.ad)
		if got != c.want {
			t.Errorf("%q over %q = %v, want %v (lowered: %s)", c.q, c.ad, got, c.want, lowered)
		}
	}
}

func TestBooleanOperatorsAndParens(t *testing.T) {
	cases := []struct {
		q    string
		ad   string
		want bool
	}{
		{`Cpus > 2 AND Owner = 'alice'`, `Cpus = 4` + "\n" + `Owner = "alice"`, true},
		{`Cpus > 2 AND Owner = 'alice'`, `Cpus = 1` + "\n" + `Owner = "alice"`, false},
		{`Cpus > 2 OR Owner = 'alice'`, `Cpus = 1` + "\n" + `Owner = "alice"`, true},
		{`(Cpus > 2 OR Cpus = 1) AND Owner = 'alice'`, `Cpus = 1` + "\n" + `Owner = "alice"`, true},
		{`NOT Cpus = 4`, `Cpus = 5`, true},
		{`NOT Cpus = 4`, `Cpus = 4`, false},
		// NOT over an absent numeric attribute: SQL gives NOT NULL = NULL, dropped.
		// ClassAd gives !undefined = undefined, also dropped.
		{`NOT Cpus = 4`, `Owner = "x"`, false},
		{`Cpus IN (1, 2, 4)`, `Cpus = 4`, true},
		{`Cpus IN (1, 2)`, `Cpus = 4`, false},
		{`Owner IN ('alice', 'bob')`, `Owner = "bob"`, true},
		{`Owner IN ('alice', 'bob')`, `Owner = "BOB"`, false},
		{`Cpus BETWEEN 2 AND 6`, `Cpus = 4`, true},
		{`Cpus BETWEEN 2 AND 6`, `Cpus = 8`, false},
		{`Cpus BETWEEN 2 AND 6`, `Owner = "x"`, false},
	}
	for _, c := range cases {
		got, lowered := evalOn(t, c.q, c.ad)
		if got != c.want {
			t.Errorf("%q over %q = %v, want %v (lowered: %s)", c.q, c.ad, got, c.want, lowered)
		}
	}
}

// TestRefusals: each of these is valid SQL that will not be lowered, and the refusal is the
// feature. Getting one wrong costs a missing capability; lowering one wrongly costs wrong rows,
// and only the first is recoverable from a bug report.
func TestRefusals(t *testing.T) {
	cases := []struct {
		q    string
		want string // a phrase the reason must contain
	}{
		{`Owner > 'R'`, "case-insensitively"},
		{`Owner < 'R'`, "case-insensitively"},
		{`Owner >= 'R'`, "case-insensitively"},
		{`Owner <> 'alice'`, "three-valued"},
		{`Owner != 'alice'`, "three-valued"},
		{`NOT Owner = 'alice'`, "negated equality"},
		{`NOT (Owner = 'alice')`, "negated equality"},
		{`Owner BETWEEN 'a' AND 'z'`, "ordering comparisons"},
		{`Nope = 'x'`, "unknown column"},
		{`Owner = 5`, "non-string literal"},
		{`Cpus = 'four'`, "non-numeric literal"},
		{`Completed = 1`, "non-boolean literal"},
		{`Cpus = Load`, "two columns"},
		{`upper(Owner) = 'A'`, ""},
		{`Owner LIKE 'a%'`, ""},
		{`Cpus + 1 = 5`, ""},
	}
	for _, c := range cases {
		_, err := Lower(c.q, cols)
		if err == nil {
			t.Errorf("%q: lowered, want a refusal", c.q)
			continue
		}
		var un *Unsupported
		if !errors.As(err, &un) {
			t.Errorf("%q: %T, want *Unsupported so a caller can tell a feature gap from a typo", c.q, err)
		}
		if c.want != "" && !strings.Contains(err.Error(), c.want) {
			t.Errorf("%q: reason %q does not mention %q", c.q, err, c.want)
		}
	}
}

// TestNegationRefusalIsPositionalNotGlobal: a string equality is fine in positive position even
// when a NOT appears elsewhere in the predicate. Refusing the whole predicate because it
// contains a NOT would be a much blunter rule than the problem warrants.
func TestNegationRefusalIsPositionalNotGlobal(t *testing.T) {
	if _, err := Lower(`NOT Cpus = 4 AND Owner = 'alice'`, cols); err != nil {
		t.Errorf("a string equality outside the NOT was refused: %v", err)
	}
	// Double negation returns to positive position, so it is allowed -- the rule is about
	// the 3VL at the comparison, not about the token appearing.
	if _, err := Lower(`NOT (NOT Owner = 'alice')`, cols); err != nil {
		t.Errorf("double negation refused: %v", err)
	}
}

// TestStringLiteralEscaping checks that a SQL literal's VALUE survives into the ClassAd
// expression. The ad is built programmatically rather than parsed from text: routing the value
// through a second parser would test the agreement of two escaping schemes rather than the
// lowering, and an earlier version of this test failed for exactly that reason.
func TestStringLiteralEscaping(t *testing.T) {
	for _, c := range []struct {
		sql   string
		value string
	}{
		{`Owner = 'a''b'`, `a'b`}, // SQL doubles a quote to escape it
		{`Owner = 'a"b'`, `a"b`},  // a double quote must be escaped on the way into ClassAd
		{`Owner = 'a\b'`, `a\b`},  // SQL does not treat a backslash as an escape; ClassAd does
		{`Owner = 'plain'`, `plain`},
	} {
		lowered, err := Lower(c.sql, cols)
		if err != nil {
			t.Errorf("%s: %v", c.sql, err)
			continue
		}
		ad := classad.New()
		ad.InsertAttrString("Owner", c.value)
		ex, perr := classad.ParseExpr(lowered)
		if perr != nil {
			t.Errorf("%s lowered to %q, which does not parse: %v", c.sql, lowered, perr)
			continue
		}
		b, berr := ex.Eval(ad).BoolValue()
		if berr != nil || !b {
			t.Errorf("%s did not match the value %q (lowered: %s)", c.sql, c.value, lowered)
		}
	}
}

func TestLexerRejectsGarbage(t *testing.T) {
	for _, q := range []string{`Owner = 'unterminated`, `Owner @ 5`, ``, `   `} {
		if _, err := Lower(q, cols); err == nil {
			t.Errorf("%q: accepted", q)
		}
	}
}
