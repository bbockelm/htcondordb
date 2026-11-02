package main

import (
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/golang-htcondor/logging"
	"github.com/bbockelm/htcondordb/arrowtype"
)

// TestFlightLimitsParsing: the limits are the endpoint's only protection against a query that
// wants more memory than the daemon has, so how a malformed value behaves matters.
//
// Falling back to unlimited is the right choice -- refusing to start over a typo in an optional
// knob would take a daemon down for less reason than it fixes -- but it is the OPPOSITE of what
// an operator who typed a limit asked for, so it must be reported rather than swallowed. Before
// this, an unparseable value read as 0 and nothing said so: the daemon ran with no ceiling and
// the only evidence was its absence.
func TestFlightLimitsParsing(t *testing.T) {
	logger, err := logging.New(&logging.Config{OutputPath: "stderr", SkipGlobalInstall: true})
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		raw  string
		want int64
	}{
		{"", 0},
		{"0", 0},
		{"1048576", 1 << 20},
		// A unit suffix, which the other size-valued knobs accept and which this one
		// claimed to accept before it did.
		{"512 MiB", 512 << 20},
		{"512MiB", 512 << 20},
		{"1 GB", 1_000_000_000},
		// Malformed: unlimited, and the operator is told.
		{"lots", 0},
		{"-5", 0},
		{"512 MB of RAM", 0},
	} {
		cfg := mkSyncCfg(t, "HTCONDORDB_FLIGHT_STREAM_MEMORY = "+c.raw+"\n")
		if got := flightBytes(cfg, logger, "HTCONDORDB_FLIGHT_STREAM_MEMORY"); got != c.want {
			t.Errorf("stream memory %q -> %d, want %d", c.raw, got, c.want)
		}
	}

	for _, c := range []struct {
		raw  string
		want int
	}{
		{"", 0},
		{"0", 0},
		{"8", 8},
		{"nine", 0},
		{"-1", 0},
		// A count is NOT a size: a suffix here is a mistake, not 8 million streams.
		{"8M", 0},
	} {
		cfg := mkSyncCfg(t, "HTCONDORDB_FLIGHT_MAX_STREAMS = "+c.raw+"\n")
		if got := flightCount(cfg, logger, "HTCONDORDB_FLIGHT_MAX_STREAMS"); got != c.want {
			t.Errorf("max streams %q -> %d, want %d", c.raw, got, c.want)
		}
	}
}

// TestFlightTimeAttrs: an epoch-seconds attribute cannot be inferred from its values -- an
// integer is an integer -- so the well-known job attributes are declared, and the knob adds to
// that set rather than replacing it. An operator naming one extra attribute must not silently
// lose QDate.
func TestFlightTimeAttrs(t *testing.T) {
	cfg := mkSyncCfg(t, "HTCONDORDB_FLIGHT_TIME_ATTRS = MyCustomTime, Another_Time\n")
	opts := flightDeriveOptions(cfg, testLogger(t))

	for _, name := range []string{"QDate", "qdate", "CompletionDate", "JobStartDate"} {
		if !opts.TimeAttrs[strings.ToLower(name)] {
			t.Errorf("%s is not recognized as an epoch-seconds attribute", name)
		}
	}
	for _, name := range []string{"MyCustomTime", "Another_Time"} {
		if !opts.TimeAttrs[strings.ToLower(name)] {
			t.Errorf("the configured attribute %s was not added", name)
		}
	}
	if opts.TimeAttrs["owner"] {
		t.Error("Owner was treated as a time attribute")
	}

	// With the knob unset the built-in set must still be there.
	if bare := flightDeriveOptions(mkSyncCfg(t, ""), testLogger(t)); !bare.TimeAttrs["qdate"] {
		t.Error("QDate is lost when the knob is unset")
	}
}

// TestFlightSchemaTTL: the schema cache's lifetime is the one Flight knob whose malformed-value
// fallback is NOT "no limit", and the difference is deliberate.
//
// The size and count knobs fall back to unlimited because that is what the endpoint did before
// they existed, so a typo cannot change behaviour. Here the pre-existing behaviour is the
// expensive one -- a profile scan per RPC -- so falling back to it on a typo would hand an
// operator who asked for a longer cache exactly the cost they were trying to avoid, with
// nothing but a log line to say why. So a bad value keeps the default, and an explicit 0 is
// honoured: disabling the cache is a legitimate thing to ask for.
func TestFlightSchemaTTL(t *testing.T) {
	logger, err := logging.New(&logging.Config{OutputPath: "stderr", SkipGlobalInstall: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		raw  string
		want time.Duration
	}{
		{"", defaultFlightSchemaTTL},
		{"0", 0},
		{"5", 5 * time.Second},
		{"90s", 90 * time.Second},
		{"2m", 2 * time.Minute},
		{"1h", time.Hour},
		// Malformed or negative: the default, and the operator is told.
		{"soon", defaultFlightSchemaTTL},
		{"-30", defaultFlightSchemaTTL},
		{"30 seconds", defaultFlightSchemaTTL},
	} {
		cfg := mkSyncCfg(t, "HTCONDORDB_FLIGHT_SCHEMA_TTL = "+c.raw+"\n")
		if got := flightSchemaTTL(cfg, logger); got != c.want {
			t.Errorf("schema TTL %q -> %v, want %v", c.raw, got, c.want)
		}
	}
}

// testLogger is a logger a config reader can warn through, since a malformed declaration is
// reported rather than failing startup.
func testLogger(t *testing.T) *logging.Logger {
	t.Helper()
	l, err := logging.New(&logging.Config{OutputPath: "stderr", SkipGlobalInstall: true})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// TestFlightOverridesAreReachable is §13.3's "implemented and unreachable": arrowtype has had
// Overrides and StrictMixedKinds since it was written, and the daemon wired only TimeAttrs, so
// an admin could declare an epoch attribute and nothing else.
//
// The exclusion is the one worth having first. Requirements and MemoryUsage are the same SHAPE
// -- both expressions in almost every ad, both evaluating to one clean kind -- and differ only
// in whether the value or the text is the content, which no measurement separates. Excluding
// one is not hiding it: the tail carries its text.
func TestFlightOverridesAreReachable(t *testing.T) {
	cfg := mkSyncCfg(t, "HTCONDORDB_FLIGHT_EXCLUDE_ATTRS = Requirements, Env\n"+
		"HTCONDORDB_FLIGHT_ATTR_KINDS = ImageSize:int, Score:real\n"+
		"HTCONDORDB_FLIGHT_STRICT_MIXED_KINDS = true\n")
	opts := flightDeriveOptions(cfg, testLogger(t))

	for _, name := range []string{"requirements", "env"} {
		if !opts.Overrides[name].Exclude {
			t.Errorf("%s was not excluded", name)
		}
	}
	if got := opts.Overrides["imagesize"].Kind; got != "int" {
		t.Errorf("ImageSize kind is %q, want int", got)
	}
	if got := opts.Overrides["score"].Kind; got != "real" {
		t.Errorf("Score kind is %q, want real", got)
	}
	if !opts.StrictMixedKinds {
		t.Error("strict mixed kinds was not enabled")
	}

	// A malformed or unknown declaration is dropped with a warning, not silently honoured
	// as something else and not fatal to startup. Honouring "ImageSize:intt" as a string
	// column would be a declaration the operator never made.
	bad := flightDeriveOptions(mkSyncCfg(t,
		"HTCONDORDB_FLIGHT_ATTR_KINDS = NoColon, ImageSize:intt, :real\n"), testLogger(t))
	if _, ok := bad.Overrides["imagesize"]; ok {
		t.Errorf("a malformed kind declaration produced an override: %v", bad.Overrides["imagesize"])
	}

	// With none of the knobs set, the seeded policy expressions are the only overrides and
	// strictness is off, so the rest of the schema is the measurement's.
	bare := flightDeriveOptions(mkSyncCfg(t, ""), testLogger(t))
	if !bare.Overrides["requirements"].Exclude {
		t.Error("Requirements is not excluded by default; a pool's schema would carry a column of meaningless booleans")
	}
	if bare.StrictMixedKinds {
		t.Error("strictness is on by default")
	}
	for name := range bare.Overrides {
		if !knownFlightPolicyExprs[name] {
			t.Errorf("unset knobs produced an override for %q, which is not a seeded policy expression", name)
		}
	}
}

// TestFlightOverridePrecedence: a contradictory configuration has to resolve the same way every
// time, not by which loop ran last.
//
// Naming a kind is an explicit "this should be a column", so it beats a seeded DEFAULT
// exclusion -- otherwise the default silently outranks the declaration, which is the no-op this
// knob exists to avoid. Naming an exclusion is an explicit "do not", so it beats everything,
// because between two explicit declarations the conservative one should win.
func TestFlightOverridePrecedence(t *testing.T) {
	// A kind for a seeded policy expression: typed, as asked.
	opts := flightDeriveOptions(mkSyncCfg(t, "HTCONDORDB_FLIGHT_ATTR_KINDS = Requirements:string\n"), testLogger(t))
	if ov := opts.Overrides["requirements"]; ov.Exclude || ov.Kind != "string" {
		t.Errorf("Requirements is %+v; a named kind must clear the default exclusion", ov)
	}

	// Both, for the same attribute: excluded.
	opts = flightDeriveOptions(mkSyncCfg(t, "HTCONDORDB_FLIGHT_ATTR_KINDS = ImageSize:int\n"+
		"HTCONDORDB_FLIGHT_EXCLUDE_ATTRS = ImageSize\n"), testLogger(t))
	if ov := opts.Overrides["imagesize"]; !ov.Exclude {
		t.Errorf("ImageSize is %+v; a named exclusion must win over a named kind", ov)
	}
}

// TestKnownKindsMatchTheDerivation keeps the validator and the thing it validates in step. A
// kind this list accepts and kindType does not would be accepted at startup and then ignored,
// which is the silent-no-op the validation exists to prevent.
func TestKnownKindsMatchTheDerivation(t *testing.T) {
	for _, kind := range arrowtype.KnownKinds() {
		opts := arrowtype.Options{Overrides: map[string]arrowtype.Override{"x": {Kind: kind}}}
		_, decisions := arrowtype.Schema([]arrowtype.Measured{{
			Name: "X", Sampled: 10, Present: 10, ValueClasses: map[string]int{"int": 10},
		}}, opts)
		if len(decisions) == 0 || decisions[0].Verdict != arrowtype.Typed {
			t.Errorf("kind %q is accepted by KnownKinds but produced %+v", kind, decisions)
		}
	}
	if arrowtype.KnownKind("intt") {
		t.Error("KnownKind accepts a kind the derivation does not know")
	}
}
