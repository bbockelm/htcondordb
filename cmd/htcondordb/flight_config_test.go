package main

import (
	"strings"
	"testing"

	"github.com/bbockelm/golang-htcondor/logging"
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
	opts := flightDeriveOptions(cfg)

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
	if bare := flightDeriveOptions(mkSyncCfg(t, "")); !bare.TimeAttrs["qdate"] {
		t.Error("QDate is lost when the knob is unset")
	}
}
