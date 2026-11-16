package federate

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func stateLogHub(buf *bytes.Buffer) *Hub {
	return &Hub{log: slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))}
}

// TestSourceStateLogsTheReason: sources{state="absent"} is a count, and the three ways an AP can be
// absent want three different responses -- it left the pool, it is refusing the hub, or it has no
// htcondordb to federate from. The reason separating them was written only to the source's
// federation_sources row, which takes a read of the hub to see, and a hub you are unsure of is
// exactly the one that is hard to read.
func TestSourceStateLogsTheReason(t *testing.T) {
	var buf bytes.Buffer
	h := stateLogHub(&buf)
	s := &source{schedd: "ce2000.chtc.wisc.edu"}

	h.logSourceState(s, StateAbsent, "no spoke advertises MirroredScheddName for this schedd")
	out := buf.String()
	for _, want := range []string{"ce2000.chtc.wisc.edu", StateAbsent, "no spoke advertises MirroredScheddName"} {
		if !strings.Contains(out, want) {
			t.Errorf("first observation did not log %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "was=") {
		t.Errorf("a first observation has no previous state to report:\n%s", out)
	}

	// A steady federation must be silent: the same state and reason again says nothing.
	buf.Reset()
	h.logSourceState(s, StateAbsent, "no spoke advertises MirroredScheddName for this schedd")
	if buf.Len() != 0 {
		t.Errorf("an unchanged state logged again:\n%s", buf.String())
	}

	// A change reports both sides, so a transition is readable without the previous line.
	buf.Reset()
	h.logSourceState(s, StateFresh, "")
	out = buf.String()
	if !strings.Contains(out, "state="+StateFresh) || !strings.Contains(out, "was="+StateAbsent) {
		t.Errorf("a change did not report both sides:\n%s", out)
	}
}

// TestSourceStateLevels: fresh is the state nothing is wrong with. Everything else is a warning,
// so a hub that is not fully federated says so at a level an operator filters for.
func TestSourceStateLevels(t *testing.T) {
	for _, tc := range []struct {
		state string
		level string
	}{
		{StateFresh, "level=INFO"},
		{StateAbsent, "level=WARN"},
		{StateStale, "level=WARN"},
		{StateUntrusted, "level=WARN"},
		{StateRetiring, "level=WARN"},
	} {
		t.Run(tc.state, func(t *testing.T) {
			var buf bytes.Buffer
			h := stateLogHub(&buf)
			h.logSourceState(&source{schedd: "ap1"}, tc.state, "because")
			if !strings.Contains(buf.String(), tc.level) {
				t.Errorf("state %q logged at the wrong level, want %s:\n%s", tc.state, tc.level, buf.String())
			}
		})
	}
}

// TestSourceStateReasonChangeAloneLogs: the state can stay absent while the reason changes from
// "refusing the hub" to "no spoke at all". That is a different problem and has to be visible.
func TestSourceStateReasonChangeAloneLogs(t *testing.T) {
	var buf bytes.Buffer
	h := stateLogHub(&buf)
	s := &source{schedd: "dap2000"}
	h.logSourceState(s, StateAbsent, "first reason")
	buf.Reset()
	h.logSourceState(s, StateAbsent, "second reason")
	if !strings.Contains(buf.String(), "second reason") {
		t.Errorf("a reason change on an unchanged state was not logged:\n%s", buf.String())
	}
}
