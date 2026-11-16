package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/golang-htcondor/logging"
)

func closeLogger(t *testing.T) (*logging.Logger, func() string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "log")
	// The zero Verbosity is VerbosityError, which drops every Info line the heartbeat emits and
	// leaves the test asserting against an empty file.
	lg, err := logging.New(&logging.Config{
		OutputPath: path, SkipGlobalInstall: true, DefaultLevel: logging.VerbosityInfo,
		DestinationLevels: map[logging.Destination]logging.Verbosity{logging.DestinationGeneral: logging.VerbosityInfo},
	})
	if err != nil {
		t.Fatal(err)
	}
	return lg, func() string {
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Fatal(rerr)
		}
		return string(b)
	}
}

// TestCloseHeartbeatNamesTheInFlightPhase is the whole point: the long step of a shutdown is
// draining the RPC server, and it was reported only once it had already finished. A heartbeat that
// says what is running now is what tells a slow close from a hung one -- and what lets a
// termination grace period be set from measurement instead of from the open timings, which measure
// different work.
func TestCloseHeartbeatNamesTheInFlightPhase(t *testing.T) {
	prev := closeProgressEvery
	closeProgressEvery = 10 * time.Millisecond
	defer func() { closeProgressEvery = prev }()

	lg, read := closeLogger(t)
	ct := newCloseTimer(lg)
	ct.record("stop-reaper", 3*time.Millisecond)
	ct.begin("drain-rpc")
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(read(), "still closing the store") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	out := read()
	if !strings.Contains(out, "still closing the store") {
		t.Fatalf("no heartbeat while a phase was in flight:\n%s", out)
	}
	if !strings.Contains(out, "phase=drain-rpc") {
		t.Errorf("the heartbeat did not name the in-flight phase:\n%s", out)
	}
	if !strings.Contains(out, "stop-reaper=3ms") {
		t.Errorf("the heartbeat did not report what had already finished:\n%s", out)
	}
	ct.record("drain-rpc", 50*time.Millisecond)
	fields := ct.finish()
	var joined string
	for _, f := range fields {
		joined += " " + strings.TrimSpace(strings.Trim(stringOf(f), "\""))
	}
	for _, want := range []string{"stop-reaper", "drain-rpc", "seconds"} {
		if !strings.Contains(joined, want) {
			t.Errorf("the final line omits %q: %s", want, joined)
		}
	}
}

// TestCloseHeartbeatSilentOnAFastClose: a close that beats the interval logs nothing extra, so the
// normal case stays two lines.
func TestCloseHeartbeatSilentOnAFastClose(t *testing.T) {
	prev := closeProgressEvery
	closeProgressEvery = time.Hour
	defer func() { closeProgressEvery = prev }()

	lg, read := closeLogger(t)
	ct := newCloseTimer(lg)
	ct.begin("drain-rpc")
	ct.record("drain-rpc", time.Millisecond)
	_ = ct.finish()
	if strings.Contains(read(), "still closing") {
		t.Errorf("a fast close emitted a heartbeat:\n%s", read())
	}
}

// TestCloseTimerFinishIsIdempotentlySafeAfterPhases: finish must stop the beat goroutine, so a test
// (or a daemon) that closes twice does not leak it or panic on a second close of the channel.
func TestCloseTimerFinishStopsTheBeat(t *testing.T) {
	prev := closeProgressEvery
	closeProgressEvery = 5 * time.Millisecond
	defer func() { closeProgressEvery = prev }()

	lg, read := closeLogger(t)
	ct := newCloseTimer(lg)
	ct.begin("close-catalog")
	_ = ct.finish()
	before := len(read())
	time.Sleep(40 * time.Millisecond)
	if after := len(read()); after != before {
		t.Errorf("the heartbeat kept logging after finish: %d -> %d bytes", before, after)
	}
}

func stringOf(v any) string {
	switch x := v.(type) {
	case string:
		return x
	default:
		return ""
	}
}
