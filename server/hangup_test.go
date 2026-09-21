package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
)

// A client that hangs up between requests is the ordinary end of a session, and it must not be
// reported with an error string -- err="failed to read frame header: EOF" reads like a fault in
// a log an operator is scanning for faults. A truncated frame is a different thing and must NOT
// be swallowed by the same test, so both are asserted here.
func TestPeerHangupIsDistinguishedFromTruncation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		hangup bool
	}{
		{"clean hangup on a frame boundary", io.EOF, true},
		{"wrapped as the stream layer wraps it", fmt.Errorf("failed to read frame header: %w", io.EOF), true},
		{"truncated frame header", io.ErrUnexpectedEOF, false},
		{"wrapped truncation", fmt.Errorf("failed to read frame header: %w", io.ErrUnexpectedEOF), false},
		{"daemon shutting down", context.Canceled, false},
		{"listener closed", net.ErrClosed, false},
		{"no error at all", nil, false},
		{"a real fault", errors.New("cipher: message authentication failed"), false},
	} {
		if got := isPeerHangup(tc.err); got != tc.hangup {
			t.Errorf("%s: isPeerHangup(%v) = %v, want %v", tc.name, tc.err, got, tc.hangup)
		}
	}
}

// Everything isPeerHangup accepts must also be a clean close, or a hangup would fall through to
// the Warn branch and become exactly the noise this is meant to remove.
func TestPeerHangupImpliesCleanClose(t *testing.T) {
	for _, err := range []error{io.EOF, fmt.Errorf("failed to read frame header: %w", io.EOF)} {
		if isPeerHangup(err) && !isCleanClose(err) {
			t.Errorf("isPeerHangup(%v) but not isCleanClose: it would be logged at Warn", err)
		}
	}
}
