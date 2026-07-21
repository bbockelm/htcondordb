package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
)

// TestIsCleanClose: ordinary connection ends are Debug-only; an authentication/decode
// failure or an unexpected transport error is surfaced (logged at Warn).
func TestIsCleanClose(t *testing.T) {
	clean := []error{
		nil,
		io.EOF,
		io.ErrUnexpectedEOF,
		context.Canceled,
		net.ErrClosed,
		fmt.Errorf("read: %w", io.EOF), // wrapped
	}
	for _, err := range clean {
		if !isCleanClose(err) {
			t.Errorf("isCleanClose(%v) = false, want true (ordinary close)", err)
		}
	}

	dirty := []error{
		errors.New("failed to decrypt message: AES-GCM decryption failed: cipher: message authentication failed"),
		errors.New("dbrpc: bad frame header"),
		context.DeadlineExceeded,
	}
	for _, err := range dirty {
		if isCleanClose(err) {
			t.Errorf("isCleanClose(%v) = true, want false (anomaly worth logging)", err)
		}
	}
}
