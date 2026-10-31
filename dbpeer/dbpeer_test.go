package dbpeer

import "testing"

// TestNilIsNotAPanic is the whole point of the package. An earlier version of
// this wiring reached through the negotiation inline and panicked on a
// connection that never handshook, which capi's pool test found only because
// it dials without CEDAR.
func TestNilIsNotAPanic(t *testing.T) {
	if got := Attrs(nil); got != nil {
		t.Errorf("Attrs(nil) = %v, want nil", got)
	}
}
