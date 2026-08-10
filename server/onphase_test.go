package server

import (
	"testing"
	"time"
)

// TestNewReportsPhases pins that New attributes its internal steps. The reason this matters is a
// concrete one: a startup phase named "open-database" reported 4.6s on a real deployment, and the
// catalog open turned out to be a small fraction of it -- the name sent the reader to the wrong
// code. Every step New takes has to be nameable, or the next slow start is another investigation.
func TestNewReportsPhases(t *testing.T) {
	var got []string
	total := time.Duration(0)
	svc, err := New(Config{
		Dir:       t.TempDir(),
		Authorize: allowAll,
		OnPhase: func(name string, d time.Duration) {
			got = append(got, name)
			total += d
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	// Every step New performs, in the order it performs them.
	want := []string{"catalog-open", "ensure-tables", "rpc-server", "maintenance-start", "txn-reaper"}
	if len(got) != len(want) {
		t.Fatalf("reported steps %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("step %d = %q, want %q (order matters: the durations are deltas)", i, got[i], want[i])
		}
	}
	if total <= 0 {
		t.Error("reported durations sum to zero")
	}
}

// TestNewWithoutOnPhase covers the nil case: reporting is opt-in and must not be required.
func TestNewWithoutOnPhase(t *testing.T) {
	svc, err := New(Config{Dir: t.TempDir(), Authorize: allowAll})
	if err != nil {
		t.Fatal(err)
	}
	svc.Close()
}
