package scheddsync

import (
	"testing"
	"time"
)

// TestNextDelay pins the idle backoff policy. It is a pure function precisely so these
// cases do not have to be observed through a running tailer, where checking that a delay
// reached its cap would mean sitting through every doubling on the way there.
func TestNextDelay(t *testing.T) {
	const base = 200 * time.Millisecond
	const max = 2 * time.Second
	cases := []struct {
		name       string
		cur        time.Duration
		progressed bool
		want       time.Duration
	}{
		{"progress snaps back from the cap", max, true, base},
		{"progress snaps back mid-climb", 800 * time.Millisecond, true, base},
		{"progress at base stays at base", base, true, base},
		{"idle doubles", base, false, 400 * time.Millisecond},
		{"idle doubles again", 400 * time.Millisecond, false, 800 * time.Millisecond},
		{"idle clamps to the cap", 1600 * time.Millisecond, false, max},
		{"idle holds at the cap", max, false, max},
		{"a delay below base returns to base", time.Millisecond, false, base},
		{"doubling cannot overflow past the cap", time.Duration(1) << 62, false, max},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := nextDelay(c.cur, base, max, c.progressed); got != c.want {
				t.Errorf("nextDelay(%v, %v, %v, %v) = %v, want %v", c.cur, base, max, c.progressed, got, c.want)
			}
		})
	}
}

// TestNextDelayFixedRate covers the configuration that opts out: idleMax == base must never
// produce anything but base, so an operator who wants a steady poll rate gets one.
func TestNextDelayFixedRate(t *testing.T) {
	const base = 500 * time.Millisecond
	d := base
	for i := 0; i < 20; i++ {
		d = nextDelay(d, base, base, false)
		if d != base {
			t.Fatalf("iteration %d: delay drifted to %v with idleMax == base", i, d)
		}
	}
}

// TestNextDelayReachesCap walks the policy the way Run does, asserting it converges on the
// cap rather than overshooting or oscillating.
func TestNextDelayReachesCap(t *testing.T) {
	const base = 200 * time.Millisecond
	const max = 2 * time.Second
	d := base
	for i := 0; i < 50; i++ {
		d = nextDelay(d, base, max, false)
		if d < base || d > max {
			t.Fatalf("iteration %d: delay %v outside [%v, %v]", i, d, base, max)
		}
	}
	if d != max {
		t.Fatalf("after 50 idle polls delay = %v, want the cap %v", d, max)
	}
}
