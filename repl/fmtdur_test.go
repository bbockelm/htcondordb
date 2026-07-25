package repl

import (
	"testing"
	"time"
)

func TestFmtDur(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{2*time.Millisecond + 239930*time.Nanosecond, "2.24ms"},                  // was 2.23993ms
		{375*time.Millisecond + 554493*time.Nanosecond, "375.55ms"},              // was 375.554493ms
		{3*time.Hour + 2*time.Minute + 17*time.Second + 615994679, "3h2m17.62s"}, // was 3h2m17.615994679s
		{0, "0s"},
		{500 * time.Nanosecond, "500ns"},
		{1500 * time.Nanosecond, "1.5µs"},
	}
	for _, c := range cases {
		if got := fmtDur(c.in); got != c.want {
			t.Errorf("fmtDur(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}
