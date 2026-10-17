package cedarsync

import (
	"errors"
	"fmt"
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestNoSuchTableClassification: only the server's "no such table" refusal is a missing
// source; any other server error (a refusal, a bad cursor, a read-only table) and any transport
// error stays an ordinary failure, with the error backoff and a warning.
func TestNoSuchTableClassification(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{&dbrpc.ServerError{Msg: "no such table: job_metrics"}, true},
		{fmt.Errorf("watch: %w", &dbrpc.ServerError{Msg: "no such table: job_metrics"}), true},
		{&dbrpc.ServerError{Msg: "permission denied"}, false},
		{&dbrpc.ServerError{Msg: "bad cursor"}, false},
		{errors.New("no such table: job_metrics"), false}, // not from the server
		{errors.New("connection reset"), false},
	}
	for _, c := range cases {
		if got := isNoSuchTable(c.err); got != c.want {
			t.Errorf("isNoSuchTable(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}
