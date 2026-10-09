package repl

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestReadOnlyTableRefusal: the gate's refusal is recognized (for the hint and the owner lookup)
// and its table recovered, while other server errors are not mistaken for it.
func TestReadOnlyTableRefusal(t *testing.T) {
	refusal := fmt.Errorf("commit: %w", &dbrpc.ServerError{Msg: `read-only table "job \"x\"": opNewAd not permitted`})
	if got := ReadOnlyTable(refusal); got != `job "x"` {
		t.Errorf("ReadOnlyTable = %q, want %q", got, `job "x"`)
	}
	if h := HintFor(refusal); !strings.Contains(h, "maintained by a writer inside the daemon") {
		t.Errorf("HintFor = %q", h)
	}
	other := &dbrpc.ServerError{Msg: "read-only connection: opNewAd not permitted"}
	if errors.Is(other, dbrpc.ErrTableReadOnly) || ReadOnlyTable(other) != "" {
		t.Error("a READ-only connection refusal must not read as an owned-table refusal")
	}
	if h := HintFor(other); !strings.Contains(h, "READ-only") {
		t.Errorf("HintFor(read-only connection) = %q", h)
	}
}
