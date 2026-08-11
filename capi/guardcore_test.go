package main

import (
	"strings"
	"testing"
)

// The guards have to be transparent when nothing goes wrong.
func TestGuardCodePassesThrough(t *testing.T) {
	code, message := guardCode("test", func() int { return hcdbBadSQL })
	if code != hcdbBadSQL {
		t.Errorf("code = %d, want %d", code, hcdbBadSQL)
	}
	// Empty is the signal that the body's own out-parameter must not be overwritten.
	if message != "" {
		t.Errorf("message = %q, want empty on the success path", message)
	}
}

func TestGuardCodeRecovers(t *testing.T) {
	code, message := guardCode("hcdb_example", func() int {
		var m map[string]string
		m["boom"] = "" // assignment to a nil map: an ordinary bug, panics at runtime
		return hcdbOK
	})
	if code != hcdbPanic {
		t.Fatalf("code = %d, want hcdbPanic (%d)", code, hcdbPanic)
	}
	// The message has to say where, what, whose fault, and carry the stack: someone filing a
	// bug report from a Python traceback has nothing else to go on.
	for _, want := range []string{panicPrefix, "hcdb_example", "nil map", "bug in htcondordb", "goroutine"} {
		if !strings.Contains(message, want) {
			t.Errorf("message does not contain %q:\n%s", want, message)
		}
	}
}

// A panic value that is not a string still has to render.
func TestGuardCodeRecoversNonStringPanic(t *testing.T) {
	_, message := guardCode("hcdb_example", func() int { panic(errFake{}) })
	if !strings.Contains(message, "fake failure") {
		t.Errorf("message = %q, want the panic value rendered", message)
	}
}

type errFake struct{}

func (errFake) Error() string { return "fake failure" }

func TestGuardHandleValue(t *testing.T) {
	h, message := guardHandleValue("test", func() uintptr { return 42 })
	if h != 42 || message != "" {
		t.Errorf("guardHandleValue = %d, %q; want 42 passed through", h, message)
	}

	h, message = guardHandleValue("hcdb_example", func() uintptr { panic("boom") })
	if h != 0 {
		t.Errorf("handle = %d, want 0 -- every caller reads that as failure", h)
	}
	if !strings.HasPrefix(message, panicPrefix) {
		t.Errorf("message %q lacks the prefix the driver classifies on", message)
	}
}

// guardVoid has nowhere to report, so the only requirement is that it returns at all.
func TestGuardVoidRecovers(t *testing.T) {
	ran := false
	guardVoid("test", func() { ran = true })
	if !ran {
		t.Error("body did not run")
	}
	guardVoid("test", func() { panic("boom") })
}

// The prefix is a contract with the driver, which classifies these as InternalError rather than
// OperationalError. Pin it so a reword here cannot silently change what Python raises.
func TestPanicPrefixIsTheDriverContract(t *testing.T) {
	if panicPrefix != "internal error in " {
		t.Errorf("panicPrefix = %q; PANIC_PREFIX in python/htcondordb/_library.py must match", panicPrefix)
	}
	if !strings.HasPrefix(panicMessage("hcdb_sql", "boom"), panicPrefix) {
		t.Error("panicMessage does not start with panicPrefix")
	}
}
