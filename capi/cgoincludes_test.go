package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestEveryCgoFileIncludesStdint: a cgo file here that uses a fixed-width or pointer-width C type
// must include <stdint.h> itself.
//
// IT BUILDS ON A MAC EITHER WAY. macOS pulls stdint.h in through stdlib.h; glibc does not. So a
// new file that leaves it out compiles clean on the machine it was written on, passes every local
// test, and fails in CI with "could not determine what C.uintptr_t refers to" -- pointing at a
// type the author never wrote down rather than at a missing include.
//
// That has now happened twice, which is what makes this a test rather than a comment. Checking it
// here costs nothing and fails on the laptop, which is the whole point: the failure it replaces
// takes a CI round trip to see.
//
// It checks the PREAMBLE rather than the use, deliberately. A file that includes stdint.h without
// needing it costs a line; a file that needs it without including it costs a push, and no cgo file
// in this package has a reason not to have it.
func TestEveryCgoFileIncludesStdint(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	preamble := regexp.MustCompile(`(?s)/\*(.*?)\*/\s*import "C"`)
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatal(err)
		}
		src := string(raw)
		if !strings.Contains(src, "import \"C\"") {
			continue
		}
		checked++
		m := preamble.FindStringSubmatch(src)
		if m == nil {
			t.Errorf("%s imports C with no preamble comment, so it can include nothing", name)
			continue
		}
		if !strings.Contains(m[1], "stdint.h") {
			t.Errorf("%s does not include <stdint.h>. It builds on macOS, where stdlib.h pulls "+
				"it in, and fails on Linux with \"could not determine what C.uintptr_t refers "+
				"to\". Add it to the preamble, as the other files here do.", name)
		}
	}
	// A rename or a move that stopped this finding any cgo files at all would pass silently,
	// which is the way a check like this usually dies.
	if checked < 5 {
		t.Fatalf("only %d cgo files found in this package; the check is no longer looking at "+
			"what it was written for", checked)
	}
}
