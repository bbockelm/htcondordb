package main

/*
#include <stdlib.h>
// stdint.h for uintptr_t: glibc does not pull it in via stdlib.h the way macOS does, so
// without this the package builds on a Mac and fails on Linux with "could not determine
// what C.uintptr_t refers to".
#include <stdint.h>
*/
import "C"

// The C-facing half of the panic guards; the logic and its tests are in guardcore.go.

// guardStatus runs an entry point that returns a status code and reports failure through
// *out. On a panic it writes the message to *out (when out is non-nil) and returns hcdbPanic.
// A body that set *out itself before returning normally keeps it.
func guardStatus(entry string, out **C.char, fn func() C.int) C.int {
	code, message := guardCode(entry, func() int { return int(fn()) })
	if message != "" && out != nil {
		*out = C.CString(message)
	}
	return C.int(code)
}

// guardHandle runs an entry point that returns an opaque handle. On a panic it writes the
// message to *errOut (when non-nil) and returns 0.
func guardHandle(entry string, errOut **C.char, fn func() C.uintptr_t) C.uintptr_t {
	h, message := guardHandleValue(entry, func() uintptr { return uintptr(fn()) })
	if message != "" && errOut != nil {
		*errOut = C.CString(message)
	}
	return C.uintptr_t(h)
}

// Panics on purpose, and returns hcdbPanic with the panic's message in *out having done so.
//
// This exists to be called by the test suite: that a panic inside a real entry point reaches
// the caller as an error, rather than aborting the process, is the property the guards exist
// for, and it cannot be tested from outside the library without a body that panics. Calling
// this in earnest does nothing but produce that error.
//
//export hcdb_selftest_panic
func hcdb_selftest_panic(out **C.char) C.int {
	return guardStatus("hcdb_selftest_panic", out, func() C.int {
		panic("deliberate panic from hcdb_selftest_panic")
	})
}
