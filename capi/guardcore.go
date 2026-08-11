package main

// The panic-guard logic, in Go types. It lives apart from the C-facing wrappers in guard.go
// because cgo is not supported in test files: anything that touches C.char cannot be unit
// tested, so everything that can be is kept here instead.

import (
	"fmt"
	"os"
	"runtime/debug"
)

// A panic that unwinds out of an exported function does not become an error for the caller --
// it aborts the whole host process. For the Python driver that means a bug anywhere in the Go
// stack (the SQL executor, the ClassAd parser, a stale handle) kills the user's interpreter
// mid-report instead of raising something they can catch, log, or report.
//
// Every exported function therefore runs its body inside one of the guards here. A recovered
// panic becomes hcdbPanic (or a zero handle) with a message carrying the stack trace, which
// the driver raises as InternalError.
//
// What this does NOT cover: Go's unrecoverable runtime failures -- "concurrent map writes",
// stack exhaustion, out of memory. Those bypass recover by design and still abort. The guards
// turn bugs into errors, not the runtime into a sandbox.

// hcdbPanic is returned by an entry point whose body panicked. It is distinct from hcdbErr so
// the driver can tell "your query was rejected" from "you found a bug in htcondordb".
const hcdbPanic = -5

// panicPrefix marks a message produced by a recovered panic. hcdb_connect_err and
// hcdb_sql_ads report failure only as a string -- they return a handle, so there is no status
// code to carry hcdbPanic -- and the driver keys on this prefix to classify those two. It is a
// contract between this file and the driver's _library module; changing it changes both.
const panicPrefix = "internal error in "

// panicMessage renders a recovered panic for the caller. The stack trace goes in the message
// rather than to stderr so it reaches the caller's exception (and its logs) instead of
// scribbling on the output of a program that may have stderr pointed elsewhere.
func panicMessage(entry string, r any) string {
	return fmt.Sprintf("%s%s: %v -- this is a bug in htcondordb, not in the calling code; "+
		"please report it with the stack trace below.\n%s", panicPrefix, entry, r, debug.Stack())
}

// guardCode runs a body that yields a status code. On a panic it returns hcdbPanic and the
// message to hand back; message is empty on every other path, which is how the caller knows
// whether to overwrite an out-parameter the body may have set itself.
func guardCode(entry string, fn func() int) (code int, message string) {
	defer func() {
		if r := recover(); r != nil {
			code, message = hcdbPanic, panicMessage(entry, r)
		}
	}()
	return fn(), ""
}

// guardHandleValue runs a body that yields an opaque handle. On a panic it returns 0, which
// every caller already reads as failure, plus the message.
func guardHandleValue(entry string, fn func() uintptr) (h uintptr, message string) {
	defer func() {
		if r := recover(); r != nil {
			h, message = 0, panicMessage(entry, r)
		}
	}()
	return fn(), ""
}

// guardVoid runs an entry point with no way to report anything -- the free functions. A panic
// there has nowhere to go but stderr, which is still better than taking the process down: the
// caller was releasing something and has no error path to take.
func guardVoid(entry string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintln(os.Stderr, "htcondordb: "+panicMessage(entry, r))
		}
	}()
	fn()
}
