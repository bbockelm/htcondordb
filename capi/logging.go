package main

/*
#include <stdlib.h>
// stdint.h for uintptr_t: glibc does not pull it in via stdlib.h the way macOS does, so
// without this the package builds on a Mac and fails on Linux with "could not determine
// what C.uintptr_t refers to".
#include <stdint.h>
*/
import "C"

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

// A library embedded in someone else's process should not write to their stderr unasked.
//
// cedar's transport and security code logs through slog's default logger -- the daemon routes it
// into its own log with slog.SetDefault -- so every connect used to print several INFO lines
// (session negotiation, key exchange, the security ClassAd) into the host program's stderr, with
// no way to turn them off from Python. In a notebook or a cron report that is noise the caller
// cannot silence and did not ask for.
//
// So the default here is silence, and the knob is explicit: HTCONDORDB_LOG_LEVEL in the
// environment, or hcdb_set_log_level. Nothing is lost by default -- failures reach the caller as
// errors, which is the contract for a library; the log is for diagnosing, not reporting.

var logOnce sync.Once

// configureLogging installs the log destination once, from HTCONDORDB_LOG_LEVEL. Called at the
// first connection rather than in init() so the environment the host pushed in (see hcdb_setenv)
// is already in place.
func configureLogging() {
	logOnce.Do(func() {
		setLogLevel(os.Getenv("HTCONDORDB_LOG_LEVEL"))
	})
}

// setLogLevel points slog's default at stderr for the named level, or at nothing for "off" (the
// default, and what an empty or unrecognized name gets).
func setLogLevel(name string) bool {
	level, on := parseLogLevel(name)
	if !on {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
		return strings.TrimSpace(name) == "" || strings.EqualFold(strings.TrimSpace(name), "off")
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	return true
}

func parseLogLevel(name string) (slog.Level, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "debug":
		return slog.LevelDebug, true
	case "info":
		return slog.LevelInfo, true
	case "warn", "warning":
		return slog.LevelWarn, true
	case "error":
		return slog.LevelError, true
	default:
		return 0, false
	}
}

// Sets where the library logs and how much: "off" (the default), "error", "warn", "info" or
// "debug". Anything else, including NULL, is treated as "off". Returns hcdbOK for a recognized
// name and hcdbErr otherwise, having gone quiet either way.
//
// Logging goes to stderr, which is the only destination a C caller and a Python caller agree on.
// A host that wants it elsewhere should capture stderr.
//
//export hcdb_set_log_level
func hcdb_set_log_level(level *C.char) C.int {
	return guardStatus("hcdb_set_log_level", nil, func() C.int {
		name := ""
		if level != nil {
			name = C.GoString(level)
		}
		// Take the once, so a later connection does not reset what was just asked for.
		logOnce.Do(func() {})
		if !setLogLevel(name) {
			return hcdbErr
		}
		return hcdbOK
	})
}
