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
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"sync"
	"time"
)

// PROFILING THE CLIENT INSIDE SOMEONE ELSE'S DAEMON.
//
// This library runs embedded in the schedd, so when the schedd answers condor_q from a mirror,
// the Go side of that work is invisible: the daemon's own profiler sees C++, and the mirror's
// profiler sees only what reaches the mirror. A cost that lives in between -- goroutines that
// accumulate across queries, a decode loop that slows as it is reused -- shows up in neither,
// and the only symptom is that queries get slower the longer the daemon has been serving them.
//
// HTCONDORDB_CLIENT_PPROF is the window into that half. Unset (the default) nothing listens.
//
// It is OFF by default and must stay that way: the host is frequently a root daemon, and the
// endpoint hands out heap contents, goroutine stacks and a CPU profile on request. Binding it
// anywhere but loopback publishes those, so a non-loopback address is logged as a warning
// rather than silently accepted -- the operator may have meant it, but they should see it in
// the log either way.
var pprofOnce sync.Once

// configurePprof starts the diagnostic HTTP server if HTCONDORDB_CLIENT_PPROF names an address.
// Called at the first connection rather than from init() so an address the host pushed in with
// hcdb_setenv is already in place, which is the same reason configureLogging is called there.
func configurePprof() {
	pprofOnce.Do(func() {
		addr := strings.TrimSpace(os.Getenv("HTCONDORDB_CLIENT_PPROF"))
		if addr == "" {
			return
		}
		if !isLoopbackAddr(addr) {
			slog.Warn("htcondordb client profiler is NOT on loopback: heap, goroutine stacks "+
				"and CPU profiles are reachable from the network", "address", addr)
		}
		// Listen before returning, so a bad address or a taken port is reported now rather
		// than swallowed by a goroutine nobody is watching.
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			slog.Error("htcondordb client profiler could not listen; continuing without it",
				"address", addr, "error", err)
			return
		}
		// Its own mux, not http.DefaultServeMux: net/http/pprof registers on the default one
		// from its init(), and a library has no business putting handlers on a mux its host
		// might also be using.
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		srv := &http.Server{
			Handler: mux,
			// A CPU profile is fetched with ?seconds=N and legitimately takes that long, so
			// there is no write timeout to cut it short; the header timeout is what keeps a
			// stalled client from holding a connection open indefinitely.
			ReadHeaderTimeout: 10 * time.Second,
		}
		slog.Info("htcondordb client profiler listening", "address", ln.Addr().String())
		go func() {
			if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
				slog.Error("htcondordb client profiler stopped", "error", err)
			}
		}()
	})
}

// isLoopbackAddr reports whether addr's host is a loopback address. A host that does not parse
// as an IP (a name, or an empty host meaning "all interfaces") is NOT treated as loopback: the
// point is to warn when the endpoint may be reachable, and "probably fine" is not that.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
