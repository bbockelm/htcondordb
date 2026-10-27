// Letting a profiler reach this library's goroutines inside somebody else's
// process.
//
// Everything expensive here happens on goroutines the C caller never sees, in a
// process that is not a Go program: no `go test -bench`, no runtime/pprof entry
// point the caller could call, and no way to tell the library's CPU from the
// daemon's in a sampling profiler attached from outside. Without this, the only
// instrument available for "why is a query costing what it costs" is the wall
// clock at the far end of a socket.
//
// So: when HTCONDORDB_CLIENT_PPROF names an address, the standard pprof
// endpoints are served on it. It is opt-in, it binds wherever the operator says
// (loopback unless they ask otherwise), and it is off in every process that does
// not set the variable -- the same trade the daemon makes with its own
// HTCONDORDB_ENABLE_PPROF.
package main

import (
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"sync"
)

var profileOnce sync.Once

// startProfileServer serves pprof on HTCONDORDB_CLIENT_PPROF's address, once per
// process. A listener that cannot be opened is reported through the library's
// own log and otherwise ignored: a profiling knob must never be the reason a
// daemon fails to answer a query.
func startProfileServer() {
	profileOnce.Do(func() {
		addr := os.Getenv("HTCONDORDB_CLIENT_PPROF")
		if addr == "" {
			return
		}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			slog.Error("pprof: cannot listen", "addr", addr, "err", err)
			return
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		slog.Info("pprof: serving profiling endpoints", "addr", ln.Addr().String())
		go func() { _ = http.Serve(ln, mux) }()
	})
}
