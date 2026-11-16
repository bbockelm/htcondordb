package main

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/logging"
)

// The observability listener (HTCONDORDB_METRICS_ADDRESS) is bound BEFORE the catalog opens.
//
// Opening a catalog is the longest thing this daemon does -- on a federation hub, minutes per
// large archive -- and it is exactly when an operator wants a profile: a start that is taking
// longer than it should is indistinguishable from one that is wedged, and pprof is how you tell.
// The listener used to be brought up after the open, so for the whole of that window a scrape or a
// `go tool pprof` got a connection refused, which reads as "the daemon is dead" rather than "the
// daemon is busy doing the thing you want to look at".
//
// /metrics cannot be served that early -- its collectors read the catalog, which does not exist
// yet -- so until the catalog is up it answers 503 with what the startup heartbeat knows. That is
// the useful answer anyway: a scraper gets a response that says "still opening, here is how far",
// not a dial error.
type obsServer struct {
	srv     *http.Server
	metrics atomic.Pointer[http.Handler]
	ready   atomic.Bool
	status  func() string
}

// startObservability binds the listener and serves pprof (when enabled) and /metrics immediately.
// It returns nil when no address is configured. The returned server's SetMetrics installs the real
// /metrics handler once the catalog is open.
func startObservability(cfg *config.Config, log *logging.Logger, status func() string) *obsServer {
	addr := getStr(cfg, "HTCONDORDB_METRICS_ADDRESS")
	if addr == "" {
		return nil
	}
	o := &obsServer{status: status}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", o.serveMetrics)
	// Served from the first moment the process has a listener, so "what is it doing" has an
	// answer during the open and not only after it.
	mux.HandleFunc("/startupz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if o.ready.Load() {
			_, _ = fmt.Fprintln(w, "catalog open; serving")
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintln(w, o.statusLine())
	})
	// pprof (opt-in via HTCONDORDB_ENABLE_PPROF): profiling endpoints on the same trusted
	// listener, so a memory/CPU anomaly in a live daemon -- or a slow start -- is one
	// `go tool pprof http://.../debug/pprof/heap` away instead of a blind restart. Off by
	// default: profiles expose internals (symbol names, allocation sites) beyond the
	// storage-size counters /metrics carries. These take no storage lock, so they answer
	// while the catalog open holds them.
	if configBool(cfg, "HTCONDORDB_ENABLE_PPROF") {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		log.Info(logging.DestinationGeneral, "pprof profiling endpoints enabled (HTCONDORDB_ENABLE_PPROF)")
	}
	o.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Info(logging.DestinationGeneral, "serving Prometheus metrics", "address", addr,
			"note", "pprof and /startupz answer from here immediately; /metrics once the catalog is open")
		if err := o.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error(logging.DestinationGeneral, "metrics endpoint failed", "err", err.Error())
		}
	}()
	return o
}

// Close stops the listener. Safe on a nil server.
func (o *obsServer) Close() {
	if o == nil || o.srv == nil {
		return
	}
	_ = o.srv.Close()
}

// SetMetrics installs the real /metrics handler. Safe on a nil server (no address configured).
func (o *obsServer) SetMetrics(h http.Handler) {
	if o == nil {
		return
	}
	o.metrics.Store(&h)
	o.ready.Store(true)
}

func (o *obsServer) serveMetrics(w http.ResponseWriter, r *http.Request) {
	if h := o.metrics.Load(); h != nil {
		(*h).ServeHTTP(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintf(w, "# the catalog is still opening; no table metrics yet\n# %s\n", o.statusLine())
}

func (o *obsServer) statusLine() string {
	if o.status == nil {
		return "opening the catalog"
	}
	return o.status()
}
