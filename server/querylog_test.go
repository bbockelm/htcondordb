package server

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// capturingHandler records the records it is asked to log, for assertions.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) find(msg string) (slog.Record, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Message == msg {
			return r, true
		}
	}
	return slog.Record{}, false
}

// TestQueryLogHook covers the HTCONDORDB_LOG_QUERIES wiring: the hook is nil when
// logging is off (no per-query overhead, nothing logged) and, when on, logs each
// query through the service logger with the query's fields.
func TestQueryLogHook(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		s := &Service{logQueries: false, log: slog.New(&capturingHandler{})}
		if hook := s.queryLogHook("alice@pool", "192.0.2.1:5000"); hook != nil {
			t.Fatal("queryLogHook must be nil when HTCONDORDB_LOG_QUERIES is off")
		}
	})

	t.Run("enabled", func(t *testing.T) {
		cap := &capturingHandler{}
		s := &Service{logQueries: true, log: slog.New(cap)}
		hook := s.queryLogHook("alice@pool", "192.0.2.1:5000")
		if hook == nil {
			t.Fatal("queryLogHook must be non-nil when HTCONDORDB_LOG_QUERIES is on")
		}
		hook(dbrpc.QueryLog{
			Op: "QueryRaw", Table: "Startd", Constraint: "true",
			Limit: 0, Rows: 5046, Duration: 12 * time.Millisecond,
		})
		rec, ok := cap.find("htcondordb query")
		if !ok {
			t.Fatal("query was not logged")
		}
		attrs := map[string]slog.Value{}
		rec.Attrs(func(a slog.Attr) bool { attrs[a.Key] = a.Value; return true })
		for k, want := range map[string]string{
			"op": "QueryRaw", "table": "Startd", "user": "alice@pool", "remote": "192.0.2.1:5000",
		} {
			if got := attrs[k].String(); got != want {
				t.Errorf("log attr %s = %q, want %q", k, got, want)
			}
		}
		if attrs["rows"].Int64() != 5046 {
			t.Errorf("log attr rows = %d, want 5046", attrs["rows"].Int64())
		}
	})
}

// TestNewLogQueries checks the Config field is copied into the Service so the
// daemon's HTCONDORDB_LOG_QUERIES param actually reaches the hook.
func TestNewLogQueries(t *testing.T) {
	svc, err := New(Config{Authorize: allowAll, LogQueries: true})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	if !svc.logQueries {
		t.Fatal("Config.LogQueries=true did not set Service.logQueries")
	}
}
