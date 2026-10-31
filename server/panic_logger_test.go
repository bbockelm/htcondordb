package server

import (
	"log/slog"
	"testing"
)

// A read that panics is failed alone and reported only through the dbrpc server's logger, so that
// logger has to be the service's. Left unset it is the process default, which only the daemon's own
// main redirects into its log -- an embedder, or a test, would see the panic go to stderr or nowhere.
func TestRPCServerLogsThroughServiceLogger(t *testing.T) {
	log := slog.New(&capturingHandler{})
	svc, err := New(Config{Dir: t.TempDir(), Logger: log, DisableMaintenance: true,
		Authorize: func(_, _, _ string) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = svc.Close() }()
	if svc.RPC().Logger != log {
		t.Fatal("the dbrpc server does not log through the service's logger; a contained panic would " +
			"not reach the daemon log")
	}
}
