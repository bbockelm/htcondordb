package server

import (
	"context"
	"net"
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestQueryStatsEnabledByDefault covers the wiring that decides whether `.slow` can
// answer at all: aggregation is on unless the daemon turned it off, so an operator does
// not have to have predicted, before the slow query happened, that they would want it.
func TestQueryStatsEnabledByDefault(t *testing.T) {
	svc, err := New(Config{Authorize: allowAll})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	rep := reportFrom(t, svc)
	if !rep.Enabled {
		t.Fatal("query aggregation is off on a default service")
	}
}

// TestQueryStatsDisabledByConfig covers the opt-out.
func TestQueryStatsDisabledByConfig(t *testing.T) {
	svc, err := New(Config{Authorize: allowAll, DisableQueryStats: true})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	rep := reportFrom(t, svc)
	if rep.Enabled {
		t.Fatal("DisableQueryStats did not reach the rpc server")
	}
}

// reportFrom reads the service's aggregate over a served connection, at DAEMON.
func reportFrom(t *testing.T, svc *Service) *dbrpc.QueryStatsReport {
	t.Helper()
	cp, sp := net.Pipe()
	go func() { _ = svc.RPC().ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	defer c.Close()
	rep, err := c.QueryStats(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	return rep
}
