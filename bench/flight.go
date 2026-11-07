package bench

import (
	"context"
	"fmt"
	"net"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
	"github.com/bbockelm/htcondordb/flightsvc"
)

// The Flight SQL endpoint, for the typed consumer.
//
// Benchmark mode enables it unconditionally: a comparison that covered only the ClassAd
// transports would miss the EVALUATED form entirely, and that form is both the cheapest measured
// here and the one a typed consumer has no alternative to.
//
// WHAT THE DERIVE OPTIONS ARE FOR, now that the profile evaluates. An expression-valued
// attribute -- MemoryUsage in these ads, as in 1996 of 2000 on a real access point -- does get a
// typed column on its own: the profile carries evaluated counts and arrowtype prefers them. So
// the override here is not what makes the column exist, and it was checked rather than assumed
// (removing it leaves the column int and populated).
//
// It is kept because a benchmark's schema must not depend on sampling. An override is a declared
// decision that wins over the measurement, so the column is the same kind on every machine and
// at every row count, where a derived one could differ if a sample saw different values. The
// TimeAttrs are the half that genuinely changes the schema: without them the epoch-second
// attributes stay integers instead of timestamps, and a consumer's date functions have nothing
// to work on.

// DefaultDeriveOptions are the Arrow type decisions for the generated dataset: its epoch-second
// attributes declared as timestamps, and its expression-valued attribute declared so it has a
// column at all.
func DefaultDeriveOptions() arrowtype.Options {
	return arrowtype.Options{
		TimeAttrs: map[string]bool{
			"qdate": true, "jobstartdate": true, "completiondate": true,
			"enteredcurrentstatus": true, "jobcurrentstartdate": true, "servertime": true,
		},
		Overrides: map[string]arrowtype.Override{
			"memoryusage": {Kind: "int"},
		},
	}
}

// anonymousAuth maps every peer to the anonymous identity. It exists only for benchmark mode,
// which serves generated data from a guarded scratch directory; nothing else in this repository
// should authenticate this way, which is why it is unexported and lives beside the guard rather
// than in flightsvc.
type anonymousAuth struct{}

func (anonymousAuth) Authenticate(context.Context, metadata.MD) (string, error) { return "", nil }
func (anonymousAuth) Name() string                                              { return "anonymous (benchmark mode)" }

// allowAnonymousRead authorizes READ for anyone and nothing else. The levels that would let a
// peer write or retune the store are refused here as they are on the dbrpc side, so the two
// endpoints agree about what an unauthenticated peer may do.
func allowAnonymousRead(perm, _, _ string) bool { return perm == "READ" }

// ServeFlight starts the Flight SQL endpoint and returns the address it bound. The caller closes
// the Server to stop it.
func (s *Server) ServeFlight(ctx context.Context, listen string) (string, error) {
	if listen == "" {
		return "", nil
	}
	// The catalog is reached through a dbrpc server, which is what flightsvc.Catalog is: the
	// same in-process path the daemon gives it.
	cat := dbrpc.NewServerCatalog(s.Catalog)
	cat.EnableQueryStats(dbrpc.QueryStatsOptions{})
	derive := DefaultDeriveOptions()
	sessions, err := flightsvc.NewSessions(cat, anonymousAuth{}, allowAnonymousRead, derive)
	if err != nil {
		cat.Close()
		return "", fmt.Errorf("building flight sessions: %w", err)
	}
	svc, err := flightsvc.NewSessionServer(sessions, derive, nil)
	if err != nil {
		cat.Close()
		return "", fmt.Errorf("building flight server: %w", err)
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		cat.Close()
		return "", fmt.Errorf("listening on %s: %w", listen, err)
	}
	gsrv := grpc.NewServer()
	flight.RegisterFlightServiceServer(gsrv, flightsvc.NewFlightServer(svc, nil))
	go func() { _ = gsrv.Serve(ln) }()
	go func() { <-ctx.Done(); gsrv.Stop() }()
	prev := s.stop
	s.stop = func() {
		gsrv.Stop()
		cat.Close()
		if prev != nil {
			prev()
		}
	}
	s.FlightAddr = ln.Addr().String()
	return s.FlightAddr, nil
}
