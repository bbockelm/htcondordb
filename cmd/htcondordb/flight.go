package main

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"

	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/logging"

	"github.com/bbockelm/htcondordb/arrowtype"
	"github.com/bbockelm/htcondordb/flightsvc"
)

// The Arrow Flight SQL listener: opt-in, on its own port, and refusing to start without an
// authenticator.
//
// NOT THROUGH SHARED PORT. Daemon.Listener resolves the command socket through an inherited
// shared-port endpoint, an inherited listener, or a self-registered named socket -- all CEDAR
// routing a gRPC client cannot speak. So this binds its own port, following the precedent the
// daemon already sets for its metrics/pprof listener.
//
// ONE DIFFERENCE FROM THAT PRECEDENT, and it matters. The metrics listener's own comment says
// it "carries no secrets -- storage sizes and timings -- but bind it to a trusted interface".
// This one carries AD CONTENTS. "Trusted interface" is not an adequate control for that, so
// authentication is mandatory and the listener refuses to start without it rather than falling
// back to something permissive. There is no way to configure an open Flight endpoint by
// omission, which is the only safe shape for a default that cannot be tightened later.

// startFlightSQL starts the Flight SQL endpoint if HTCONDORDB_FLIGHT_ADDRESS is set. It
// returns a stop function, or nil if the endpoint is not configured.
func startFlightSQL(ctx context.Context, cfg *config.Config, log *logging.Logger, cat flightsvc.Catalog, authorize flightsvc.Authorizer) (func(), error) {
	addr := getStr(cfg, "HTCONDORDB_FLIGHT_ADDRESS")
	if addr == "" {
		return nil, nil
	}

	authn, err := flightAuthenticator(cfg, log)
	if err != nil {
		// Fail the startup rather than serving unauthenticated. An operator who set the
		// address meant to serve; serving the job queue to anyone who connects is not
		// what they meant, and a warning in a log is not a control.
		return nil, fmt.Errorf("HTCONDORDB_FLIGHT_ADDRESS is set but no authentication is configured: %w", err)
	}

	sessions, err := flightsvc.NewSessions(cat, authn, authorize, flightDeriveOptions(cfg))
	if err != nil {
		return nil, err
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		sessions.Close()
		return nil, fmt.Errorf("flight listener on %s: %w", addr, err)
	}

	gsrv := grpc.NewServer(
		grpc.UnaryInterceptor(sessions.UnaryAuthInterceptor()),
		grpc.StreamInterceptor(sessions.StreamAuthInterceptor()),
	)
	fsrv, err := flightsvc.NewSessionFlightServer(sessions, nil)
	if err != nil {
		_ = ln.Close()
		sessions.Close()
		return nil, err
	}
	flight.RegisterFlightServiceServer(gsrv, fsrv)

	log.Info(logging.DestinationGeneral, "serving Arrow Flight SQL",
		"address", ln.Addr().String(), "authentication", authn.Name())

	go func() {
		if err := gsrv.Serve(ln); err != nil {
			log.Error(logging.DestinationGeneral, "flight listener stopped", "err", err.Error())
		}
	}()
	go func() {
		<-ctx.Done()
		gsrv.GracefulStop()
		sessions.Close()
	}()

	return func() { gsrv.Stop(); sessions.Close() }, nil
}

// flightAuthenticator builds the configured mechanism. Exactly one is available today
// (SciToken bearer credentials), and an unrecognised or absent setting is an error rather than
// a fallback -- see the comment on startFlightSQL for why there is no permissive default.
func flightAuthenticator(cfg *config.Config, log *logging.Logger) (flightsvc.Authenticator, error) {
	switch method := getStr(cfg, "HTCONDORDB_FLIGHT_AUTHENTICATION"); method {
	case "SCITOKEN", "scitoken":
		// Issuers are required: cedar's verifier accepts nothing without them, mirroring
		// HTCondor's own SCITOKENS map-file behaviour, so an endpoint configured without
		// any could never admit a caller.
		return flightsvc.SciTokenAuthenticator(flightsvc.SciTokenConfig{
			Issuers:   getList(cfg, "HTCONDORDB_FLIGHT_SCITOKEN_ISSUERS"),
			Audiences: getList(cfg, "SCITOKENS_SERVER_AUDIENCE"),
		})
	case "":
		return nil, fmt.Errorf("set HTCONDORDB_FLIGHT_AUTHENTICATION (SCITOKEN)")
	default:
		return nil, fmt.Errorf("unknown HTCONDORDB_FLIGHT_AUTHENTICATION %q (known: SCITOKEN)", method)
	}
}

// flightDeriveOptions carries the type declarations into the endpoint: which attributes are
// epoch seconds, and the per-attribute overrides for the decisions type inference cannot make.
//
// Seeded from the same list the CLI and the Grafana plugin carry, which is the same knowledge
// written down three times now -- it wants unifying behind the stored type map, and this is
// the third site that will have to change when that lands.
func flightDeriveOptions(cfg *config.Config) arrowtype.Options {
	opts := arrowtype.Options{TimeAttrs: map[string]bool{}}
	for name := range knownFlightTimeAttrs {
		opts.TimeAttrs[name] = true
	}
	for _, name := range getList(cfg, "HTCONDORDB_FLIGHT_TIME_ATTRS") {
		opts.TimeAttrs[lower(name)] = true
	}
	return opts
}

var knownFlightTimeAttrs = map[string]bool{
	"qdate": true, "jobstartdate": true, "jobcurrentstartdate": true,
	"enteredcurrentstatus": true, "completiondate": true, "lastheardfrom": true,
	"daemonstarttime": true, "lastmatchtime": true, "joblaststartdate": true,
	"shadowbday": true, "enteredcurrentactivity": true, "mycurrenttime": true,
	"lastbenchmark": true,
}

// getList reads a configuration parameter as a list, split on commas and whitespace, the way
// HTCondor writes a list-valued parameter. Empty entries are dropped so a trailing comma does
// not become an empty issuer -- which, with AcceptIssuers, would be an issuer nothing matches
// rather than one that matches everything, but is still not what anybody meant.
func getList(cfg *config.Config, key string) []string {
	raw := getStr(cfg, key)
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func lower(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
