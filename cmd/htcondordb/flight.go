package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
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
// ONE DIFFERENCE FROM THAT PRECEDENT. The metrics listener's own comment says it "carries no
// secrets -- storage sizes and timings -- but bind it to a trusted interface". This one carries
// AD CONTENTS, so a trusted interface is not the control; the authorization policy is.
//
// CONFIGURATION:
//
//	HTCONDORDB_FLIGHT_ADDRESS          ":9877" -- enables the endpoint
//	HTCONDORDB_FLIGHT_AUTHENTICATION   OPTIONAL (default) | SCITOKEN | NONE
//	HTCONDORDB_FLIGHT_SCITOKEN_ISSUERS trusted issuers, required for SCITOKEN
//	HTCONDORDB_FLIGHT_STREAM_MEMORY    bytes one stream may hold (0 = unlimited)
//	HTCONDORDB_FLIGHT_MAX_STREAMS      concurrent streams (0 = unlimited)
//	HTCONDORDB_FLIGHT_TIME_ATTRS       extra epoch-seconds attributes
//
// Authentication follows CEDAR rather than being stricter: OPTIONAL by default, which verifies
// a credential when one is offered and otherwise serves the anonymous identity, leaving
// ALLOW_READ to decide. The endpoint therefore adds no exposure of its own -- what an anonymous
// caller may read here is what the same caller may already read over CEDAR, decided by the same
// policy, so an operator who has tightened one has tightened both. REQUIRED is available for a
// pool that wants this door stricter than that one.

// startFlightSQL starts the Flight SQL endpoint if HTCONDORDB_FLIGHT_ADDRESS is set. It
// returns a stop function, or nil if the endpoint is not configured.
func startFlightSQL(ctx context.Context, cfg *config.Config, log *logging.Logger, cat flightsvc.Catalog, authorize flightsvc.Authorizer) (func(), error) {
	addr := getStr(cfg, "HTCONDORDB_FLIGHT_ADDRESS")
	if addr == "" {
		return nil, nil
	}

	authn, err := flightAuthenticator(cfg)
	if err != nil {
		return nil, err
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
	fsrv, err := flightsvc.NewSessionFlightServer(sessions, nil, flightsvc.Settings{
		// The build stamp, so a client's adbc_get_info() reports this daemon rather
		// than the placeholder.
		Version: version,
		// Both default to unlimited when unset, which is what the endpoint did before
		// the limits existed. An operator who has not configured them gets the old
		// behaviour rather than a surprise refusal.
		//
		// A MALFORMED value is reported rather than swallowed. Falling back to
		// unlimited is the only option that cannot wedge startup over a typo, but it
		// is also the opposite of what an operator who typed a limit asked for, so
		// doing it silently would leave a daemon running with no ceiling and nothing
		// to say why. configBytes takes a unit suffix ("512 MiB"), as the other
		// size-valued knobs do.
		StreamBudgetBytes: flightBytes(cfg, log, "HTCONDORDB_FLIGHT_STREAM_MEMORY"),
		MaxStreams:        flightCount(cfg, log, "HTCONDORDB_FLIGHT_MAX_STREAMS"),
	})
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

// flightAuthenticator builds the configured mechanism.
//
// The default is OPTIONAL, which is what HTCondor's SEC_*_AUTHENTICATION defaults to and what
// CEDAR therefore does: verify a credential when one is offered, and otherwise serve the
// anonymous identity and let ALLOW_READ decide. That keeps one policy governing both doors.
// REQUIRED is available for a pool that wants this endpoint stricter than its CEDAR one.
//
// Permissive is not open. An anonymous caller still has to satisfy ALLOW_READ, still gets READ
// level only, and still has private attributes stripped -- what changes is that the pool's
// policy decides rather than this code.
func flightAuthenticator(cfg *config.Config) (flightsvc.Authenticator, error) {
	method := getStr(cfg, "HTCONDORDB_FLIGHT_AUTHENTICATION")
	if method == "" {
		method = "OPTIONAL"
	}
	switch strings.ToUpper(method) {
	case "NONE":
		return flightsvc.AnonymousAuthenticator(), nil
	case "OPTIONAL":
		// With no issuers configured there is no credential this could verify, so
		// OPTIONAL degenerates to anonymous rather than erroring: an operator who
		// configured no token issuers has not asked for token authentication.
		sci, err := flightSciToken(cfg)
		if err != nil {
			return flightsvc.AnonymousAuthenticator(), nil
		}
		return flightsvc.OptionalAuth(sci), nil
	case "SCITOKEN", "REQUIRED":
		return flightSciToken(cfg)
	default:
		return nil, fmt.Errorf("unknown HTCONDORDB_FLIGHT_AUTHENTICATION %q (known: OPTIONAL, SCITOKEN, NONE)", method)
	}
}

// flightSciToken builds the SciToken verifier. Issuers are required because cedar's verifier
// accepts nothing without them, mirroring HTCondor's own SCITOKENS map-file behaviour.
func flightSciToken(cfg *config.Config) (flightsvc.Authenticator, error) {
	return flightsvc.SciTokenAuthenticator(flightsvc.SciTokenConfig{
		Issuers:   getList(cfg, "HTCONDORDB_FLIGHT_SCITOKEN_ISSUERS"),
		Audiences: getList(cfg, "SCITOKENS_SERVER_AUDIENCE"),
	})
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

// flightBytes reads a byte-valued limit, accepting a unit suffix, and REPORTS a value it cannot
// parse instead of quietly leaving the endpoint unlimited.
//
// Unlimited is the right fallback -- refusing to start over a typo in an optional knob would
// take a daemon down for less reason than it fixes -- but it is also the opposite of the
// operator's intent, which is exactly why it has to be said out loud.
func flightBytes(cfg *config.Config, log *logging.Logger, key string) int64 {
	raw := strings.TrimSpace(getStr(cfg, key))
	n := configBytes(cfg, key)
	if raw != "" && raw != "0" && n == 0 {
		log.Error(logging.DestinationGeneral,
			"unparseable Flight SQL size; serving with no limit", "knob", key, "value", raw)
	}
	return n
}

// flightCount reads a count-valued limit, reporting a malformed or negative value for the same
// reason flightBytes does.
func flightCount(cfg *config.Config, log *logging.Logger, key string) int {
	raw := strings.TrimSpace(getStr(cfg, key))
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		log.Error(logging.DestinationGeneral,
			"unparseable Flight SQL count; serving with no limit", "knob", key, "value", raw)
		return 0
	}
	return n
}
