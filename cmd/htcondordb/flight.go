package main

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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
//	HTCONDORDB_FLIGHT_SCHEMA_TTL       how long a derived schema is reused (0 = no cache)
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

	sessions, err := flightsvc.NewSessions(cat, authn, authorize, flightDeriveOptions(cfg, log))
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
		// A short schema cache by default. Deriving a schema samples records, and
		// every statement needs one -- twice, since GetFlightInfo answers the shape
		// and DoGet builds the encoder -- so a client looping over a prepared
		// statement was paying a profile scan per call inside the daemon. Short
		// because the schema follows the table: 30 seconds collapses a burst without
		// holding yesterday's columns.
		SchemaTTL: flightSchemaTTL(cfg, log),
		// Where each table's declared columns live across restarts. The cache above
		// stops the schema moving within its TTL; this stops it moving at all, which
		// is what a consumer caching a catalog needs. Beside the database rather than
		// in a directory of its own: it describes that database's tables and is
		// meaningless without them, so the two should be backed up and moved together.
		SchemaFile: flightSchemaFile(cfg, log),
		Warn: func(m string) {
			log.Warn(logging.DestinationGeneral, m)
		},
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
// The epoch attributes come from arrowtype, which is where the Grafana plugin reads them too.
// They were written down twice, in two modules, and the second copy had drifted -- so a column
// could graph as a time in one place and as a large integer in the other.
func flightDeriveOptions(cfg *config.Config, log *logging.Logger) arrowtype.Options {
	opts := arrowtype.Options{TimeAttrs: arrowtype.DefaultTimeAttrs()}
	// Configured attributes are ADDED rather than replacing the built-ins: an operator
	// naming one extra must not silently lose QDate.
	for _, name := range getList(cfg, "HTCONDORDB_FLIGHT_TIME_ATTRS") {
		opts.TimeAttrs[lower(name)] = true
	}
	// Three passes, and the ORDER is the precedence rule. The seeds are a default, a named
	// kind is an explicit "type this" that beats a default, and a named exclusion is an
	// explicit "do not" that beats everything -- so a contradictory configuration resolves
	// the conservative way rather than by which loop happened to run last.
	override := func(name string, set func(*arrowtype.Override)) {
		if opts.Overrides == nil {
			opts.Overrides = map[string]arrowtype.Override{}
		}
		ov := opts.Overrides[lower(name)]
		set(&ov)
		opts.Overrides[lower(name)] = ov
	}

	// The policy expressions, excluded by default for the reason the knob below exists: an
	// attribute whose TEXT is the content, which the derivation cannot tell from one whose
	// VALUE is the content because the two are the same shape. Measured on a real AP,
	// Requirements evaluates to a boolean in 1685 of 2000 ads -- so it would get a column,
	// and that column would be 85% meaningless booleans and 15% null.
	//
	// Seeded rather than left to every deployment to discover, the same way the epoch
	// attributes are. Excluding is not hiding: the text still arrives in the tail column.
	for name := range knownFlightPolicyExprs {
		override(name, func(ov *arrowtype.Override) { ov.Exclude = true })
	}
	// Forcing a column's type, for the attribute whose declared kind is known and whose
	// sample happens not to show it. A forced kind the data does not hold is allowed and is
	// not silent -- the encoder counts every value it then cannot store.
	//
	// Naming a kind also clears a DEFAULT exclusion: it is an unambiguous statement that this
	// attribute should be a column, and a seeded default that quietly outranked it would be
	// the silent no-op this whole knob exists to avoid.
	for _, spec := range getList(cfg, "HTCONDORDB_FLIGHT_ATTR_KINDS") {
		name, kind, ok := strings.Cut(spec, ":")
		name, kind = strings.TrimSpace(name), lower(strings.TrimSpace(kind))
		if !ok || name == "" || kind == "" {
			log.Error(logging.DestinationGeneral,
				"ignoring a Flight SQL attribute kind declaration; want Attribute:kind",
				"knob", "HTCONDORDB_FLIGHT_ATTR_KINDS", "entry", spec)
			continue
		}
		if !arrowtype.KnownKind(kind) {
			log.Error(logging.DestinationGeneral,
				"ignoring a Flight SQL attribute kind declaration; unknown kind",
				"knob", "HTCONDORDB_FLIGHT_ATTR_KINDS", "entry", spec, "kind", kind,
				"known", strings.Join(arrowtype.KnownKinds(), ", "))
			continue
		}
		override(name, func(ov *arrowtype.Override) { ov.Kind, ov.Exclude = kind, false })
	}
	// An attribute an admin wants out of the schema whatever the measurement says, and
	// whatever else was declared about it.
	for _, name := range getList(cfg, "HTCONDORDB_FLIGHT_EXCLUDE_ATTRS") {
		override(name, func(ov *arrowtype.Override) { ov.Exclude = true })
	}
	// Refusing an attribute whose kinds genuinely disagree, rather than serving it as text.
	// Off by default because a text column is a faithful representation of mixed data; an
	// operator who would rather hear about it than read it turns this on.
	opts.StrictMixedKinds = configBool(cfg, "HTCONDORDB_FLIGHT_STRICT_MIXED_KINDS")
	return opts
}

// knownFlightPolicyExprs are the attributes whose EXPRESSION is what anyone wants, so a typed
// column of what they evaluate to is worse than no column: it answers a question nobody asked
// and hides the one thing that was useful. They are HTCondor's matchmaking and lifecycle
// policy, where an expression evaluated outside the context it was written for is meaningless
// rather than wrong -- Requirements with no machine to match against, PeriodicRemove with no
// elapsed time.
//
// Being a declaration is the point, not a shortcoming. The same shape with the opposite answer
// is MemoryUsage, an expression in nearly every ad whose evaluated integer IS the content, and
// nothing in the data tells the two apart.
var knownFlightPolicyExprs = map[string]bool{
	"requirements": true, "rank": true, "start": true,
	"periodichold": true, "periodicrelease": true, "periodicremove": true,
	"onexithold": true, "onexitremove": true, "leavejobinqueue": true,
	"isowner": true, "withinresourcelimits": true,
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

// flightSchemaFile resolves where the declared column list is kept.
//
// HTCONDORDB_FLIGHT_SCHEMA_FILE if set; otherwise beside the database, which is the only
// durable directory this daemon is given. An in-memory database has no such directory and gets
// no file -- the declaration then lives for the process, which still keeps a schema still
// within one session and is the most that can be promised with nowhere to write.
//
// An explicit "none" turns persistence off with a database directory present, for a host that
// would rather re-measure than keep state it has to reason about.
func flightSchemaFile(cfg *config.Config, log *logging.Logger) string {
	if v, ok := cfg.Get("HTCONDORDB_FLIGHT_SCHEMA_FILE"); ok {
		v = strings.TrimSpace(v)
		if strings.EqualFold(v, "none") {
			return ""
		}
		if v != "" {
			return v
		}
	}
	dir := resolveDBDir(cfg)
	if dir == "" {
		log.Info(logging.DestinationGeneral,
			"no database directory, so Flight column types are declared in memory only; "+
				"they will be re-measured after a restart")
		return ""
	}
	return filepath.Join(dir, "flight-schema.json")
}

// defaultFlightSchemaTTL is how long a derived table schema is reused when the knob is unset.
const defaultFlightSchemaTTL = 30 * time.Second

// flightSchemaTTL reads the schema cache's lifetime. An explicit "0" disables the cache and is
// not an error; anything unparseable falls back to the default and is reported.
//
// Note the fallback differs from the size and count knobs above, deliberately. Those fall back
// to "no limit" because that is what the endpoint did before they existed, so a typo cannot
// change behaviour. Here the pre-existing behaviour is the EXPENSIVE one, and silently
// restoring it on a typo would hand an operator who asked for a longer cache a daemon that
// re-derives every schema on every RPC, with nothing but this log line to say why.
func flightSchemaTTL(cfg *config.Config, log *logging.Logger) time.Duration {
	raw := strings.TrimSpace(getStr(cfg, "HTCONDORDB_FLIGHT_SCHEMA_TTL"))
	if raw == "" {
		return defaultFlightSchemaTTL
	}
	secs, ok := configSeconds(cfg, "HTCONDORDB_FLIGHT_SCHEMA_TTL")
	if !ok || secs < 0 {
		log.Error(logging.DestinationGeneral,
			"unparseable Flight SQL schema cache lifetime; using the default",
			"knob", "HTCONDORDB_FLIGHT_SCHEMA_TTL", "value", raw,
			"default", defaultFlightSchemaTTL.String())
		return defaultFlightSchemaTTL
	}
	return time.Duration(secs) * time.Second
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
