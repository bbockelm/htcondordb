package flightsvc

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	peerpkg "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// Serving Flight from inside the daemon, without a second authorization story.
//
// flightsvc reads through a dbrpc CLIENT, and in process the daemon holds the server side. The
// bridge is a loopback: each Flight connection gets its own in-process dbrpc session, served
// with the ServeOptions its authenticated identity earns.
//
// That is not a workaround, it is the point. dbrpc's server already enforces read-only,
// private-attribute stripping and the private-constraint gate, and the encoder REQUIRES ads
// already redacted to what the reader may see -- a requirement it cannot check for itself.
// Going straight to the catalog would mean reimplementing all of that beside it, and the two
// copies would drift. Through the loopback, a Flight reader gets exactly what the same
// identity would get over CEDAR, by construction rather than by a second policy kept in
// agreement. The cost is a pipe and a goroutine per connection, and ad text crossing it --
// the rung-0 cost already accounted for.

// Authenticator turns a connection's gRPC metadata into a mapped HTCondor identity.
//
// An EMPTY user with no error is the anonymous peer, and it is a legitimate answer. That
// follows CEDAR: `peerUser` returns "" for a peer that authenticated with nothing
// (server.go:518), the ALLOW_/DENY_ policy is then asked about that empty identity, and
// whatever it says is the answer. Matching it means this endpoint adds no exposure of its own
// -- what an anonymous caller may read is whatever the pool's ALLOW_READ already grants an
// anonymous CEDAR caller, decided by the same policy, and an operator who has tightened one
// has tightened both.
//
// The distinction that still matters is between presenting NOTHING and presenting something
// BAD. No credential is anonymous; a credential that fails to verify is an error and refuses
// the connection, because a caller who meant to authenticate and failed should be told so
// rather than quietly downgraded to anonymous and then puzzled by what is missing.
type Authenticator interface {
	// Authenticate returns the mapped user, "" for an anonymous peer, or an error to
	// refuse the connection outright.
	Authenticate(ctx context.Context, md metadata.MD) (string, error)
	// Name identifies the mechanism in logs and refusals.
	Name() string
}

// Authorizer reports whether an identity holds an HTCondor authorization level. It is the same
// shape as server.Authorizer and authz.Policy.Authorize, so the daemon passes the policy it
// already has rather than growing a second one.
type Authorizer func(perm, peerAddr, user string) bool

// Catalog is the dbrpc server-side handle the loopback serves from.
type Catalog interface {
	ServeConnOpts(conn dbrpc.MsgConn, opts dbrpc.ServeOptions) error
}

// Sessions builds a per-connection Flight service over an in-process dbrpc loopback.
type Sessions struct {
	cat    Catalog
	authn  Authenticator
	authz  Authorizer
	derive arrowtype.Options

	mu   sync.Mutex
	open map[*session]struct{}
}

type session struct {
	client *dbrpc.Client
}

// NewSessions requires an authenticator. A nil one is a programming error rather than a
// permissive default, because the failure mode of a permissive default here is an open,
// unauthenticated read endpoint onto the job queue.
func NewSessions(cat Catalog, authn Authenticator, authorize Authorizer, derive arrowtype.Options) (*Sessions, error) {
	if cat == nil {
		return nil, fmt.Errorf("flightsvc: no catalog")
	}
	if authn == nil {
		return nil, fmt.Errorf("flightsvc: no authenticator; an unauthenticated Flight endpoint is refused rather than defaulted")
	}
	if authorize == nil {
		return nil, fmt.Errorf("flightsvc: no authorizer")
	}
	return &Sessions{cat: cat, authn: authn, authz: authorize, derive: derive,
		open: map[*session]struct{}{}}, nil
}

// levelOptions mirrors the daemon's own mapping from authorization level to dbrpc options.
//
// Flight is READ-ONLY whatever the peer holds: writes are a non-goal, and enabling an endpoint
// must never be the thing that widens what a principal can do. A DAEMON-authorized peer does
// see private attributes, matching what it would see over CEDAR -- withholding them here would
// be a second, quieter policy.
func levelOptions(authorize Authorizer, addr, user string) dbrpc.ServeOptions {
	switch {
	case authorize("DAEMON", addr, user):
		return dbrpc.ServeOptions{ReadOnly: true, IncludePrivate: true, Privileged: true}
	default:
		return dbrpc.ServeOptions{ReadOnly: true, IncludePrivate: false}
	}
}

// For authenticates the RPC and returns a dbrpc client scoped to its identity, plus a release
// to call when the RPC's work -- including any stream it starts -- is done.
//
// It hands back a CLIENT rather than a whole Flight service because one Flight server now
// serves every identity: what varies per RPC is the authorization the reads go through, and
// that is exactly what the client carries.
func (s *Sessions) For(ctx context.Context) (*dbrpc.Client, func(), error) {
	client, _, release, err := s.forWithIdentity(ctx)
	return client, release, err
}

// ForScoped is For plus whether this RPC's session sees private attributes.
//
// That bit is not derivable from the client: it is a property of the ServeOptions the SERVER
// side of the loopback was given, which the client never learns. It is reported here because
// it is the one thing that makes two identities derive different schemas from the same table,
// so anything caching a derivation has to key on it.
func (s *Sessions) ForScoped(ctx context.Context) (*dbrpc.Client, bool, func(), error) {
	return s.forScoped(ctx)
}

// Identity returns the identity this RPC authenticated as, without opening a session. It is
// what a prepared statement's handle is bound to, so one identity cannot use another's.
func (s *Sessions) Identity(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	user, err := s.authn.Authenticate(ctx, md)
	if err != nil {
		return "", status.Errorf(codes.Unauthenticated, "%s: %v", s.authn.Name(), err)
	}
	return user, nil
}

// forWithIdentity is openSession without the privilege bit, for callers that only need the identity.
func (s *Sessions) forWithIdentity(ctx context.Context) (*dbrpc.Client, string, func(), error) {
	client, user, _, release, err := s.openSession(ctx)
	return client, user, release, err
}

// forScoped is openSession without the identity.
func (s *Sessions) forScoped(ctx context.Context) (*dbrpc.Client, bool, func(), error) {
	client, _, private, release, err := s.openSession(ctx)
	return client, private, release, err
}

// openSession authenticates, authorizes, and opens one loopback session, reporting the identity it
// authenticated as and whether the session was served with private attributes included.
func (s *Sessions) openSession(ctx context.Context) (*dbrpc.Client, string, bool, func(), error) {
	md, _ := metadata.FromIncomingContext(ctx)
	user, err := s.authn.Authenticate(ctx, md)
	if err != nil {
		return nil, "", false, nil, status.Errorf(codes.Unauthenticated, "%s: %v", s.authn.Name(), err)
	}
	// An empty user is the anonymous peer, not a failure: the policy below decides whether
	// it may read, exactly as it does for an anonymous CEDAR peer.
	addr := peerAddr(ctx)
	if !s.authz("READ", addr, user) {
		return nil, "", false, nil, status.Errorf(codes.PermissionDenied, "%s is not authorized to read",
			describeUser(user))
	}

	cc, sc := net.Pipe()
	opts := levelOptions(s.authz, addr, user)
	go func() { _ = s.cat.ServeConnOpts(dbrpc.NewStreamConn(sc), opts) }()

	// Carry the peer's advertisement, as every dbrpc client must. A client built without it
	// reads every server as SupportUnknown and quietly keeps to the probe paths -- which
	// from the outside looks exactly like talking to an old server, with nothing failing
	// and nothing logged (see command/advertise_wiring_test.go, which checks that no client
	// forgets).
	//
	// A loopback is the one case with no handshake to capture it from, and also the one
	// case that needs none: the peer is this very process, so its advertisement is
	// ServerSessionAttrs() exactly. Reaching for a handshake here would be inventing a
	// negotiation with ourselves.
	attrs, missing := loopbackPeerAttrs()
	if len(missing) > 0 {
		// Not fatal: an attribute a client wants and the server does not advertise
		// leaves that one reading as unknown, which is the safe reading and is what a
		// networked client would also get. Worth saying out loud, because silence here
		// is indistinguishable from everything being wired.
		return nil, "", false, nil, fmt.Errorf(
			"dbrpc advertises no %v, which a client asks for; the loopback would read them as unknown",
			missing)
	}
	client := dbrpc.NewClient(dbrpc.NewStreamConn(cc), dbrpc.WithPeerSessionAttrs(attrs))

	sess := &session{client: client}
	s.mu.Lock()
	s.open[sess] = struct{}{}
	s.mu.Unlock()

	return client, user, opts.IncludePrivate, func() {
		s.mu.Lock()
		delete(s.open, sess)
		s.mu.Unlock()
		client.Close()
	}, nil
}

// Close tears down every session still open, so a shutdown does not leave loopback goroutines
// serving connections nobody is reading.
func (s *Sessions) Close() {
	s.mu.Lock()
	open := make([]*session, 0, len(s.open))
	for sess := range s.open {
		open = append(open, sess)
	}
	s.open = map[*session]struct{}{}
	s.mu.Unlock()
	for _, sess := range open {
		sess.client.Close()
	}
}

// Derive returns the type declarations these sessions were built with, so the Flight server
// built over them uses the same ones.
func (s *Sessions) Derive() arrowtype.Options { return s.derive }

func peerAddr(ctx context.Context) string {
	if p, ok := peerFromContext(ctx); ok {
		return p
	}
	return ""
}

// BearerToken pulls a bearer credential out of gRPC metadata, which is where every ADBC client
// puts one. Both the standard "authorization: Bearer <tok>" and a bare token are accepted,
// because clients differ and rejecting a bare token teaches nothing.
func BearerToken(md metadata.MD) (string, bool) {
	for _, key := range []string{"authorization", "auth-token-bin", "x-flight-token"} {
		for _, v := range md.Get(key) {
			if v == "" {
				continue
			}
			if len(v) > 7 && strings.EqualFold(v[:7], "bearer ") {
				return strings.TrimSpace(v[7:]), true
			}
			return strings.TrimSpace(v), true
		}
	}
	return "", false
}

// UnaryAuthInterceptor and StreamAuthInterceptor reject a connection whose metadata does not
// authenticate, before any handler runs. The per-RPC session lookup still authenticates --
// these exist so a refusal costs nothing and so an unauthenticated caller never reaches
// handler code at all.
func (s *Sessions) UnaryAuthInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := s.check(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func (s *Sessions) StreamAuthInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := s.check(ss.Context()); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// check refuses a connection whose credential is BAD, before any handler runs. It does not
// refuse one that presented no credential: that is the anonymous peer, and whether it may read
// is the authorizer's decision in For, not this one's.
func (s *Sessions) check(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	if _, err := s.authn.Authenticate(ctx, md); err != nil {
		return status.Errorf(codes.Unauthenticated, "%s: %v", s.authn.Name(), err)
	}
	return nil
}

// describeUser names an identity for an error message. The anonymous peer has no name, and
// saying so reads better than an empty pair of quotes and points at the right config knob.
func describeUser(user string) string {
	if user == "" {
		return "an unauthenticated peer"
	}
	return user
}

// peerFromContext returns the peer's "host:port", for the address half of an authorization
// decision. HTCondor's ALLOW_/DENY_ policy matches on host as well as identity, so dropping
// the address would silently widen every host-scoped rule to every host.
func peerFromContext(ctx context.Context) (string, bool) {
	p, ok := peerpkg.FromContext(ctx)
	if !ok || p.Addr == nil {
		return "", false
	}
	return p.Addr.String(), true
}

// loopbackPeerAttrs is the peer advertisement for an in-process session, plus any attribute a
// networked client would ask for that the server does not actually advertise.
//
// The coverage check is the point. A loopback cannot capture an advertisement from a handshake,
// so it supplies the server's own -- correct, because the peer IS this process. But that is
// only equivalent while the two sides name the same attributes: if dbrpc grows one a client
// asks for and the server does not set, a loopback reads it as unknown and nothing says so,
// which is the same silent failure command/advertise_wiring_test.go exists to prevent. So the
// mismatch is reported rather than assumed away.
func loopbackPeerAttrs() (map[string]string, []string) {
	attrs := dbrpc.ServerSessionAttrs()
	var missing []string
	for _, name := range dbrpc.ClientSessionAttrNames() {
		if _, ok := attrs[name]; !ok {
			missing = append(missing, name)
		}
	}
	return attrs, missing
}
