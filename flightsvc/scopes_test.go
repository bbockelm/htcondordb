package flightsvc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// scopedAuth authenticates to one identity with a fixed authorization bounding set.
type scopedAuth struct {
	user   string
	limits []string
}

func (scopedAuth) Name() string { return "scoped" }
func (a scopedAuth) Authenticate(context.Context, metadata.MD) (string, error) {
	return a.user, nil
}
func (a scopedAuth) AuthenticateScoped(context.Context, metadata.MD) (string, []string, error) {
	return a.user, a.limits, nil
}

// TestScopesBoundFlightLevel: alice is authorized at every level, so what she is served is
// decided by her credential's bounding set, as over CEDAR. A READ-limited credential is served
// at READ (no private attributes); an unlimited or DAEMON-limited one at DAEMON; a bounding set
// that names no level (a SciToken with no condor scope) is refused, because the session is a
// READ command and READ is outside the set.
func TestScopesBoundFlightLevel(t *testing.T) {
	cat, done := store(t)
	defer done()
	all := allowLevels("alice", "READ", "WRITE", "DAEMON")

	for _, c := range []struct {
		name        string
		limits      []string
		wantPrivate bool
		wantRefused bool
	}{
		{"unlimited", nil, true, false},
		{"read", []string{"DENY", "READ"}, false, false},
		{"write", []string{"WRITE"}, false, false},
		{"daemon", []string{"DENY", "DAEMON"}, true, false},
		{"no level", []string{"DENY"}, false, true},
		{"empty set", []string{}, false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, err := NewSessions(cat, scopedAuth{user: "alice", limits: c.limits}, all, arrowtype.Options{})
			if err != nil {
				t.Fatal(err)
			}
			client, release, err := s.For(context.Background())
			if c.wantRefused {
				if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("err = %v, want PermissionDenied", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if got := len(mustSchema(t, client).FieldIndices("ClaimId")) != 0; got != c.wantPrivate {
				t.Errorf("ClaimId served = %v, want %v", got, c.wantPrivate)
			}
			if opts := levelOptions(all, "", "alice", c.limits); opts.IncludePrivate != c.wantPrivate || opts.Privileged != c.wantPrivate {
				t.Errorf("levelOptions = %+v, want IncludePrivate and Privileged %v", opts, c.wantPrivate)
			}
		})
	}
}

// testIssuer is an https SciToken issuer serving one ES256 key.
type testIssuer struct {
	srv *httptest.Server
	key *ecdsa.PrivateKey
}

func newTestIssuer(t *testing.T) *testIssuer {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	iss := &testIssuer{key: key}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": iss.srv.URL, "jwks_uri": iss.srv.URL + "/jwks"})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		pad := func(b []byte) string {
			out := make([]byte, 32)
			copy(out[32-len(b):], b)
			return base64.RawURLEncoding.EncodeToString(out)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "EC", "crv": "P-256", "kid": "k1", "alg": "ES256", "use": "sig",
			"x": pad(key.X.Bytes()), "y": pad(key.Y.Bytes()),
		}}})
	})
	iss.srv = httptest.NewTLSServer(mux)
	t.Cleanup(iss.srv.Close)
	return iss
}

// token signs a SciToken for subject alice with the given scope claim.
func (iss *testIssuer) token(t *testing.T, scope string) string {
	t.Helper()
	enc := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(b)
	}
	now := time.Now().Unix()
	input := enc(map[string]string{"alg": "ES256", "typ": "JWT", "kid": "k1"}) + "." +
		enc(map[string]any{"iss": iss.srv.URL, "sub": "alice", "scope": scope, "iat": now, "exp": now + 600, "jti": "t"})
	sum := sha256.Sum256([]byte(input))
	r, s, err := ecdsa.Sign(rand.Reader, iss.key, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// TestSciTokenScopesBoundFlightLevel runs the bound through the real SciToken verification: the
// identity is authorized at every level, so a token scoped to condor:/READ is served without
// private attributes, condor:/DAEMON with them, and a token with no condor scope is refused --
// through the OPTIONAL wrapper the endpoint is configured with by default, too.
func TestSciTokenScopesBoundFlightLevel(t *testing.T) {
	cat, done := store(t)
	defer done()
	iss := newTestIssuer(t)
	sci, err := SciTokenAuthenticator(SciTokenConfig{Issuers: []string{iss.srv.URL}, HTTPClient: iss.srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	user := "alice@127.0.0.1"
	all := allowLevels(user, "READ", "WRITE", "DAEMON")

	for _, c := range []struct {
		scope       string
		wantLimits  []string
		wantPrivate bool
		wantRefused bool
	}{
		{"condor:/READ", []string{"DENY", "READ"}, false, false},
		{"condor:/DAEMON", []string{"DENY", "DAEMON"}, true, false},
		{"read:/data", []string{"DENY"}, false, true},
	} {
		for _, authn := range []Authenticator{sci, OptionalAuth(sci)} {
			t.Run(authn.Name()+"/"+c.scope, func(t *testing.T) {
				md := metadata.Pairs("authorization", "Bearer "+iss.token(t, c.scope))
				gotUser, limits, err := authn.(ScopedAuthenticator).AuthenticateScoped(context.Background(), md)
				if err != nil {
					t.Fatal(err)
				}
				if gotUser != user || !slices.Equal(limits, c.wantLimits) {
					t.Fatalf("authenticated as %q limited to %q, want %q limited to %q", gotUser, limits, user, c.wantLimits)
				}

				s, err := NewSessions(cat, authn, all, arrowtype.Options{})
				if err != nil {
					t.Fatal(err)
				}
				ctx := metadata.NewIncomingContext(context.Background(), md)
				client, release, err := s.For(ctx)
				if c.wantRefused {
					if status.Code(err) != codes.PermissionDenied {
						t.Fatalf("err = %v, want PermissionDenied", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				if got := len(mustSchema(t, client).FieldIndices("ClaimId")) != 0; got != c.wantPrivate {
					t.Errorf("ClaimId served = %v, want %v", got, c.wantPrivate)
				}
			})
		}
	}
}
