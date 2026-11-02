package server

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
	"github.com/bbockelm/cedar/stream"
	"github.com/bbockelm/golang-htcondor/sessioncache"

	"github.com/bbockelm/htcondordb/command"
)

// These tests run the session command over a real CEDAR handshake authenticated by IDTOKENs, so
// the token's scopes reach the handler the way they do in the daemon: through the session's
// LimitAuthorization, kept with the cached session for resumption.

const (
	scopesTrustDomain = "scopes.test"
	scopesServerAddr  = "<127.0.0.1:9618>"
	// scopesIdentity is the identity every token names. The authorizer grants it every level,
	// as ALLOW_DAEMON = condor@<trust domain> does for a pool's daemon identity, so any session
	// served below DAEMON was lowered by its token's scopes.
	scopesIdentity = "condor@" + scopesTrustDomain
)

// syncBuffer is a bytes.Buffer safe for the logger's concurrent writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type scopesHarness struct {
	t      *testing.T
	keyDir string
	svc    *Service
	logs   *syncBuffer
}

func newScopesHarness(t *testing.T) *scopesHarness {
	t.Helper()
	keyDir := t.TempDir()
	// A POOL signing key, scrambled as HTCondor stores it.
	key := []byte("token_scopes_test_pool_key_32by!")
	deadbeef := []byte{0xde, 0xad, 0xbe, 0xef}
	for i := range key {
		key[i] ^= deadbeef[i%len(deadbeef)]
	}
	if err := os.WriteFile(filepath.Join(keyDir, "POOL"), key, 0o600); err != nil {
		t.Fatal(err)
	}

	logs := &syncBuffer{}
	svc, err := New(Config{
		Authorize: func(_, _, user string) bool { return user == scopesIdentity },
		Logger:    slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	ad, err := classad.ParseOld("Owner = \"alice\"\nClusterId = 1\nClaimId = \"secret-claim\"")
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.DB().Put("1.0", ad); err != nil {
		t.Fatal(err)
	}
	return &scopesHarness{t: t, keyDir: keyDir, svc: svc, logs: logs}
}

// server returns a CEDAR server requiring TOKEN authentication that serves the session command,
// caching its sessions in cache.
func (h *scopesHarness) server(cache *security.SessionCache) *cedarserver.Server {
	srv := cedarserver.New(&security.SecurityConfig{
		AuthMethods:             []security.AuthMethod{security.AuthToken},
		Authentication:          security.SecurityRequired,
		Encryption:              security.SecurityRequired,
		Integrity:               security.SecurityRequired,
		CryptoMethods:           []security.CryptoMethod{security.CryptoAES},
		TrustDomain:             scopesTrustDomain,
		TokenPoolSigningKeyFile: filepath.Join(h.keyDir, "POOL"),
		TokenSigningKeyDir:      h.keyDir,
		SessionCache:            cache,
	})
	srv.Authorizer = h.svc.authorize
	h.svc.RegisterOn(srv)
	return srv
}

// token returns an IDTOKEN for scopesIdentity with one "condor:/<LEVEL>" scope per entry of
// levels, and no scope claim at all for nil.
func (h *scopesHarness) token(levels []string) string {
	h.t.Helper()
	now := time.Now().Unix()
	tok, err := security.GenerateJWT(h.keyDir, "POOL", scopesIdentity, scopesTrustDomain, now, now+3600, levels)
	if err != nil {
		h.t.Fatal(err)
	}
	return tok
}

func clientConfig(token string, cache *security.SessionCache) *security.SecurityConfig {
	return &security.SecurityConfig{
		AuthMethods:    []security.AuthMethod{security.AuthToken},
		Authentication: security.SecurityRequired,
		Encryption:     security.SecurityRequired,
		Integrity:      security.SecurityRequired,
		CryptoMethods:  []security.CryptoMethod{security.CryptoAES},
		Token:          token,
		TrustDomain:    scopesTrustDomain,
		IssuerKeys:     []string{"POOL"},
		Command:        command.DBSession,
		PeerName:       scopesServerAddr,
		SessionCache:   cache,
	}
}

// connect handshakes with srv as cfg describes and returns a dbrpc client on the session.
func (h *scopesHarness) connect(srv *cedarserver.Server, cfg *security.SecurityConfig) (*dbrpc.Client, *security.SecurityNegotiation) {
	h.t.Helper()
	serverConn, clientConn := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.ServeConn(ctx, serverConn)
	}()
	st := stream.NewStream(clientConn)
	st.SetPeerAddr(scopesServerAddr)
	neg, err := security.NewAuthenticator(cfg, st).ClientHandshake(ctx)
	if err != nil {
		cancel()
		h.t.Fatalf("client handshake: %v", err)
	}
	client := dbrpc.NewClient(dbrpc.NewCedarConn(ctx, st))
	h.t.Cleanup(func() {
		_ = client.Close()
		_ = clientConn.Close()
		<-done
		cancel()
	})
	return client, neg
}

// access is what a session was able to do.
type access struct {
	private    bool // the private attribute was served
	write      bool // a write committed
	privileged bool // an admin action passed the DAEMON gate
}

func (h *scopesHarness) probe(client *dbrpc.Client) access {
	h.t.Helper()
	ctx := context.Background()
	var got access

	rows, err := client.Query(ctx, "true")
	if err != nil {
		h.t.Fatalf("query: %v", err)
	}
	if len(rows) == 0 || !strings.Contains(rows[0], "Owner") {
		h.t.Fatalf("query returned %q, want the ad", rows)
	}
	got.private = strings.Contains(rows[0], "secret-claim")

	// An action no table has: a session past the DAEMON gate is told the action is unknown, one
	// short of it is refused before the action is looked at (a READ session as read-only, a WRITE
	// one as lacking DAEMON), so nothing is changed either way.
	_, err = client.Admin(ctx, "no.such.action")
	if err == nil {
		h.t.Fatal("admin of an unknown action succeeded")
	}
	got.privileged = strings.Contains(err.Error(), "unknown admin action")

	if tx, err := client.Begin(ctx); err == nil {
		if err := tx.NewClassAd(ctx, "probe."+time.Now().Format("150405.000000000"), "Owner = \"probe\""); err == nil {
			got.write = tx.Commit(ctx) == nil
		} else {
			_ = tx.Abort(ctx)
		}
	}
	return got
}

// TestTokenScopesBoundSessionLevel: the identity is in ALLOW_DAEMON, so its level is decided by
// the token's scopes alone. A READ-scoped token gets a read-only session with private attributes
// redacted and admin actions refused; an unscoped or DAEMON-scoped token gets DAEMON, as before
// scopes were honored; a WRITE-scoped token gets WRITE.
func TestTokenScopesBoundSessionLevel(t *testing.T) {
	cases := []struct {
		name   string
		scopes []string
		want   access
	}{
		{"read-scoped", []string{"READ"}, access{}},
		{"unscoped", nil, access{private: true, write: true, privileged: true}},
		{"daemon-scoped", []string{"DAEMON"}, access{private: true, write: true, privileged: true}},
		{"write-scoped", []string{"WRITE"}, access{write: true}},
		{"administrator-scoped", []string{"ADMINISTRATOR"}, access{write: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newScopesHarness(t)
			srv := h.server(security.NewSessionCache())
			client, _ := h.connect(srv, clientConfig(h.token(tc.scopes), security.NewSessionCache()))
			if got := h.probe(client); got != tc.want {
				t.Errorf("session access = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestTokenScopesBoundLogged: a session the scopes lowered says so once, with the identity, the
// level the ALLOW_* tables grant it and the level it got; a session they did not lower does not.
func TestTokenScopesBoundLogged(t *testing.T) {
	const msg = "htcondordb session level bounded by the credential's authorization limits"

	h := newScopesHarness(t)
	srv := h.server(security.NewSessionCache())
	_ = h.probe(mustConnect(h, srv, nil))
	if strings.Contains(h.logs.String(), msg) {
		t.Errorf("an unscoped session logged a bound:\n%s", h.logs.String())
	}

	h = newScopesHarness(t)
	srv = h.server(security.NewSessionCache())
	_ = h.probe(mustConnect(h, srv, []string{"READ"}))
	var lines []string
	for _, l := range strings.Split(h.logs.String(), "\n") {
		if strings.Contains(l, msg) {
			lines = append(lines, l)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("%d bound lines for one session, want 1:\n%s", len(lines), h.logs.String())
	}
	for _, want := range []string{"level=DEBUG", "user=" + scopesIdentity, "allow_level=DAEMON", "level=READ", "limits=[READ]"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("bound line lacks %q: %s", want, lines[0])
		}
	}
}

func mustConnect(h *scopesHarness, srv *cedarserver.Server, scopes []string) *dbrpc.Client {
	client, _ := h.connect(srv, clientConfig(h.token(scopes), security.NewSessionCache()))
	return client
}

// TestTokenScopesSurviveSessionResumption: the bound is kept with the session, so a connection
// that resumes a READ-scoped session -- from the server's cache, or from the cache persisted
// across a restart -- is still served at READ.
func TestTokenScopesSurviveSessionResumption(t *testing.T) {
	h := newScopesHarness(t)
	srv := h.server(security.NewSessionCache())
	clientCache := security.NewSessionCache()
	token := h.token([]string{"READ"})

	client, neg := h.connect(srv, clientConfig(token, clientCache))
	if got := h.probe(client); got != (access{}) {
		t.Fatalf("first session access = %+v, want READ", got)
	}
	if neg.SessionId == "" {
		t.Fatal("no session id")
	}
	resume := func(t *testing.T, srv *cedarserver.Server) access {
		t.Helper()
		cfg := clientConfig(token, clientCache)
		cfg.SessionID = neg.SessionId
		client, rneg := h.connect(srv, cfg)
		if !rneg.SessionResumed {
			t.Fatal("the client did not resume the session")
		}
		return h.probe(client)
	}

	t.Run("cached", func(t *testing.T) {
		if got := resume(t, srv); got != (access{}) {
			t.Errorf("resumed session access = %+v, want READ", got)
		}
	})

	t.Run("persisted", func(t *testing.T) {
		// A server stores the sessions it negotiates in the process-wide cache. Round-trip this
		// one through the record SEC_PERSIST_SESSIONS writes and reads back at the next start,
		// into a fresh cache, and drop the live copy so only the restored one can be resumed.
		live, ok := security.GetSessionCache().Lookup(neg.SessionId)
		if !ok {
			t.Fatal("the server cached no session")
		}
		restored, err := sessioncache.RecordToEntry(sessioncache.EntryToRecord(live))
		if err != nil {
			t.Fatal(err)
		}
		security.GetSessionCache().Invalidate(neg.SessionId)
		cache := security.NewSessionCache()
		cache.Store(restored)
		if got := resume(t, h.server(cache)); got != (access{}) {
			t.Errorf("session resumed after a restart: access = %+v, want READ", got)
		}
	})
}
