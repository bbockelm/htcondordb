package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
	"github.com/bbockelm/cedar/stream"

	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/server"
	"github.com/bbockelm/htcondordb/userfunc"
)

// Registering a user function is an administrative act on a global, shared name space: the
// function runs inside every query that names it, and a name that has been claimed cannot be
// given back. So the question these tests ask is not "does the handler work" but "who reaches
// it", and they ask it over a REAL CEDAR handshake, because the authorization that matters is the
// one the server applies at dispatch and not a level this package could assert about itself.

const (
	ufTrustDomain = "userfunc.test"
	ufServerAddr  = "<127.0.0.1:9618>"
	ufIdentity    = "admin@" + ufTrustDomain
)

// ufHarness is a CEDAR server serving DBUserFuncControl, with an authorizer that grants exactly
// the levels a test names.
type ufHarness struct {
	t      *testing.T
	keyDir string
	srv    *cedarserver.Server
	mgr    *userfunc.Manager
}

func newUFHarness(t *testing.T, granted ...string) *ufHarness {
	t.Helper()
	keyDir := t.TempDir()
	key := []byte("userfunc_control_test_pool_key!!")
	deadbeef := []byte{0xde, 0xad, 0xbe, 0xef}
	for i := range key {
		key[i] ^= deadbeef[i%len(deadbeef)]
	}
	if err := os.WriteFile(filepath.Join(keyDir, "POOL"), key, 0o600); err != nil {
		t.Fatal(err)
	}

	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	mgr := userfunc.New(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(mgr.Close)
	mgr.LoadStored(cat)

	srv := cedarserver.New(&security.SecurityConfig{
		AuthMethods:             []security.AuthMethod{security.AuthToken},
		Authentication:          security.SecurityRequired,
		Encryption:              security.SecurityRequired,
		Integrity:               security.SecurityRequired,
		CryptoMethods:           []security.CryptoMethod{security.CryptoAES},
		TrustDomain:             ufTrustDomain,
		TokenPoolSigningKeyFile: filepath.Join(keyDir, "POOL"),
		TokenSigningKeyDir:      keyDir,
	})
	srv.Authorizer = func(perm, _, user string) bool {
		return user == ufIdentity && slices.Contains(granted, perm)
	}
	registerUserFuncControl(srv, &userFuncController{mgr: mgr})
	return &ufHarness{t: t, keyDir: keyDir, srv: srv, mgr: mgr}
}

// exchange runs one request against the server as an authenticated peer and returns the response
// ad, or the error the handshake or the dispatch produced.
func (h *ufHarness) exchange(req *classad.ClassAd) (*classad.ClassAd, error) {
	h.t.Helper()
	now := time.Now().Unix()
	// No scope claim, so the credential bounds nothing and the ALLOW_ tables alone decide: the
	// test is about the command's registered levels, not about token scopes.
	tok, err := security.GenerateJWT(h.keyDir, "POOL", ufIdentity, ufTrustDomain, now, now+3600, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	serverConn, clientConn := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.srv.ServeConn(ctx, serverConn) }()
	defer func() {
		_ = clientConn.Close()
		<-done
	}()

	st := stream.NewStream(clientConn)
	st.SetPeerAddr(ufServerAddr)
	cfg := &security.SecurityConfig{
		AuthMethods:    []security.AuthMethod{security.AuthToken},
		Authentication: security.SecurityRequired,
		Encryption:     security.SecurityRequired,
		Integrity:      security.SecurityRequired,
		CryptoMethods:  []security.CryptoMethod{security.CryptoAES},
		Token:          tok,
		TrustDomain:    ufTrustDomain,
		IssuerKeys:     []string{"POOL"},
		Command:        command.DBUserFuncControl,
		PeerName:       ufServerAddr,
	}
	if _, err := security.NewAuthenticator(cfg, st).ClientHandshake(ctx); err != nil {
		return nil, err
	}
	out := message.NewMessageForStream(st)
	if err := out.PutClassAd(ctx, req); err != nil {
		return nil, err
	}
	if err := out.FinishMessage(ctx); err != nil {
		return nil, err
	}
	return message.NewMessageFromStream(st).GetClassAd(ctx)
}

// registerRequest is a registration of a small script function.
func registerRequest(name, body string) *classad.ClassAd {
	req := classad.New()
	req.InsertAttrString("Action", "register")
	req.InsertAttrString("Name", name)
	req.InsertAttrString(userfunc.AttrKind, userfunc.KindScript)
	req.InsertAttrString(userfunc.AttrSource, "def "+name+body)
	return req
}

// TestUserFuncControlRefusesWrite is the one that matters: WRITE is where the analysts are, and a
// WRITE-authorized peer must not be able to claim a word in the query language. READ is included
// so the refusal is not an artifact of the identity being unknown.
func TestUserFuncControlRefusesWrite(t *testing.T) {
	for _, level := range []string{"READ", "WRITE"} {
		t.Run(level, func(t *testing.T) {
			h := newUFHarness(t, "READ", level)
			_, err := h.exchange(registerRequest("ufCtlWriteDenied", "(n):\n    return n\n"))
			if err == nil {
				t.Fatalf("a %s-authorized peer registered a user function", level)
			}
			if names := h.mgr.Names(); slices.ContainsFunc(names, func(s string) bool {
				return strings.EqualFold(s, "ufCtlWriteDenied")
			}) {
				t.Errorf("a %s-authorized peer claimed the name anyway: %v", level, names)
			}
		})
	}
}

// TestUserFuncControlAdmitsDaemonAndAdministrator: both levels are legitimate registrants -- a
// pool's daemon identity and its human administrator are different principals -- and either alone
// is enough.
func TestUserFuncControlAdmitsDaemonAndAdministrator(t *testing.T) {
	for i, level := range []string{"DAEMON", "ADMINISTRATOR"} {
		t.Run(level, func(t *testing.T) {
			h := newUFHarness(t, "READ", "WRITE", level)
			name := []string{"ufCtlDaemonOK", "ufCtlAdminOK"}[i]
			resp, err := h.exchange(registerRequest(name, "(n):\n    return n + 1\n"))
			if err != nil {
				t.Fatalf("a %s-authorized peer was refused: %v", level, err)
			}
			if ok, _ := resp.EvaluateAttrBool("Ok"); !ok {
				msg, _ := resp.EvaluateAttrString("Error")
				t.Fatalf("register: %s", msg)
			}
			note, _ := resp.EvaluateAttrString("Note")
			if !strings.Contains(note, "sha256:") {
				t.Errorf("the answer does not name the code that was registered: %s", note)
			}
			// Provenance is the identity the handshake proved, not one the request named.
			for _, st := range h.mgr.List() {
				if strings.EqualFold(st.Name, name) && st.RegisteredBy != ufIdentity {
					t.Errorf("provenance says %q, want the authenticated %q", st.RegisteredBy, ufIdentity)
				}
			}
		})
	}
}

// TestUserFuncControlListsWhatIsRegistered: the listing is the third place a function's state is
// visible, and the only one that does not require having been watching at start-up.
func TestUserFuncControlListsWhatIsRegistered(t *testing.T) {
	h := newUFHarness(t, "READ", "WRITE", "DAEMON")
	if _, err := h.exchange(registerRequest("ufCtlListed", "(a, b):\n    return a + b\n")); err != nil {
		t.Fatal(err)
	}
	req := classad.New()
	req.InsertAttrString("Action", "list")
	resp, err := h.exchange(req)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := resp.EvaluateAttrInt("Count"); n != 1 {
		t.Fatalf("the listing counts %d functions, want 1", n)
	}
	// The listing's rows follow the response on the same stream; exchange reads only the first
	// ad, so the count above is what this asserts over the wire. The row's content is the
	// manager's, and its shape is checked there.
	if st := h.mgr.List(); len(st) != 1 || st[0].Arity != 2 || st[0].State != userfunc.StateOK {
		t.Errorf("listing: %+v", st)
	}
}

// TestUserFuncTableIsOwnedByTheDaemon: a record in this table becomes a registered function at the
// next restart, so a client that could write one would claim a name without going through the
// administrative command. Ownership is what closes that route, at every level, DAEMON included.
func TestUserFuncTableIsOwnedByTheDaemon(t *testing.T) {
	owners := server.NewTableOwners()
	if err := claimUserFuncTable(owners); err != nil {
		t.Fatalf("claiming %s: %v", userfunc.Table, err)
	}
	if owners.Writable(userfunc.Table, "") {
		t.Errorf("%s is writable by a remote session, so a name can be claimed by writing a record", userfunc.Table)
	}
	// Case does not get around it: a table is a directory, and on a case-insensitive filesystem
	// "User_Functions" opens the same files.
	if owners.Writable(strings.ToUpper(userfunc.Table), "") {
		t.Errorf("%s is writable under another spelling", strings.ToUpper(userfunc.Table))
	}
}
