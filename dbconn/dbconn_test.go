package dbconn

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/cedar/security"
)

func TestAdvertiseAndDiscoverAgree(t *testing.T) {
	var server, client security.SecurityConfig
	Advertise(&server)
	Discover(&client)

	if len(server.SessionAttrs) == 0 {
		t.Fatal("Advertise set no attributes")
	}
	for name := range server.SessionAttrs {
		found := false
		for _, want := range client.WantSessionAttrs {
			if want == name {
				found = true
			}
		}
		if !found {
			t.Errorf("the server advertises %s but Discover never asks for it, so it is thrown away in the handshake", name)
		}
	}
}

// TestNilConfigIsNotACrash: both halves are called on configs built several
// layers away, and a daemon that panicked here would fail to start rather than
// fall back.
func TestNilConfigIsNotACrash(t *testing.T) {
	Advertise(nil)
	Discover(nil)
}

// TestPeerOptionWithoutAConnectionIsUnknown pins the safe direction. A client
// that never authenticated, or whose config skipped Discover, must read as a
// peer that advertised nothing -- not as one that refused every feature.
func TestPeerOptionWithoutAConnectionIsUnknown(t *testing.T) {
	c := dbrpc.NewClient(nopConn{}, PeerOption(nil))
	defer func() { _ = c.Close() }()

	if got := c.PeerProtocol(); got != 0 {
		t.Errorf("PeerProtocol() = %d, want 0", got)
	}
	if got := c.Supports(dbrpc.FeatProjectRefs); got != dbrpc.SupportUnknown {
		t.Errorf("Supports() = %s, want unknown", got)
	}
}

type nopConn struct{}

func (nopConn) WriteMsg([]byte) error    { return os.ErrClosed }
func (nopConn) ReadMsg() ([]byte, error) { return nil, os.ErrClosed }
func (nopConn) Close() error             { return nil }

// TestEveryDbrpcClientGoesThroughDbconn is the guard that matters here.
//
// Nine places in this repository dial the daemon. The handshake carrying the
// advertisement is cedar's and is tested there; what this repository can still
// get wrong is adding a tenth dial site that builds its dbrpc client directly.
// That site would see every server as unknown and quietly keep to the probe
// paths -- indistinguishable, from the outside, from talking to an old server.
//
// capi is the one allowed exception: it wraps its transport to notice the peer
// dying, so it cannot use New, and passes PeerOption itself.
func TestEveryDbrpcClientGoesThroughDbconn(t *testing.T) {
	root := ".."
	allowed := map[string]bool{
		filepath.Join("dbconn", "dbconn.go"): true,
		filepath.Join("capi", "capi.go"):     true,
	}

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor", "docs", "packaging":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		if !strings.Contains(string(body), "dbrpc.NewClient(") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if allowed[rel] {
			return nil
		}
		t.Errorf("%s calls dbrpc.NewClient directly; use dbconn.New (or dbconn.PeerOption) "+
			"so the server's advertised capabilities are not discarded", rel)
		return nil
	})
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}
}
