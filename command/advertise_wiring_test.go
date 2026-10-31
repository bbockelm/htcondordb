package command_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The advertisement wiring is three lines repeated at ten places, and the
// places are spread over five Go modules: the root, grafana, and the three
// exporters, which depend on neither the root module nor each other. That is
// why these are plain assignments at each site rather than a shared helper --
// there is no package all five can import without inventing a dependency
// between programs that are deliberately independent.
//
// The cost of that is a line somebody can forget, and forgetting it is
// invisible: a client that never asks for the attributes sees every server as
// SupportUnknown and quietly keeps to the probe paths, which from the outside
// is exactly what talking to an old server looks like. Nothing fails, nothing
// logs, and the mixed-version paths silently become the only paths.
//
// So the repetition is checked here instead, across every module at once. This
// file scans source text because that is the only thing that spans modules the
// root module cannot import.

// repoRoot is this file's module root; the scan walks everything below it,
// including the sibling modules.
const repoRoot = ".."

func goFiles(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor", "docs", "packaging", "python", "testdata":
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
		rel, _ := filepath.Rel(repoRoot, path)
		out[rel] = string(body)
		return nil
	})
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("scanned no files; the walk is broken and this test proves nothing")
	}
	return out
}

// TestEveryDbrpcClientReadsWhatThePeerAdvertised: a dbrpc client built without
// WithPeerSessionAttrs throws away the handshake's advertisement.
func TestEveryDbrpcClientReadsWhatThePeerAdvertised(t *testing.T) {
	found := 0
	for rel, body := range goFiles(t) {
		if !strings.Contains(body, "dbrpc.NewClient(") {
			continue
		}
		found++
		if !strings.Contains(body, "dbrpc.WithPeerSessionAttrs(") {
			t.Errorf("%s builds a dbrpc client without dbrpc.WithPeerSessionAttrs, "+
				"so it discards what the server advertised and sees every peer as unknown", rel)
		}
	}
	if found == 0 {
		t.Fatal("found no dbrpc.NewClient call anywhere; the scan is not looking where it thinks")
	}
}

// TestEveryDbSessionClientAsksForTheAdvertisement: capturing the attributes is
// opt-in per connection. A client that builds a DBSession security config and
// never sets WantSessionAttrs gets an empty map, and then
// WithPeerSessionAttrs faithfully carries nothing.
func TestEveryDbSessionClientAsksForTheAdvertisement(t *testing.T) {
	found := 0
	for rel, body := range goFiles(t) {
		if !strings.Contains(body, "dbrpc.NewClient(") {
			continue
		}
		found++
		if !strings.Contains(body, "dbrpc.ClientSessionAttrNames()") {
			t.Errorf("%s builds a dbrpc client but never sets "+
				"sec.WantSessionAttrs = dbrpc.ClientSessionAttrNames(), so the handshake "+
				"carries the server's advertisement and the client drops it", rel)
		}
	}
	if found == 0 {
		t.Fatal("found no dbrpc.NewClient call anywhere; the scan is not looking where it thinks")
	}
}

// TestTheServerAdvertises is the other end: exactly one place builds the
// listener, and if it stops advertising every client falls back to probing.
func TestTheServerAdvertises(t *testing.T) {
	for rel, body := range goFiles(t) {
		if strings.Contains(body, "dbrpc.ServerSessionAttrs()") {
			t.Logf("server advertises in %s", rel)
			return
		}
	}
	t.Error("nothing sets sec.SessionAttrs = dbrpc.ServerSessionAttrs(); the daemon advertises nothing " +
		"and every client is left probing")
}
