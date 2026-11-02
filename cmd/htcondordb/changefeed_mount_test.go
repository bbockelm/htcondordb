package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/changefeed"
	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// The tokens gate access and decide privilege; identity comes from the query.
func TestChangeFeedAuthorizer(t *testing.T) {
	auth, err := changeFeedAuthorizer("s3cret", "pr1v")
	if err != nil {
		t.Fatal(err)
	}
	req := func(header string) *http.Request {
		r := httptest.NewRequest("GET", "/changefeed/v1/subscribe?src=ap40&subscriber=sink1", nil)
		if header != "" {
			r.Header.Set("Authorization", header)
		}
		return r
	}

	if g, ok := auth(req("Bearer s3cret")); !ok || g.Privileged || g.Src != "ap40" || g.Subscriber != "sink1" {
		t.Fatalf("unprivileged token: got %+v ok=%v, want ap40/sink1 unprivileged", g, ok)
	}
	if g, ok := auth(req("Bearer pr1v")); !ok || !g.Privileged || g.Src != "ap40" || g.Subscriber != "sink1" {
		t.Fatalf("privileged token: got %+v ok=%v, want ap40/sink1 privileged", g, ok)
	}
	for _, h := range []string{"Bearer wrong", "", "Bearer ", "s3cret", "Bearer s3cretX", "Bearer pr1"} {
		if _, ok := auth(req(h)); ok {
			t.Errorf("Authorization %q was admitted", h)
		}
	}
}

func TestChangeFeedAuthorizerConfigurations(t *testing.T) {
	// No tokens: the feed's own default (everyone, unprivileged).
	if auth, err := changeFeedAuthorizer("", ""); err != nil || auth != nil {
		t.Errorf("no tokens: got auth=%v err=%v, want nil, nil", auth != nil, err)
	}
	// The same token for both would hand plaintext to every holder of the shared one.
	if _, err := changeFeedAuthorizer("same", "same"); !errors.Is(err, errChangeFeedTokensEqual) {
		t.Errorf("equal tokens: err = %v, want errChangeFeedTokensEqual", err)
	}
	// Only the unprivileged token: nothing is privileged.
	auth, err := changeFeedAuthorizer("s3cret", "")
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer s3cret")
	if g, ok := auth(r); !ok || g.Privileged {
		t.Errorf("unprivileged-only: got %+v ok=%v", g, ok)
	}
	r.Header.Set("Authorization", "Bearer ")
	if _, ok := auth(r); ok {
		t.Error("an empty bearer matched the unset privileged token")
	}
	// Only the privileged token: an operator who wants just a trusted consumer.
	auth, err = changeFeedAuthorizer("", "pr1v")
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer pr1v")
	if g, ok := auth(r); !ok || !g.Privileged {
		t.Errorf("privileged-only: got %+v ok=%v", g, ok)
	}
	r.Header.Set("Authorization", "Bearer ")
	if _, ok := auth(r); ok {
		t.Error("an empty bearer matched the unset unprivileged token")
	}
}

// End to end through the feed: a sealed attribute reaches a subscriber on the privileged token in
// plaintext, and one on the ordinary token as undefined.
func TestChangeFeedTokensRedactSealedValues(t *testing.T) {
	cat, err := db.OpenCatalogConfig(db.CatalogConfig{
		Dir:            t.TempDir(),
		PoolKeys:       []db.KEK{{ID: "POOL", Material: []byte("pool-key-material-for-the-change-feed-test")}},
		EncryptedAttrs: []string{"Secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	tbl, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	ad, err := classad.Parse(`[Cpus = 4; Secret = "hunter2"]`)
	if err != nil {
		t.Fatal(err)
	}
	if err := tbl.Put("a", ad); err != nil {
		t.Fatal(err)
	}

	h, err := changeFeedHandler(cat, "plain-token", "priv-token", &changefeed.MemRegistry{}, "")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	first := func(token string) *classad.ClassAd {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		q := url.Values{"table": {"jobs"}, "subscriber": {"s1"}, "src": {"src1"}, "cursor": {"@begin"}}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+changefeed.PathSubscribe+"?"+q.Encode(), http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("subscribe: status %d", resp.StatusCode)
		}
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		for sc.Scan() {
			line := sc.Bytes()
			if !bytes.HasPrefix(line, []byte("data:")) {
				continue
			}
			var ev changefeed.Event
			if err := json.Unmarshal(bytes.TrimSpace(line[len("data:"):]), &ev); err != nil {
				t.Fatal(err)
			}
			if ev.Key != "a" {
				continue
			}
			ch, err := changefeed.ToChange(ev)
			if err != nil {
				t.Fatal(err)
			}
			return ch.Ad
		}
		t.Fatalf("stream ended without the event for a: %v", sc.Err())
		return nil
	}

	priv := first("priv-token")
	if s, ok := priv.EvaluateAttrString("Secret"); !ok || s != "hunter2" {
		t.Errorf("privileged subscriber: Secret = %q (ok=%v), want hunter2", s, ok)
	}
	plain := first("plain-token")
	if s, ok := plain.EvaluateAttrString("Secret"); ok {
		t.Errorf("unprivileged subscriber received the sealed value %q", s)
	}
	if v, ok := plain.EvaluateAttrInt("Cpus"); !ok || v != 4 {
		t.Errorf("unprivileged subscriber: Cpus = %d (ok=%v), want 4", v, ok)
	}
}
