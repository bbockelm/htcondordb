package main

import (
	"net/http/httptest"
	"testing"
)

// TestBearerAuthorizer: the token gates access; identity comes from the query.
func TestBearerAuthorizer(t *testing.T) {
	auth := bearerAuthorizer("s3cret")

	req := httptest.NewRequest("GET", "/changefeed/v1/subscribe?src=ap40&subscriber=sink1", nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	if src, sub, ok := auth(req); !ok || src != "ap40" || sub != "sink1" {
		t.Fatalf("valid token: got src=%q sub=%q ok=%v, want ap40/sink1/true", src, sub, ok)
	}

	bad := httptest.NewRequest("GET", "/changefeed/v1/subscribe?src=ap40", nil)
	bad.Header.Set("Authorization", "Bearer wrong")
	if _, _, ok := auth(bad); ok {
		t.Error("wrong token should be rejected")
	}

	none := httptest.NewRequest("GET", "/changefeed/v1/subscribe", nil)
	if _, _, ok := auth(none); ok {
		t.Error("missing token should be rejected")
	}
}
