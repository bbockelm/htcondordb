package opensearchsync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestBulkRequestTimeout verifies a single _bulk request is bounded by RequestTimeout, so a
// stalled OpenSearch cannot hang a bulk goroutine (and, via the in-flight cap's backpressure,
// wedge the whole exporter without ever exiting for the daemon to restart it).
func TestBulkRequestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hang until the client cancels (the point of the test), but self-bound so srv.Close()
		// can't wait forever if a request is somehow not cancelled.
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second): // > the 150ms client timeout, bounds srv.Close()
		}
	}))
	defer srv.Close()

	c, err := NewOSBulkClient(Config{Addresses: []string{srv.URL}, RequestTimeout: Duration(150 * time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	_, err = c.Bulk(context.Background(), "idx", []byte("{\"index\":{}}\n{\"x\":1}\n"))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Bulk returned nil against a hanging server; RequestTimeout not enforced")
	}
	if elapsed > 3*time.Second {
		t.Errorf("Bulk took %s to fail; the 150ms RequestTimeout was not enforced", elapsed)
	}
}
