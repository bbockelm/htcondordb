package dbad

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// The panic counter is advertised as well as scraped: the ad is how a pool without the daemon's
// /metrics endpoint watches it, and a daemon that survives a panicking read otherwise looks healthy.
func TestRequestPanicsReachesTheAd(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cat.Close() }()
	was := requestPanics
	t.Cleanup(func() { requestPanics = was })
	requestPanics = func() int64 { return 4 }

	ad := classad.New()
	Augment(cat, nil, nil, nil, "")(ad)
	got, ok := ad.EvaluateAttrInt("RequestPanics")
	if !ok {
		t.Fatal("RequestPanics is not on the ad: an operator querying it sees undefined, not a count")
	}
	if got != 4 {
		t.Errorf("RequestPanics = %d, want 4 (the live counter)", got)
	}
}
