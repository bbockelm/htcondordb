package federate

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
)

// capCollector behaves like the collector client: Limit 0 means its default of 50 ads, a negative
// Limit means all of them.
type capCollector struct {
	ads  []*classad.ClassAd
	opts *htcondor.QueryOptions
}

func (c *capCollector) QueryAdsWithOptions(_ context.Context, _, _ string, opts *htcondor.QueryOptions) ([]*classad.ClassAd, *htcondor.PageInfo, error) {
	c.opts = opts
	n := len(c.ads)
	switch {
	case opts == nil || opts.Limit == 0:
		n = min(n, 50)
	case opts.Limit > 0:
		n = min(n, opts.Limit)
	}
	return c.ads[:n], nil, nil
}

// TestPoolCollectorUnlimited: discovery asks for every ad. The client's default caps a query at 50;
// an AP set cut at 50 would read every other AP as absent.
func TestPoolCollectorUnlimited(t *testing.T) {
	cc := &capCollector{}
	for i := 0; i < 120; i++ {
		cc.ads = append(cc.ads, scheddAd(t, fmt.Sprintf("ap%d.example.org", i), "<10.0.0.1:9618>"))
	}
	ads, err := PoolCollector{client: cc}.Query(context.Background(), "Schedd", "true", scheddProjection)
	if err != nil {
		t.Fatal(err)
	}
	if len(ads) != 120 {
		t.Errorf("got %d ads, want all 120", len(ads))
	}
	if cc.opts == nil || cc.opts.Limit >= 0 || !slices.Equal(cc.opts.Projection, scheddProjection) {
		t.Errorf("query options = %+v, want Limit < 0 and the projection", cc.opts)
	}
}
