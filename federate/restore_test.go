package federate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/cedarsync"
)

// dialLog records every spoke address the hub dials; every dial fails.
type dialLog struct {
	mu    sync.Mutex
	addrs map[string]int
}

func (d *dialLog) dial(addr string) cedarsync.Dial {
	return func(context.Context) (*dbrpc.Client, func(), error) {
		d.mu.Lock()
		if d.addrs == nil {
			d.addrs = map[string]int{}
		}
		d.addrs[addr]++
		d.mu.Unlock()
		return nil, nil, errors.New("spoke unreachable (test)")
	}
}

func (d *dialLog) count(addr string) int { d.mu.Lock(); defer d.mu.Unlock(); return d.addrs[addr] }

// TestRestoredSpokeAddressRevalidated: a hub restarting with the collector down resumes streaming
// from each persisted spoke address only if it still passes host validation (or is a configured
// static spoke). A persisted row is not proof of a pairing -- it may predate a configuration change,
// or have been planted -- and its address is dialed under a real AP's name.
func TestRestoredSpokeAddressRevalidated(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	src := mustTable(t, cat, TableSources)
	now := itoa(int(time.Now().Unix()))
	for key, text := range map[string]string{
		"ap1.example.org": `ScheddName = "ap1.example.org"; SpokeAddress = "<10.0.0.1:9620>"; LastSeen = ` + now,
		// Claims ap2 from another host, and claims to be static.
		"ap2.example.org": `ScheddName = "ap2.example.org"; SpokeAddress = "<10.6.6.6:9620>"; Static = true; LastSeen = ` + now,
		// Configured static spoke: trusted as configured.
		"ap3.example.org": `ScheddName = "ap3.example.org"; SpokeAddress = "<10.9.9.9:9620>"; Static = true; LastSeen = ` + now,
	} {
		tx := src.Begin()
		tx.NewClassAd(key, parseAd(t, text))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	disc := &Discovery{Collector: &fakeCollector{scheddErr: errors.New("collector down")}, ScheddConstraint: `true`,
		Resolve: resolveMap(map[string][]string{"ap1.example.org": {"10.0.0.1"}, "ap2.example.org": {"10.0.0.2"}}),
		Static:  []Spoke{{Schedd: "ap3.example.org", Address: "<10.9.9.9:9620>"}}}
	var dl dialLog
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: dl.dial})
	defer hub.stop(t)
	waitFor(t, "ap1 dialed", func() bool { return dl.count("<10.0.0.1:9620>") > 0 })
	waitFor(t, "ap3 dialed", func() bool { return dl.count("<10.9.9.9:9620>") > 0 })
	time.Sleep(200 * time.Millisecond)
	if n := dl.count("<10.6.6.6:9620>"); n != 0 {
		t.Fatalf("restored address that fails host validation was dialed %d times", n)
	}
	if st := sourceState(cat, "ap2.example.org"); st != StateUntrusted {
		t.Errorf("ap2 state = %q, want %q", st, StateUntrusted)
	}
	if v, _ := sourceRowAttrBool(cat, "ap2.example.org", "Static"); v {
		t.Error("a persisted Static flag outlived the configuration")
	}
}
