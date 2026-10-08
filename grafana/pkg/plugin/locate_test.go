package plugin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

// settings builds the DataSourceInstanceSettings Grafana would hand NewDatasource
// for the given JSONData.
func settings(t *testing.T, jsonData map[string]any) backend.DataSourceInstanceSettings {
	t.Helper()
	raw, err := json.Marshal(jsonData)
	if err != nil {
		t.Fatal(err)
	}
	return backend.DataSourceInstanceSettings{UID: "test", JSONData: raw}
}

// TestAddressIsUsedVerbatim: a configured address is dialed as given, with no
// collector involved.
func TestAddressIsUsedVerbatim(t *testing.T) {
	cc := connConfig{Address: " <1.2.3.4:9618?sock=htcondordb_1_a> "}
	got, err := cc.resolveAddress(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("resolveAddress: %v", err)
	}
	if want := "<1.2.3.4:9618?sock=htcondordb_1_a>"; got != want {
		t.Errorf("address = %q, want %q", got, want)
	}
}

// TestPoolLookupIsAttempted: with no address, resolution goes to the collector.
// The lookup fails here (nothing is listening), which is what proves the pool
// path ran -- an address-only implementation would never mention a collector.
func TestPoolLookupIsAttempted(t *testing.T) {
	cc := connConfig{Pool: "127.0.0.1:1", Name: "db1@host"}
	_, err := cc.resolveAddress(context.Background(), 2*time.Second)
	if err == nil {
		t.Fatal("expected the lookup against a dead collector to fail")
	}
	if !strings.Contains(err.Error(), "locating htcondordb") {
		t.Errorf("error should say it was locating the daemon, got: %v", err)
	}
}

// TestConfigRejectsBothAddressAndPool: the two can name different daemons, so
// accepting both would run queries somewhere nobody asked for.
func TestConfigRejectsBothAddressAndPool(t *testing.T) {
	for _, js := range []map[string]any{
		{"address": "db:9618", "pool": "cm.example.edu"},
		{"address": "db:9618", "name": "db1@host"},
	} {
		_, err := NewDatasource(context.Background(), settings(t, js))
		if err == nil {
			t.Errorf("%v: expected a refusal", js)
			continue
		}
		if !strings.Contains(err.Error(), "not both") {
			t.Errorf("%v: error should say not both, got: %v", js, err)
		}
	}
}

// TestConfigRequiresOneOrTheOther: an empty config still fails, and the message
// names both ways out rather than only the address.
func TestConfigRequiresOneOrTheOther(t *testing.T) {
	_, err := NewDatasource(context.Background(), settings(t, map[string]any{}))
	if err == nil {
		t.Fatal("expected an unconfigured datasource to be refused")
	}
	if !strings.Contains(err.Error(), "pool") {
		t.Errorf("error should mention the pool option, got: %v", err)
	}
}

// TestPoolOnlyConfigIsAccepted: a pool with no address is a complete configuration.
func TestPoolOnlyConfigIsAccepted(t *testing.T) {
	inst, err := NewDatasource(context.Background(), settings(t,
		map[string]any{"pool": "cm-1.example.edu", "name": "db1@host"}))
	if err != nil {
		t.Fatalf("pool-only config rejected: %v", err)
	}
	ds, ok := inst.(*Datasource)
	if !ok {
		t.Fatalf("got %T, want *Datasource", inst)
	}
	if ds.cfg.Pool != "cm-1.example.edu" || ds.cfg.Name != "db1@host" {
		t.Errorf("pool/name not carried through: %+v", ds.cfg)
	}
	if ds.cfg.Address != "" {
		t.Errorf("address should be empty, got %q", ds.cfg.Address)
	}
	if ds.cfg.HTCfg == nil {
		t.Error("HTCfg should be populated so an empty pool can fall back to COLLECTOR_HOST")
	}
	ds.Dispose()
}
