package main

import (
	"context"
	"testing"
)

// TestResolveCedarSyncSettings covers the per-source config parsing (defaults + overrides) and the
// change-detection signature.
func TestResolveCedarSyncSettings(t *testing.T) {
	cfg := mkSyncCfg(t, `
HTCONDORDB_REPLICATE_SOURCES = ap40 ap55
HTCONDORDB_REPLICATE_AP40_ADDRESS = <10.0.0.1:9619>
HTCONDORDB_REPLICATE_AP40_CONSTRAINT = Owner == "alice"
HTCONDORDB_REPLICATE_AP55_ADDRESS = <10.0.0.2:9619>
HTCONDORDB_REPLICATE_AP55_TABLE = jobs
HTCONDORDB_REPLICATE_AP55_TARGET = central_jobs
HTCONDORDB_REPLICATE_AP55_TYPE = table
HTCONDORDB_REPLICATE_AP55_SRC = site55
`)
	s := resolveCedarSyncSettings(cfg)
	if !s.enabled || len(s.sources) != 2 {
		t.Fatalf("enabled=%v sources=%d, want true 2", s.enabled, len(s.sources))
	}
	byName := map[string]replicateSource{}
	for _, src := range s.sources {
		byName[src.Name] = src
	}

	// ap40: defaults (TABLE->history, TARGET->TABLE, SRC->name, TYPE->archive) + constraint.
	a := byName["ap40"]
	if a.Address != "<10.0.0.1:9619>" || a.Source != "history" || a.Target != "history" ||
		a.Src != "ap40" || a.Type != "archive" || a.Constraint != `Owner == "alice"` {
		t.Errorf("ap40 resolved wrong: %+v", a)
	}
	// ap55: explicit table type, distinct target + src.
	b := byName["ap55"]
	if b.Source != "jobs" || b.Target != "central_jobs" || b.Type != "table" || b.Src != "site55" {
		t.Errorf("ap55 resolved wrong: %+v", b)
	}

	// Signature is stable across identical resolves, and changes when config changes.
	if resolveCedarSyncSettings(cfg).sig != s.sig {
		t.Error("signature not stable for identical config")
	}
	cfg2 := mkSyncCfg(t, `
HTCONDORDB_REPLICATE_SOURCES = ap40
HTCONDORDB_REPLICATE_AP40_ADDRESS = <10.0.0.1:9619>
`)
	if resolveCedarSyncSettings(cfg2).sig == s.sig {
		t.Error("signature should differ when the source set changes")
	}

	// No sources -> disabled.
	if got := resolveCedarSyncSettings(mkSyncCfg(t, "")); got.enabled || got.sig != "" {
		t.Errorf("empty config: enabled=%v sig=%q, want disabled", got.enabled, got.sig)
	}
}

// TestCedarSyncManagerDisabled: apply with no sources is a clean no-op (no runners, no panic).
func TestCedarSyncManagerDisabled(t *testing.T) {
	m := &cedarSyncManager{parent: context.Background(), cat: nil, logger: discardLogger()}
	if err := m.apply(mkSyncCfg(t, "")); err != nil {
		t.Fatalf("apply(disabled): %v", err)
	}
	if m.sig != "" || m.cancel != nil {
		t.Error("disabled apply should leave the manager stopped")
	}
	// A source missing its ADDRESS is rejected before anything starts.
	err := m.apply(mkSyncCfg(t, "HTCONDORDB_REPLICATE_SOURCES = ap40\n"))
	if err == nil {
		t.Fatal("a source with no ADDRESS should error")
	}
}
