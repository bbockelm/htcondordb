package main

import (
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/collections"
)

// TestOpenTimingFieldsCarryZoneCounts: the open-timing line must say how many segments had their
// zone map recomputed and whether the write-back stuck, because the recompute DURATION is the same
// whether the next restart pays it again or not.
func TestOpenTimingFieldsCarryZoneCounts(t *testing.T) {
	dg := collections.OpenIndexDiag{
		Dir:            "/db/tables/history",
		SealedSegments: 793,
		Timing: collections.OpenTiming{
			LoadSidecars:      12 * time.Second,
			ZoneRecompute:     11 * time.Second,
			ZoneRecomputed:    658,
			ZoneUpgraded:      650,
			ZoneUpgradeFailed: 8,
		},
	}
	kv := openTimingFields(dg)
	if len(kv)%2 != 0 {
		t.Fatalf("odd key/value list: %v", kv)
	}
	got := map[string]any{}
	for i := 0; i < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok {
			t.Fatalf("key %d is %T, want string", i, kv[i])
		}
		got[k] = kv[i+1]
	}
	for k, want := range map[string]any{
		"zoneRecompute":        "11s",
		"zoneRecomputed":       658,
		"zoneUpgraded":         650,
		"zoneUpgradeFailed":    8,
		"zoneRecomputeOutlook": "recurring",
		"sealed":               793,
	} {
		if got[k] != want {
			t.Errorf("%s = %v, want %v", k, got[k], want)
		}
	}
}

func TestZoneRecomputeOutlook(t *testing.T) {
	for _, tc := range []struct {
		name string
		t    collections.OpenTiming
		want string
	}{
		{"nothing recomputed", collections.OpenTiming{}, "none"},
		{"all written back", collections.OpenTiming{ZoneRecomputed: 5, ZoneUpgraded: 5}, "one-time"},
		{"some write-backs failed", collections.OpenTiming{ZoneRecomputed: 5, ZoneUpgraded: 4, ZoneUpgradeFailed: 1}, "recurring"},
		{"none written back", collections.OpenTiming{ZoneRecomputed: 5, ZoneUpgradeFailed: 5}, "recurring"},
	} {
		if got := zoneRecomputeOutlook(tc.t); got != tc.want {
			t.Errorf("%s: outlook = %q, want %q", tc.name, got, tc.want)
		}
	}
}
