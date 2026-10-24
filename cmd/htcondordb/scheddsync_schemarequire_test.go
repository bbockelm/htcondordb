package main

import (
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// TestSchemaRequireDefaults pins the shipped list. It is not a free-form preference: a columnar
// field is admitted on presence, and every attribute here is written only once a job has RUN, so
// on a table that also holds jobs removed before they ran none of them would ever qualify.
func TestSchemaRequireDefaults(t *testing.T) {
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
	got := splitAttrList(s.schemaRequireAttrs)
	want := []string{
		"ResidentSetSize", "ProportionalSetSize", "CurrentResidentSetSize",
		"ImageSize", "DiskUsage", "RemoteWallClockTime", "CommittedTime",
		"RemoteUserCpu", "RemoteSysCpu", "BytesSent", "BytesRecvd",
	}
	if !slices.Equal(got, want) {
		t.Errorf("default required attributes = %v, want %v", got, want)
	}
	// MemoryUsage is frequently an EXPRESSION in a job ad (the shadow passes through whatever the
	// starter sent), so it has no dominant literal value. A slot for it would escape in every
	// record, and the fast count paths read a held expression as UNDEFINED -- wrong counts, not
	// just slow ones. The library refuses it; this keeps it from being added back by habit.
	if slices.ContainsFunc(got, func(n string) bool { return strings.EqualFold(n, "MemoryUsage") }) {
		t.Error("MemoryUsage is in the default required set; it is expression-valued and a slot " +
			"for it would be escaped in every record")
	}
}

// TestSchemaRequireOverrides: the knob is read, and "none" turns the defaults off. Empty cannot
// mean "none" -- every other attribute knob here reads empty as "unset, take the default".
func TestSchemaRequireOverrides(t *testing.T) {
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
		"HTCONDORDB_SCHEMA_REQUIRE_ATTRS = ResidentSetSize, BytesSent\n"))
	if got := splitAttrList(s.schemaRequireAttrs); !slices.Equal(got, []string{"ResidentSetSize", "BytesSent"}) {
		t.Errorf("configured set = %v, want [ResidentSetSize BytesSent]", got)
	}

	s, _ = resolveScheddSyncSettings(mkSyncCfg(t, syncOn+"HTCONDORDB_SCHEMA_REQUIRE_ATTRS = none\n"))
	if s.schemaRequireAttrs != "" {
		t.Errorf("`none` resolved to %q, want empty", s.schemaRequireAttrs)
	}

	s, _ = resolveScheddSyncSettings(mkSyncCfg(t, syncOn+"HTCONDORDB_SCHEMA_REQUIRE_ATTRS = \n"))
	if s.schemaRequireAttrs == "" {
		t.Error("an empty value resolved to no required attributes; empty means unset here")
	}
}

// TestApplySchemaRequireReassertKeepsRuntime is why this is re-asserted rather than set at create:
// archiveconfig.json is authoritative on reopen, so an archive made before the list existed would
// keep an empty set forever. And the re-assert must not erase what an operator added by hand --
// the same add-only courtesy the index reconciliation extends.
func TestApplySchemaRequireReassertKeepsRuntime(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	// An archive as it exists in a deployment today: no required set.
	hist, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hist.AddSchemaRequireAttrs("MyLocalAttr"); err != nil {
		t.Fatal(err)
	}

	m := &scheddSyncManager{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
		"HTCONDORDB_SCHEMA_REQUIRE_ATTRS = ResidentSetSize\n"))
	m.applySchemaRequire("history", splitAttrList(s.schemaRequireAttrs), hist.SetSchemaRequireAttrs)

	got := hist.SchemaRequireAttrs()
	if !slices.Contains(got, "residentsetsize") {
		t.Errorf("configured attribute not applied to an existing archive; set = %v", got)
	}
	if !slices.Contains(got, "mylocalattr") {
		t.Errorf("re-asserting the config dropped a hand-added attribute; set = %v", got)
	}
}
