package archivedropbox

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

func TestConfigFormatDefaultAndValidation(t *testing.T) {
	c := Config{Table: "history", Directory: "/d"}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Format != FormatClassAd {
		t.Errorf("Format = %q, want %q", c.Format, FormatClassAd)
	}

	j := Config{Table: "history", Directory: "/d", Format: FormatJSON}
	if err := j.Validate(); err != nil {
		t.Fatalf("format json should be accepted: %v", err)
	}

	bad := Config{Table: "history", Directory: "/d", Format: "yaml"}
	if err := bad.Validate(); err == nil {
		t.Error("unknown format should error")
	}
}

func TestConfigFormatJSONRoundTrip(t *testing.T) {
	c, err := ParseConfig([]byte(`{"table":"history","directory":"/d","format":"json"}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Format != FormatJSON {
		t.Fatalf("Format = %q, want %q", c.Format, FormatJSON)
	}
	raw, err := c.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	c2, err := ParseConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	if c2.Format != FormatJSON {
		t.Fatalf("round-trip Format = %q, want %q", c2.Format, FormatJSON)
	}
}

func TestEntryNameExtension(t *testing.T) {
	ad, err := classad.Parse(`[GlobalJobId = "ap40#12.0#1700000000"]`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := entryName(3, "k", ad, FormatClassAd), "000003-ap40#12.0#1700000000.classad"; got != want {
		t.Errorf("entryName(classad) = %q, want %q", got, want)
	}
	if got, want := entryName(3, "k", ad, FormatJSON), "000003-ap40#12.0#1700000000.json"; got != want {
		t.Errorf("entryName(json) = %q, want %q", got, want)
	}
}

func TestMarshalRecordJSON(t *testing.T) {
	ad, err := classad.Parse(`[GlobalJobId = "ap40#12.0#1700000000"; JobStatus = 4; Owner = "alice"; Requirements = TARGET.Memory > 1024]`)
	if err != nil {
		t.Fatal(err)
	}
	body, err := marshalRecord(ad, FormatJSON)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(body, "\n") {
		t.Error("JSON record should end in a newline so entries concatenate into NDJSON")
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("record is not valid JSON: %v\n%s", err, body)
	}
	if got["Owner"] != "alice" {
		t.Errorf("Owner = %v, want alice", got["Owner"])
	}
	if v, ok := got["JobStatus"].(float64); !ok || v != 4 {
		t.Errorf("JobStatus = %v, want 4", got["JobStatus"])
	}
	// A non-literal expression survives as condor_history -json renders it.
	req, _ := got["Requirements"].(string)
	if !strings.HasPrefix(req, "/Expr(") {
		t.Errorf("Requirements = %v, want a /Expr(...)/ string", got["Requirements"])
	}
}

func TestBuildLossReportJSON(t *testing.T) {
	body, err := buildLossReport("hist-dropbox", "history", 1_700_000_000, 1_700_050_000, 1_700_060_000, FormatJSON)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("loss report is not valid JSON: %v\n%s", err, body)
	}
	if got["MyType"] != "ArchiveDropboxDataLoss" {
		t.Errorf("MyType = %v", got["MyType"])
	}
	if v, ok := got["EstimatedLossSeconds"].(float64); !ok || v != 50000 {
		t.Errorf("EstimatedLossSeconds = %v, want 50000", got["EstimatedLossSeconds"])
	}
	if got, want := FormatJSON.lossExt(), ".json"; got != want {
		t.Errorf("lossExt = %q, want %q", got, want)
	}
}

// TestRunnerJSONFormatEndToEnd is the whole path with format "json": the records land in the
// tarball as .json entries whose bodies parse as JSON.
func TestRunnerJSONFormatEndToEnd(t *testing.T) {
	c, cat := testServer(t)
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, arch, `GlobalJobId = "ap40#12.0#1700000000"; JobStatus = 4; CompletionDate = 1700000100; Owner = "alice"; ClusterId = 12`)

	dir := t.TempDir()
	cfg := Config{Table: "history", Directory: dir, RollJobs: 1, RollInterval: Duration(200 * time.Millisecond), Format: FormatJSON}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := cfg.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateExporter(context.Background(), db.ExporterDef{Name: "json-dropbox", Kind: Kind, Config: raw}); err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner("json-dropbox", cfg, c, w, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	waitFor(t, func() bool { return countTarballs(dir) >= 1 })
	merged := allTarballEntries(t, dir)
	found := false
	for name, body := range merged {
		if !strings.HasSuffix(name, ".json") {
			t.Errorf("entry %q should have the .json extension", name)
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(body), &rec); err != nil {
			t.Fatalf("entry %q is not valid JSON: %v\n%s", name, err, body)
		}
		if rec["GlobalJobId"] == "ap40#12.0#1700000000" && rec["Owner"] == "alice" {
			found = true
		}
	}
	if !found {
		t.Fatalf("alice record not exported as JSON; entries: %v", keys(merged))
	}

	cancel()
	<-done
}
