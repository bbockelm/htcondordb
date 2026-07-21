package repl

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// TestExporterMetaCommands exercises the read-only .exporters / .exporter visibility over a
// privileged catalog-backed connection: listing name+kind, showing a definition with its
// opaque config, and reporting resume-state presence.
func TestExporterMetaCommands(t *testing.T) {
	e, cat, cleanup := newPrivCatalogExec(t)
	defer cleanup()

	// Seed an exporter directly in the catalog (creation is done by the kafkasync CLI, not
	// the repl; the repl only reads).
	def := db.ExporterDef{
		Name:   "jobs-kafka",
		Kind:   "kafka",
		Config: json.RawMessage(`{"table":"jobs","brokers":["b:9092"],"topic":"htc.jobs"}`),
	}
	if err := cat.CreateExporter(def); err != nil {
		t.Fatal(err)
	}

	s := &session{exec: e}

	// .exporters lists name + kind.
	var list bytes.Buffer
	s.showExporters(&list)
	if !strings.Contains(list.String(), "jobs-kafka") || !strings.Contains(list.String(), "kafka") {
		t.Fatalf(".exporters output = %q", list.String())
	}

	// .exporter <name> shows the definition, the config, and "no state" before any checkpoint.
	var show bytes.Buffer
	s.showExporter(&show, "jobs-kafka")
	out := show.String()
	for _, want := range []string{"name:", "jobs-kafka", "kind:", "kafka", "config:", "htc.jobs", "state:", "none"} {
		if !strings.Contains(out, want) {
			t.Fatalf(".exporter output missing %q; got:\n%s", want, out)
		}
	}

	// After a checkpoint, state is reported present with its size.
	if err := cat.SaveExporterState("jobs-kafka", []byte("cursor=abc;seq=7")); err != nil {
		t.Fatal(err)
	}
	var show2 bytes.Buffer
	s.showExporter(&show2, "jobs-kafka")
	if !strings.Contains(show2.String(), "state:  present") {
		t.Fatalf(".exporter after checkpoint = %q", show2.String())
	}

	// Unknown name is reported cleanly.
	var missing bytes.Buffer
	s.showExporter(&missing, "ghost")
	if !strings.Contains(missing.String(), "no exporter named") {
		t.Fatalf(".exporter ghost = %q", missing.String())
	}
}

// TestExporterListEmpty: no exporters -> a friendly message, not an error.
func TestExporterListEmpty(t *testing.T) {
	e, _, cleanup := newPrivCatalogExec(t)
	defer cleanup()
	var buf bytes.Buffer
	(&session{exec: e}).showExporters(&buf)
	if !strings.Contains(buf.String(), "no exporters") {
		t.Fatalf("empty .exporters = %q", buf.String())
	}
}
