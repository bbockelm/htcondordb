package opensearchsync

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

// These tests run against a REAL OpenSearch reachable at $OPENSEARCH_URL (set by the
// opensearch-integration CI workflow). They are skipped otherwise, so the normal `go test`
// suite needs no server.

func osIntegrationClient(t *testing.T) *osBulkClient {
	t.Helper()
	url := os.Getenv("OPENSEARCH_URL")
	if url == "" {
		t.Skip("set OPENSEARCH_URL to run the OpenSearch integration tests")
	}
	c, err := NewOSBulkClient(Config{
		Addresses:          []string{url},
		Username:           os.Getenv("OPENSEARCH_USER"),
		PasswordEnv:        "OPENSEARCH_PASSWORD", // resolved from env by NewOSBulkClient
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatalf("NewOSBulkClient: %v", err)
	}
	return c
}

func deleteIndex(t *testing.T, c *osBulkClient, index string) {
	t.Helper()
	_, _ = c.api.Indices.Delete(context.Background(), opensearchapi.IndicesDeleteReq{Indices: []string{index}})
}

// TestIntegrationBulkExternalVersioning verifies end-to-end that the bulk path is idempotent via
// external versioning: a stale version is a no-op conflict, a newer version overwrites.
func TestIntegrationBulkExternalVersioning(t *testing.T) {
	c := osIntegrationClient(t)
	ctx := context.Background()
	const idx = "opensearchsync-it-versioning"
	deleteIndex(t, c, idx)
	if err := c.EnsureIndex(ctx, idx); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	defer deleteIndex(t, c, idx)

	bulk := func(version uint64, owner string) BulkOutcome {
		docs := []Doc{{ID: "job1", Version: version, Body: []byte(`{"Owner":"` + owner + `","Status":"Completed"}`)}}
		out, err := c.Bulk(ctx, idx, buildBulkBody(idx, docs))
		if err != nil {
			t.Fatalf("Bulk(v%d): %v", version, err)
		}
		return out
	}

	if out := bulk(10, "alice"); out.Indexed != 1 {
		t.Fatalf("initial index: %+v, want Indexed 1", out)
	}
	if out := bulk(5, "stale"); out.Conflicts != 1 || out.Indexed != 0 {
		t.Errorf("stale version: %+v, want Conflicts 1 (external version rejects)", out)
	}
	if out := bulk(20, "bob"); out.Indexed != 1 {
		t.Errorf("newer version: %+v, want Indexed 1 (overwrite)", out)
	}
}

// TestIntegrationAdditiveMappingPatch verifies EnsureIndex additively adds the adstash field
// mappings to an existing index without dropping pre-existing fields.
func TestIntegrationAdditiveMappingPatch(t *testing.T) {
	c := osIntegrationClient(t)
	ctx := context.Background()
	const idx = "opensearchsync-it-patch"
	deleteIndex(t, c, idx)
	defer deleteIndex(t, c, idx)

	// Pre-create a bare index that lacks the adstash fields but carries a custom one.
	bare := `{"mappings":{"properties":{"Foo":{"type":"keyword"}}}}`
	if _, err := c.api.Indices.Create(ctx, opensearchapi.IndicesCreateReq{Index: idx, Body: strings.NewReader(bare)}); err != nil {
		t.Fatalf("pre-create bare index: %v", err)
	}

	if err := c.EnsureIndex(ctx, idx); err != nil {
		t.Fatalf("EnsureIndex (patch): %v", err)
	}

	props := integrationMappingProps(t, c, idx)
	// A known adstash field must now be present and typed, and the pre-existing field preserved.
	if _, ok := props["Owner"]; !ok {
		t.Error("additive patch did not add the Owner field")
	}
	if _, ok := props["JobStatus"]; !ok {
		t.Error("additive patch did not add the JobStatus field")
	}
	if _, ok := props["Foo"]; !ok {
		t.Error("additive patch dropped the pre-existing Foo field")
	}

	// A second EnsureIndex is a clean no-op (nothing missing).
	if err := c.EnsureIndex(ctx, idx); err != nil {
		t.Fatalf("EnsureIndex (idempotent): %v", err)
	}
}

func integrationMappingProps(t *testing.T, c *osBulkClient, index string) map[string]json.RawMessage {
	t.Helper()
	resp, err := c.api.Indices.Mapping.Get(context.Background(), &opensearchapi.MappingGetReq{Indices: []string{index}})
	if err != nil {
		t.Fatalf("mapping get: %v", err)
	}
	for _, idx := range resp.GetIndices() {
		var m struct {
			Properties map[string]json.RawMessage `json:"properties"`
		}
		if err := json.Unmarshal(idx.Mappings, &m); err != nil {
			t.Fatalf("unmarshal mapping: %v", err)
		}
		return m.Properties
	}
	t.Fatalf("no mapping returned for %q", index)
	return nil
}

// TestIntegrationRunnerToOpenSearch runs the full pipeline against a real OpenSearch: a history
// archive change stream -> the adstash transform -> the async bulk uploader -> the index. It
// verifies the transformed document lands under its adstash id with the derived/typed fields.
func TestIntegrationRunnerToOpenSearch(t *testing.T) {
	c := osIntegrationClient(t)
	ctx := context.Background()
	const idx = "opensearchsync-it-runner"
	deleteIndex(t, c, idx)
	defer deleteIndex(t, c, idx)
	if err := c.EnsureIndex(ctx, idx); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}

	// In-process dbrpc server + a history archive holding one completed job.
	dbc, cat := testServer(t)
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, arch, `GlobalJobId = "ap40#12.0#1700000000"; JobStatus = 4; JobUniverse = 5; CompletionDate = 1700000100; Owner = "alice"; ClusterId = 12`)

	cfg := Config{Table: "history", Addresses: []string{os.Getenv("OPENSEARCH_URL")}, Index: idx, InsecureSkipVerify: true}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, _ := cfg.Marshal()
	if err := dbc.CreateExporter(ctx, db.ExporterDef{Name: "hist-real", Kind: Kind, Config: raw}); err != nil {
		t.Fatal(err)
	}

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- NewRunner("hist-real", cfg, dbc, c, testLaunch, nil).Run(rctx) }()

	id := "ap40#12.0#1700000000#1700000100"
	waitFor(t, func() bool {
		_, _ = c.api.Indices.Refresh(ctx, &opensearchapi.IndicesRefreshReq{Index: []string{idx}})
		resp, err := c.api.Document.Get(ctx, opensearchapi.DocumentGetReq{Index: idx, DocumentID: id})
		return err == nil && resp.Found
	})
	resp, err := c.api.Document.Get(ctx, opensearchapi.DocumentGetReq{Index: idx, DocumentID: id})
	if err != nil || !resp.Found {
		t.Fatalf("document %q not found in OpenSearch (err %v)", id, err)
	}
	src := string(resp.Source)
	for _, want := range []string{`"Owner":"alice"`, `"Status":"Completed"`, `"Universe":"Vanilla"`, `"RecordTime":1700000100`, `"ScheddName":"ap40"`} {
		if !strings.Contains(src, want) {
			t.Errorf("indexed doc missing %s:\n%s", want, src)
		}
	}
	cancel()
	<-done
}
