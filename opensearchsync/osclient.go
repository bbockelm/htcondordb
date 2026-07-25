package opensearchsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

// osBulkClient is the opensearch-go-backed BulkClient. It performs one _bulk round-trip and
// classifies each item's status (409 conflict / 429-503 retryable / else permanent), which is
// all the Uploader needs; the Uploader owns retry, backpressure, and the in-flight watermark.
type osBulkClient struct {
	api *opensearchapi.Client
}

// NewOSBulkClient builds a BulkClient (and index manager) from cfg, resolving the password from
// its file/env reference and TLS material from disk. Nothing secret is taken from the stored
// Config.
func NewOSBulkClient(cfg Config) (*osBulkClient, error) {
	password, err := resolvePassword(cfg)
	if err != nil {
		return nil, err
	}
	oc := opensearch.Config{
		Addresses:          cfg.Addresses,
		Username:           cfg.Username,
		Password:           password,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}
	if cfg.CACertFile != "" {
		pem, err := os.ReadFile(cfg.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("opensearchsync: reading CA cert: %w", err)
		}
		oc.CACert = pem
	}
	api, err := opensearchapi.NewClient(opensearchapi.Config{Client: oc})
	if err != nil {
		return nil, fmt.Errorf("opensearchsync: building OpenSearch client: %w", err)
	}
	return &osBulkClient{api: api}, nil
}

func resolvePassword(cfg Config) (string, error) {
	switch {
	case cfg.PasswordFile != "":
		b, err := os.ReadFile(cfg.PasswordFile)
		if err != nil {
			return "", fmt.Errorf("opensearchsync: reading password file: %w", err)
		}
		return strings.TrimSpace(string(b)), nil
	case cfg.PasswordEnv != "":
		return os.Getenv(cfg.PasswordEnv), nil
	default:
		return "", nil
	}
}

// Bulk implements BulkClient.
func (c *osBulkClient) Bulk(ctx context.Context, index string, ndjson []byte) (BulkOutcome, error) {
	resp, err := c.api.Bulk(ctx, opensearchapi.BulkReq{Index: index, Body: bytes.NewReader(ndjson)})
	if err != nil {
		return BulkOutcome{}, err
	}
	var out BulkOutcome
	for _, item := range resp.Items {
		for _, res := range item { // one entry per action ("index")
			conflict, retryable := classifyStatus(res.Status)
			switch {
			case res.Status >= 200 && res.Status < 300:
				out.Indexed++
			case conflict:
				out.Conflicts++
			case retryable:
				out.Retryable++
			default:
				e := ItemError{ID: res.ID, Status: res.Status}
				if res.Error != nil {
					e.Type = res.Error.Type
					e.Reason = res.Error.Reason
				}
				out.Permanent = append(out.Permanent, e)
			}
		}
	}
	return out, nil
}

// EnsureIndex creates the target index with the adstash mappings if it does not exist, and
// otherwise additively patches it -- adding any explicit field mappings that are missing without
// touching or narrowing existing ones (mirroring adstash's setup_index). This keeps the index in
// step as new attributes appear in the transform's tables, and is safe to call on every start.
func (c *osBulkClient) EnsureIndex(ctx context.Context, index string) error {
	exists, err := c.api.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{Indices: []string{index}})
	if err == nil && exists.StatusCode == http.StatusOK {
		return c.patchIndexMapping(ctx, index) // present (index or alias): add missing fields
	}
	body, err := json.Marshal(buildMappings())
	if err != nil {
		return err
	}
	_, err = c.api.Indices.Create(ctx, opensearchapi.IndicesCreateReq{Index: index, Body: bytes.NewReader(body)})
	if err != nil {
		// A concurrent creator (or an existing alias) can race us; treat "already exists" as an
		// existing index and fall through to the additive patch.
		if strings.Contains(err.Error(), "resource_already_exists_exception") {
			return c.patchIndexMapping(ctx, index)
		}
		return fmt.Errorf("opensearchsync: creating index %q: %w", index, err)
	}
	return nil
}

// patchIndexMapping reads the current mapping of index (resolving an alias to its concrete
// indices) and PUTs only the explicit field mappings that are absent -- never replacing or
// narrowing an existing field, so it cannot break an index or lose data. Dynamic templates set
// at creation are left as-is.
func (c *osBulkClient) patchIndexMapping(ctx context.Context, index string) error {
	resp, err := c.api.Indices.Mapping.Get(ctx, &opensearchapi.MappingGetReq{Indices: []string{index}})
	if err != nil {
		return fmt.Errorf("opensearchsync: reading mapping of %q: %w", index, err)
	}
	desired := indexProperties()
	for name, idx := range resp.GetIndices() {
		var current struct {
			Properties map[string]json.RawMessage `json:"properties"`
		}
		_ = json.Unmarshal(idx.Mappings, &current)

		missing := map[string]any{}
		for field, spec := range desired {
			if _, ok := current.Properties[field]; !ok {
				missing[field] = spec
			}
		}
		if len(missing) == 0 {
			continue
		}
		body, err := json.Marshal(map[string]any{"properties": missing})
		if err != nil {
			return err
		}
		if _, err := c.api.Indices.Mapping.Put(ctx, opensearchapi.MappingPutReq{Indices: []string{name}, Body: bytes.NewReader(body)}); err != nil {
			return fmt.Errorf("opensearchsync: patching mapping of %q (+%d fields): %w", name, len(missing), err)
		}
	}
	return nil
}
