// Package opensearchsync mirrors an htcondordb table's change stream into an
// OpenSearch/Elasticsearch index, applying the condor_adstash document transform. Like
// kafkasync it is a standalone module + binary (a dbrpc client), so the OpenSearch client
// dependency never enters the core daemon build; it registers via the generic exporter
// registry with Kind = "opensearch".
//
// Delivery is at-least-once. Documents are bulk-indexed with _id = GlobalJobId#RecordTime and
// version_type=external (version = a monotonic ExportSeq), so duplicate/stale re-indexes are
// idempotent no-op version conflicts. Bulk requests are submitted with a bounded in-flight
// window; the resume cursor advances only over the oldest fully-acked contiguous prefix, and
// reading blocks when too many documents are in flight — capping the restart replay cost.
package opensearchsync

import (
	"encoding/json"
	"fmt"
	"time"
)

// Kind is the exporter registry kind for this exporter.
const Kind = "opensearch"

// Config is one OpenSearch exporter's definition, stored opaquely in ExporterDef.Config.
// Secrets are referenced, never stored: Password comes from PasswordFile/PasswordEnv resolved
// in this process at runtime, and TLS material is filesystem paths.
type Config struct {
	// Table is the source table/archive whose change stream is mirrored (e.g. "history").
	Table string `json:"table"`

	// Addresses are the OpenSearch node URLs (e.g. "https://os1.example.edu:9200").
	Addresses []string `json:"addresses"`
	// Index is the target index or write alias. Default "htcondor-000001".
	Index string `json:"index,omitempty"`

	// Username and one of PasswordFile/PasswordEnv provide HTTP basic auth (optional).
	Username     string `json:"username,omitempty"`
	PasswordFile string `json:"passwordFile,omitempty"`
	PasswordEnv  string `json:"passwordEnv,omitempty"`

	// CACertFile is a PEM bundle to verify the server; InsecureSkipVerify disables
	// verification (test only).
	CACertFile         string `json:"caCertFile,omitempty"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty"`

	// BatchSize is the max documents per _bulk request. Default 250 (adstash's se_bunch_size).
	BatchSize int `json:"batchSize,omitempty"`
	// FlushInterval bounds how long documents wait before a partial batch is sent / the
	// cursor is checkpointed. Default 2s.
	FlushInterval Duration `json:"flushInterval,omitempty"`
	// RequestTimeout bounds a single _bulk request. Default 120s (adstash's se_timeout).
	RequestTimeout Duration `json:"requestTimeout,omitempty"`

	// MaxConcurrentBulk is the number of _bulk requests that may be outstanding at once.
	// Default 4.
	MaxConcurrentBulk int `json:"maxConcurrentBulk,omitempty"`
	// MaxInFlightDocs caps the total documents across all outstanding bulk requests; reading
	// the change stream blocks once this is reached (backpressure). It bounds how much must be
	// replayed on restart. Default 5000.
	MaxInFlightDocs int `json:"maxInFlightDocs,omitempty"`

	// ManageIndex, when true (default), creates the index with the adstash mappings if it is
	// missing and additively patches any missing fields.
	ManageIndex *bool `json:"manageIndex,omitempty"`
}

// Defaults, exported so the cmd help and tests can reference them.
const (
	DefaultIndex             = "htcondor-000001"
	DefaultBatchSize         = 250
	DefaultFlushInterval     = 2 * time.Second
	DefaultRequestTimeout    = 120 * time.Second
	DefaultMaxConcurrentBulk = 4
	DefaultMaxInFlightDocs   = 5000
)

// Validate fills defaults and checks required fields.
func (c *Config) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("opensearchsync: table is required")
	}
	if len(c.Addresses) == 0 {
		return fmt.Errorf("opensearchsync: at least one address is required")
	}
	if c.PasswordFile != "" && c.PasswordEnv != "" {
		return fmt.Errorf("opensearchsync: set at most one of passwordFile/passwordEnv")
	}
	if c.Index == "" {
		c.Index = DefaultIndex
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = Duration(DefaultFlushInterval)
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = Duration(DefaultRequestTimeout)
	}
	if c.MaxConcurrentBulk <= 0 {
		c.MaxConcurrentBulk = DefaultMaxConcurrentBulk
	}
	if c.MaxInFlightDocs <= 0 {
		c.MaxInFlightDocs = DefaultMaxInFlightDocs
	}
	// A single bulk batch must fit within the in-flight budget or reading would deadlock.
	if c.MaxInFlightDocs < c.BatchSize {
		c.MaxInFlightDocs = c.BatchSize
	}
	return nil
}

// ManageIndexEnabled reports the effective ManageIndex setting (default true).
func (c *Config) ManageIndexEnabled() bool { return c.ManageIndex == nil || *c.ManageIndex }

// Marshal serializes the config for storage in an ExporterDef.
func (c Config) Marshal() ([]byte, error) { return json.Marshal(c) }

// ParseConfig unmarshals an exporter Config from its stored JSON and validates it.
func ParseConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("opensearchsync: parsing config: %w", err)
		}
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

// Duration is a JSON-friendly time.Duration ("2s", "120s"; a bare number is nanoseconds).
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch x := v.(type) {
	case float64:
		*d = Duration(x)
	case string:
		dur, err := time.ParseDuration(x)
		if err != nil {
			return err
		}
		*d = Duration(dur)
	default:
		return fmt.Errorf("invalid duration %v", v)
	}
	return nil
}
