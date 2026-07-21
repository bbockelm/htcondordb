// Package kafkasync mirrors an htcondordb table's change stream into a Kafka topic so
// several database instances can be aggregated through a broker. It runs out-of-process as
// a dbrpc client: it reads its exporter definition and checkpoints its resume state in the
// database's catalog (via the exporter registry), and it is the ONLY component that depends
// on a Kafka client -- the core database never does.
//
// Delivery is at-least-once. The watch stream is at-least-once and has no before-image, and
// the checkpoint (in the DB) cannot be committed atomically with a produce (to Kafka), so a
// crash can re-deliver the tail after the last checkpoint. Consumers converge regardless:
// records are keyed by the ad key and carry a monotonic version header, and the topic is
// meant to be log-compacted, so a duplicate is a no-op and the latest value wins. See
// Runner for the reset/delete-sweep handling that covers the missing before-image.
package kafkasync

import (
	"encoding/json"
	"fmt"
	"time"
)

// Kind is the exporter kind this package implements; it is stored in db.ExporterDef.Kind.
const Kind = "kafka"

// Config is a Kafka exporter's definition, stored opaquely in db.ExporterDef.Config. The
// database never interprets it; only this package does.
type Config struct {
	// Table is the source table whose change stream is mirrored.
	Table string `json:"table"`
	// Brokers is the Kafka bootstrap broker list (host:port).
	Brokers []string `json:"brokers"`
	// Topic is the destination topic. It should be configured for log compaction so the
	// per-key latest value (and tombstones) are retained and duplicates collapse.
	Topic string `json:"topic"`

	// BatchSize flushes and checkpoints after this many produced records during live
	// tailing (0 = a sane default). A larger batch amortizes checkpoints; a smaller one
	// shortens the at-least-once replay window after a crash.
	BatchSize int `json:"batchSize,omitempty"`
	// FlushInterval flushes and checkpoints at least this often even below BatchSize, so a
	// trickle of changes is not stranded unacknowledged. 0 = a sane default.
	FlushInterval Duration `json:"flushInterval,omitempty"`

	// Topic management. By default the exporter ensures the topic exists with
	// cleanup.policy=compact at startup (a change-data changelog wants compaction so the
	// per-key latest value is retained and duplicates/tombstones collapse). Set
	// ManageTopic=false when the topic is provisioned externally (or the exporter's
	// principal lacks create rights); then Partitions/ReplicationFactor/Compact are unused.
	ManageTopic       *bool `json:"manageTopic,omitempty"`       // default true
	Compact           *bool `json:"compact,omitempty"`           // default true
	Partitions        int   `json:"partitions,omitempty"`        // default 1
	ReplicationFactor int   `json:"replicationFactor,omitempty"` // default 1

	// SASL/TLS are intentionally minimal for now; extend as deployments require. Empty
	// means a plaintext connection (suitable for a trusted network or a local broker).
	SASL *SASLConfig `json:"sasl,omitempty"`
	TLS  bool        `json:"tls,omitempty"`
}

func boolOr(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}

// ManagesTopic reports whether the exporter should create/configure the topic itself.
func (c Config) ManagesTopic() bool { return boolOr(c.ManageTopic, true) }

// Compacts reports whether a managed topic is created with cleanup.policy=compact.
func (c Config) Compacts() bool { return boolOr(c.Compact, true) }

// SASLConfig configures SASL/PLAIN authentication to the broker. Its presence in the stored
// config is why GetExporter is DAEMON-gated on the server.
type SASLConfig struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

const (
	defaultBatchSize     = 500
	defaultFlushInterval = 2 * time.Second
)

// Validate checks a config is usable and returns it with defaults filled in.
func (c Config) Validate() (Config, error) {
	if c.Table == "" {
		return c, fmt.Errorf("kafka exporter: table must be set")
	}
	if len(c.Brokers) == 0 {
		return c, fmt.Errorf("kafka exporter: at least one broker must be set")
	}
	if c.Topic == "" {
		return c, fmt.Errorf("kafka exporter: topic must be set")
	}
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = Duration(defaultFlushInterval)
	}
	if c.Partitions <= 0 {
		c.Partitions = 1
	}
	if c.ReplicationFactor <= 0 {
		c.ReplicationFactor = 1
	}
	return c, nil
}

// ParseConfig unmarshals and validates a Kafka exporter config from its opaque JSON.
func ParseConfig(raw json.RawMessage) (Config, error) {
	var c Config
	if err := json.Unmarshal(raw, &c); err != nil {
		return Config{}, fmt.Errorf("kafka exporter config: %w", err)
	}
	return c.Validate()
}

// Marshal serializes a config to the opaque JSON stored in db.ExporterDef.Config.
func (c Config) Marshal() (json.RawMessage, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Duration is a time.Duration that marshals as a Go duration string ("2s") in JSON, so the
// stored config is human-readable.
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
	case float64: // bare number = nanoseconds, matching time.Duration
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
