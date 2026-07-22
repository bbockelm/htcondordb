package kafkasync

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// Header is a Kafka record header (metadata alongside key/value).
type Header struct {
	Key   string
	Value []byte
}

// Record is one change to publish. A nil Value is a tombstone (a delete), which a
// log-compacted topic garbage-collects.
type Record struct {
	Key     []byte
	Value   []byte
	Headers []Header
}

// Producer publishes records to the destination. Produce must return only after the broker
// has acknowledged every record (or an error): that synchronous-ack boundary is exactly
// what the Runner checkpoints against, and is what makes delivery at-least-once rather than
// best-effort. The abstraction also lets tests substitute an in-memory fake for the broker.
type Producer interface {
	Produce(ctx context.Context, recs []Record) error
	Close() error
}

// kgoProducer is the franz-go implementation of Producer. It runs with the idempotent
// producer enabled (franz-go's default) and acks=all, so in-session retries never create
// duplicates on a partition; cross-restart duplicates are handled by the version header +
// log compaction, not here.
type kgoProducer struct {
	cl    *kgo.Client
	topic string
}

// NewProducer builds a franz-go producer from a validated Config. Unless ManageTopic is
// disabled, it ensures the destination topic exists with cleanup.policy=compact before
// returning (a changelog topic wants compaction so per-key latest values and tombstones are
// retained and duplicates collapse). ctx bounds the topic-ensure step.
func NewProducer(ctx context.Context, cfg Config) (Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()), // required by the idempotent producer
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	}
	if cfg.TLS != nil {
		tlsCfg, err := buildTLSConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	if cfg.SASL != nil {
		// Resolve the password from its referenced source (file/env) here, in the exporter
		// process -- it is never read from, or stored in, the catalog config.
		pass, err := cfg.SASL.ResolvePassword()
		if err != nil {
			return nil, fmt.Errorf("kafka: %w", err)
		}
		opts = append(opts, kgo.SASL(plain.Auth{User: cfg.SASL.Username, Pass: pass}.AsMechanism()))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}
	if cfg.ManagesTopic() {
		if err := ensureTopic(ctx, cl, cfg); err != nil {
			cl.Close()
			return nil, err
		}
	}
	return &kgoProducer{cl: cl, topic: cfg.Topic}, nil
}

// buildTLSConfig turns a TLSConfig (all filesystem paths) into a *tls.Config, loading a
// custom CA and/or a client certificate for mutual TLS from disk in the exporter process.
func buildTLSConfig(t *TLSConfig) (*tls.Config, error) {
	c := &tls.Config{MinVersion: tls.VersionTLS12}
	if t.ServerName != "" {
		c.ServerName = t.ServerName
	}
	if t.CAFile != "" {
		pem, err := readCredentialFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kafka TLS: reading caFile: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafka TLS: caFile %q contained no valid certificates", t.CAFile)
		}
		c.RootCAs = pool
	}
	if t.CertFile != "" { // KeyFile presence is validated alongside in Config.Validate
		// Read both as root (the key is typically root-owned 0600), then pair in memory --
		// tls.LoadX509KeyPair would read them under the current identity.
		certPEM, err := readCredentialFile(t.CertFile)
		if err != nil {
			return nil, fmt.Errorf("kafka TLS: reading certFile: %w", err)
		}
		keyPEM, err := readCredentialFile(t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kafka TLS: reading keyFile: %w", err)
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("kafka TLS: loading client cert/key for mutual TLS: %w", err)
		}
		c.Certificates = []tls.Certificate{cert}
	}
	return c, nil
}

// ensureTopic creates the destination topic if it does not already exist, configuring
// compaction when requested. An already-existing topic is not an error (its config is left
// as-is, so an operator can pre-provision it).
func ensureTopic(ctx context.Context, cl *kgo.Client, cfg Config) error {
	adm := kadm.NewClient(cl)
	var topicCfg map[string]*string
	if cfg.Compacts() {
		policy := "compact"
		topicCfg = map[string]*string{"cleanup.policy": &policy}
	}
	resp, err := adm.CreateTopic(ctx, int32(cfg.Partitions), int16(cfg.ReplicationFactor), topicCfg, cfg.Topic)
	if err != nil {
		// A per-topic "already exists" arrives as resp.Err below; a top-level error here is
		// a connection/authorization failure.
		if errors.Is(err, kerr.TopicAlreadyExists) {
			return nil
		}
		return fmt.Errorf("kafka: ensuring topic %q: %w", cfg.Topic, err)
	}
	if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("kafka: ensuring topic %q: %w", cfg.Topic, resp.Err)
	}
	return nil
}

func (p *kgoProducer) Produce(ctx context.Context, recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	krecs := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		kr := &kgo.Record{Topic: p.topic, Key: r.Key, Value: r.Value}
		for _, h := range r.Headers {
			kr.Headers = append(kr.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
		}
		krecs = append(krecs, kr)
	}
	// ProduceSync blocks until the broker acknowledges every record (or one fails).
	return p.cl.ProduceSync(ctx, krecs...).FirstErr()
}

func (p *kgoProducer) Close() error {
	p.cl.Close()
	return nil
}
