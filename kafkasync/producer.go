package kafkasync

import (
	"context"
	"crypto/tls"
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
	if cfg.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	if cfg.SASL != nil {
		opts = append(opts, kgo.SASL(plain.Auth{User: cfg.SASL.Username, Pass: cfg.SASL.Password}.AsMechanism()))
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
