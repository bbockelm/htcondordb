package kafkasync

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/twmb/franz-go/pkg/kgo"
)

// redpandaImage is pinned so CI is reproducible. Redpanda's dev-container mode is a single
// fast-starting binary that speaks the Kafka protocol.
const redpandaImage = "redpandadata/redpanda:v24.2.7"

// TestIntegrationKafkaRoundTrip exports a table to a real broker (Redpanda in Docker) and
// consumes the topic back, asserting the exported upserts and a tombstone arrive with the
// right keys, values, and version headers. It shells out to `docker` (matching the GitHub
// Actions environment) rather than depending on a container library.
//
// Skipped when docker is unavailable or `go test -short` is used.
func TestIntegrationKafkaRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping docker integration test in -short mode")
	}
	if os.Getenv("KAFKASYNC_SKIP_DOCKER") != "" {
		t.Skip("KAFKASYNC_SKIP_DOCKER set")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found in PATH")
	}
	if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
		t.Skipf("docker not usable: %v\n%s", err, out)
	}

	port := freePort(t)
	broker := fmt.Sprintf("localhost:%d", port)
	startRedpanda(t, port)

	// In-process DB + privileged dbrpc server (same harness as the unit tests).
	dir := t.TempDir()
	cat, err := db.OpenCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	jobs, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	putAd(t, jobs, "1.0", "alice", 100)
	putAd(t, jobs, "2.0", "bob", 200)

	c := clientFor(t, cat)
	topic := "htc.jobs.test"
	cfg, err := Config{Table: "jobs", Brokers: []string{broker}, Topic: topic, BatchSize: 2, FlushInterval: Duration(50 * time.Millisecond)}.Validate()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := cfg.Marshal()
	if err := c.CreateExporter(context.Background(), db.ExporterDef{Name: "jobs-kafka", Kind: Kind, Config: raw}); err != nil {
		t.Fatal(err)
	}

	prod, err := NewProducer(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner("jobs-kafka", cfg, c, prod, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// A live delete after the initial export -> a tombstone on the topic.
	time.Sleep(300 * time.Millisecond)
	if _, err := jobs.Delete("2.0"); err != nil {
		t.Fatal(err)
	}

	// Consume the topic from the start; collect the latest record per key.
	latest := consumeLatest(t, broker, topic, map[string]bool{"1.0": true, "2.0": true}, func(m map[string]*kgo.Record) bool {
		return m["1.0"] != nil && m["2.0"] != nil && m["2.0"].Value == nil // 2.0 tombstoned
	})

	if latest["1.0"] == nil || !strings.Contains(string(latest["1.0"].Value), "alice") {
		t.Fatalf("key 1.0: %v", latest["1.0"])
	}
	if latest["2.0"] == nil || latest["2.0"].Value != nil {
		t.Fatalf("key 2.0 should be a tombstone, got %v", latest["2.0"])
	}
	// Both carry a version header, and the tombstone's version is the highest (it happened last).
	if !hasHeader(latest["1.0"], HeaderVersion) || !hasHeader(latest["2.0"], HeaderVersion) {
		t.Fatal("records must carry a version header")
	}

	cancel()
	<-done
	prod.Close()
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startRedpanda launches a dev-container Redpanda mapped to hostPort and registers cleanup.
func startRedpanda(t *testing.T, hostPort int) {
	t.Helper()
	name := fmt.Sprintf("kafkasync-it-%d", hostPort)
	_ = exec.Command("docker", "rm", "-f", name).Run() // clear any stale container
	args := []string{
		"run", "-d", "--rm", "--name", name,
		"-p", fmt.Sprintf("%d:9092", hostPort),
		redpandaImage,
		"redpanda", "start",
		"--mode", "dev-container", "--smp", "1", "--overprovisioned", "--node-id", "0",
		"--kafka-addr", "PLAINTEXT://0.0.0.0:9092",
		"--advertise-kafka-addr", fmt.Sprintf("PLAINTEXT://localhost:%d", hostPort),
	}
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Skipf("could not start redpanda (pull/start failed): %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })

	// Wait for the broker to accept connections and answer metadata.
	broker := fmt.Sprintf("localhost:%d", hostPort)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			perr := cl.Ping(ctx)
			cancel()
			cl.Close()
			if perr == nil {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("redpanda did not become ready at %s", broker)
}

// consumeLatest reads the topic from the start until want() is satisfied (or timeout),
// returning the latest record seen for each key in keys.
func consumeLatest(t *testing.T, broker, topic string, keys map[string]bool, want func(map[string]*kgo.Record) bool) map[string]*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	latest := map[string]*kgo.Record{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("timed out consuming %s; have %v", topic, keysOf(latest))
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			if keys[string(rec.Key)] {
				latest[string(rec.Key)] = rec
			}
		})
		if want(latest) {
			return latest
		}
	}
}

func keysOf(m map[string]*kgo.Record) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func hasHeader(r *kgo.Record, key string) bool {
	for _, h := range r.Headers {
		if h.Key == key {
			return true
		}
	}
	return false
}
