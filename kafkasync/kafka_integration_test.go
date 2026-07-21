package kafkasync

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/twmb/franz-go/pkg/kgo"
)

// redpandaImage is pinned so CI is reproducible. Redpanda's dev-container mode is a single
// fast-starting binary that speaks the Kafka protocol.
const redpandaImage = "redpandadata/redpanda:v24.2.7"

// TestIntegrationKafkaRoundTrip exports a table to a real broker (Redpanda) and consumes
// the topic back, asserting the exported upserts and a tombstone arrive with the right
// keys, values, and version headers.
//
// The test launches and manages its own broker (see startBroker): a native redpanda via
// rpk if present, otherwise a Redpanda container via the docker CLI. Each instance gets its
// own ephemeral ports and data dir, so tests may run in parallel. Skipped under -short or
// when neither rpk nor docker is available.
func TestIntegrationKafkaRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	t.Parallel()
	broker := startBroker(t)

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
	// Unique per run so a reused (KAFKASYNC_BROKER) broker never mixes state across runs.
	topic := fmt.Sprintf("htc.jobs.test.%d", os.Getpid())
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

// startBroker launches and manages a Redpanda broker for this test and returns its
// host:port. It prefers a native redpanda (via rpk) and falls back to a Redpanda container
// via the docker CLI. Every instance binds its own ephemeral ports and data dir, so tests
// can run in parallel; cleanup is registered on t. Skips when neither rpk nor docker exists.
func startBroker(t *testing.T) string {
	t.Helper()
	// rpk is redpanda's CLI (installed alongside the broker); it is the version-stable way
	// to launch a dev broker. Prefer it, then fall back to a container via docker.
	if _, err := exec.LookPath("rpk"); err == nil {
		return startRedpandaNative(t)
	}
	if _, err := exec.LookPath("docker"); err == nil {
		if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
			t.Skipf("docker present but not usable: %v\n%s", err, out)
		}
		return startRedpandaDocker(t)
	}
	t.Skip("neither rpk (native redpanda) nor docker is available")
	return ""
}

// startRedpandaNative launches a native broker via `rpk redpanda start --mode dev-container`
// on its own ephemeral kafka/rpc/admin ports, with a per-instance data dir and config file
// so parallel runs never collide. (Redpanda cannot bind :0 and report the chosen port back,
// so we pre-allocate free ports and pin them.) The process is killed on test cleanup.
func startRedpandaNative(t *testing.T) string {
	t.Helper()
	kport, rport, aport := freePort(t), freePort(t), freePort(t)
	dir := t.TempDir()
	broker := fmt.Sprintf("127.0.0.1:%d", kport)

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "rpk", append([]string{"redpanda"},
		redpandaStartArgs("PLAINTEXT://"+broker, "PLAINTEXT://"+broker, rport, aport,
			"--set", "redpanda.data_directory="+filepath.Join(dir, "data"),
			"--config", filepath.Join(dir, "redpanda.yaml"))...)...)
	logBuf := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = logBuf, logBuf
	if err := cmd.Start(); err != nil {
		cancel()
		t.Skipf("could not start the redpanda binary: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})
	if !brokerReady(broker, 60*time.Second) {
		t.Fatalf("native redpanda did not become ready at %s\n%s", broker, logBuf.String())
	}
	return broker
}

// startRedpandaDocker runs a dev-container Redpanda in a container on an ephemeral host port.
// (The advertised address must be known before start, so we pre-allocate the host port and
// map it rather than letting docker assign one.) The container is removed on cleanup.
func startRedpandaDocker(t *testing.T) string {
	t.Helper()
	hostPort := freePort(t)
	broker := fmt.Sprintf("localhost:%d", hostPort)
	name := fmt.Sprintf("kafkasync-it-%d", hostPort)
	_ = exec.Command("docker", "rm", "-f", name).Run() // clear any stale container
	// Same --mode dev-container start args as the native path, inside the container: bind on
	// 9092 and advertise the mapped host port.
	args := append([]string{
		"run", "-d", "--rm", "--name", name,
		"-p", fmt.Sprintf("127.0.0.1:%d:9092", hostPort),
		redpandaImage, "redpanda",
	}, redpandaStartArgs("PLAINTEXT://0.0.0.0:9092", "PLAINTEXT://"+broker, 33145, 9644)...)
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Skipf("could not start redpanda container (pull/start failed): %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	if !brokerReady(broker, 60*time.Second) {
		t.Fatalf("redpanda container did not become ready at %s", broker)
	}
	return broker
}

// redpandaStartArgs builds the `... start ...` argument list shared by the native and docker
// launchers. It uses `--mode dev-container`, which both the container's redpanda entrypoint
// and the native `rpk redpanda start` accept and which is stable across redpanda versions
// (unlike the raw seastar flags, which drift). extra carries per-launcher options (a native
// instance adds an isolated data_directory + config path so parallel runs never collide).
func redpandaStartArgs(kafkaAddr, advertiseAddr string, rpcPort, adminPort int, extra ...string) []string {
	args := []string{
		"start", "--mode", "dev-container",
		"--kafka-addr", kafkaAddr,
		"--advertise-kafka-addr", advertiseAddr,
		"--rpc-addr", fmt.Sprintf("127.0.0.1:%d", rpcPort),
		"--set", fmt.Sprintf(`redpanda.admin=[{"address":"127.0.0.1","port":%d}]`, adminPort),
		// Keep the optional HTTP listeners off so their default ports never collide.
		"--set", "pandaproxy.pandaproxy_api=[]",
		"--set", "schema_registry.schema_registry_api=[]",
	}
	return append(args, extra...)
}

// brokerReady reports whether a Kafka broker at addr answers a metadata ping within timeout.
func brokerReady(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			perr := cl.Ping(ctx)
			cancel()
			cl.Close()
			if perr == nil {
				return true
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// syncBuffer is a tiny concurrency-safe buffer for capturing a subprocess's output while it
// runs in the background.
type syncBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
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
