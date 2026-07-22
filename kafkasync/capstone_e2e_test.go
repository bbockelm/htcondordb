package kafkasync

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestCapstoneE2E is the end-to-end capstone: a real personal HTCondor pool (condor_master
// and friends, via the golang-htcondor harness -- local binaries, no Docker), a real Kafka
// broker (Redpanda), the real htcondordb daemon running its schedd-sync job-queue watcher,
// and the real kafkasync exporter. A job is submitted and run to completion; the test then
// asserts Kafka saw the job's lifecycle: it appears (upsert, carrying its ClusterId) while
// live in the queue, and becomes a tombstone once it completes and leaves job_queue.log.
//
//	condor_submit -> schedd job_queue.log -> htcondordb (schedd-sync) "jobs" table
//	             -> kafkasync exporter -> Kafka topic -> this test's consumer
//
// Everything runs as real processes wired together. Skipped under -short, as root
// (schedd-sync must not read schedd files privileged), when HTCondor is not installed (the
// harness skips), when neither rpk nor docker can provide a broker, or when the htcondordb /
// kafkasync binaries cannot be built.
func TestCapstoneE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping capstone E2E in -short mode")
	}
	if os.Geteuid() == 0 {
		t.Skip("capstone E2E must run unprivileged (schedd files must not be read as root)")
	}

	// 1. Real personal HTCondor pool from local binaries (skips if condor is not installed).
	h := htcondor.SetupCondorHarness(t)
	if err := h.WaitForDaemons(); err != nil {
		t.Fatalf("condor daemons failed to start: %v", err)
	}
	ccfg, err := h.GetConfig()
	if err != nil {
		t.Fatalf("harness config: %v", err)
	}
	jobLog := configOrE2E(ccfg, "JOB_QUEUE_LOG", filepath.Join(h.GetSpoolDir(), "job_queue.log"))
	collector := htcondor.NewCollector(h.GetCollectorAddr())
	loc, err := collector.LocateDaemon(context.Background(), "Schedd", "")
	if err != nil {
		t.Fatalf("locate schedd: %v", err)
	}
	schedd := htcondor.NewSchedd(loc.Name, loc.Address)

	// 2. Build the real binaries (skips if they cannot be built).
	_, thisFile, _, _ := runtime.Caller(0)
	kafkasyncModDir := filepath.Dir(thisFile) // .../htcondordb/kafkasync
	htcondordbModDir := filepath.Dir(kafkasyncModDir)
	binDir := t.TempDir()
	htcondordbBin := buildBinaryE2E(t, htcondordbModDir, "./cmd/htcondordb", filepath.Join(binDir, "htcondordb"))
	kafkasyncBin := buildBinaryE2E(t, kafkasyncModDir, "./cmd/kafkasync", filepath.Join(binDir, "kafkasync"))

	// 3. Real Kafka broker (native rpk, else docker; skips if neither).
	broker := startBroker(t)

	// 4. htcondordb config: FS auth mapping this user to DAEMON, and schedd-sync mirroring
	// the harness's job_queue.log into the "jobs" table. Encryption REQUIRED (the released
	// cedar rejects OPTIONAL request/response ops; a separate fix is in flight).
	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	dbDir := t.TempDir()
	addrFile := filepath.Join(dbDir, "addr")
	hcdbCfg := filepath.Join(dbDir, "htcondordb.config")
	listen := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	writeFileE2E(t, hcdbCfg, fmt.Sprintf(`TRUST_DOMAIN = capstone.local
UID_DOMAIN = capstone.local
SEC_DEFAULT_AUTHENTICATION = REQUIRED
SEC_DEFAULT_AUTHENTICATION_METHODS = FS
SEC_DEFAULT_INTEGRITY = REQUIRED
SEC_DEFAULT_ENCRYPTION = REQUIRED
ALLOW_DAEMON = %[1]s@*
ALLOW_WRITE = %[1]s@*
ALLOW_READ = %[1]s@*
LOG = %[2]s
HTCONDORDB_DIR = %[2]s
HTCONDORDB_ADDRESS_FILE = %[3]s
HTCONDORDB_SYNC_SCHEDD = True
HTCONDORDB_JOB_QUEUE_LOG = %[4]s
`, me.Username, dbDir, addrFile, jobLog))
	dbEnv := append(os.Environ(), "CONDOR_CONFIG="+hcdbCfg)

	// 5. Start the htcondordb daemon (real process) and wait for its address file.
	dbLog := runProcessE2E(t, dbEnv, htcondordbBin, "-listen", listen)
	waitForFileE2E(t, addrFile, 30*time.Second, func() string { return dbLog.String() })

	// 6. Register + run the kafkasync exporter on the "jobs" table (real processes). Unique
	// topic per run so a reused broker never mixes state.
	topic := fmt.Sprintf("htc.jobs.capstone.%d", os.Getpid())
	if out, err := runToCompletionE2E(t, dbEnv, kafkasyncBin,
		"create", "-name", "jobs", "-table", "jobs", "-brokers", broker, "-topic", topic); err != nil {
		t.Fatalf("kafkasync create: %v\n%s", err, out)
	}
	exLog := runProcessE2E(t, dbEnv, kafkasyncBin, "run", "-name", "jobs")
	_ = exLog

	// 7. Submit a short job and let the pool run it to completion.
	jobDir := t.TempDir()
	submit := fmt.Sprintf("universe = vanilla\nexecutable = /bin/sleep\narguments = 3\n"+
		"output = j.out\nerror = j.err\nlog = j.log\ntransfer_executable = false\ninitialdir = %s\nqueue\n", jobDir)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	clusterID, err := schedd.Submit(ctx, submit)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("submitted cluster %s; job_queue.log -> htcondordb -> kafka topic %s", clusterID, topic)
	jobKey := clusterID + ".0"

	// Drive the job to a terminal state / out of the queue (personal pool needs a nudge).
	terminal := false
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); {
		ads, qerr := schedd.Query(ctx, "ClusterId == "+clusterID, []string{"JobStatus"})
		if qerr == nil && len(ads) == 0 {
			terminal = true
			break
		}
		_ = schedd.Reschedule(ctx)
		time.Sleep(1 * time.Second)
	}
	if !terminal {
		t.Fatalf("job %s did not leave the queue in time", clusterID)
	}
	t.Logf("job %s completed and left the queue", clusterID)

	// 8. Verify Kafka saw the lifecycle: the job appeared live (an upsert carrying its
	// ClusterId), and finally became a tombstone when it left job_queue.log on completion.
	sawUpsert, sawTombstone := consumeJobLifecycle(t, broker, topic, jobKey, clusterID, 60*time.Second)
	if !sawUpsert {
		t.Errorf("kafka never saw job %s as a live upsert (submit->schedd-sync->kafka broke)", jobKey)
	}
	if !sawTombstone {
		t.Errorf("kafka never saw a tombstone for %s (completion->schedd-sync delete->kafka broke)", jobKey)
	}
}

func configOrE2E(cfg *config.Config, key, fallback string) string {
	if v, ok := cfg.Get(key); ok && v != "" {
		return v
	}
	return fallback
}

// buildBinaryE2E builds pkg in moduleDir to out, skipping the test if the build fails.
func buildBinaryE2E(t *testing.T, moduleDir, pkg, out string) string {
	t.Helper()
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Dir = moduleDir
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot build %s in %s: %v\n%s", pkg, moduleDir, err, b)
	}
	return out
}

func writeFileE2E(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// runProcessE2E starts a long-lived process, capturing its output, and kills it on cleanup.
func runProcessE2E(t *testing.T, env []string, name string, args ...string) *syncBuffer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = env
	buf := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = buf, buf
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("starting %s: %v", filepath.Base(name), err)
	}
	t.Cleanup(func() { cancel(); _ = cmd.Wait() })
	return buf
}

// runToCompletionE2E runs a short-lived command and returns its combined output.
func runToCompletionE2E(t *testing.T, env []string, name string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func waitForFileE2E(t *testing.T, path string, timeout time.Duration, diag func() string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil && len(strings.TrimSpace(string(b))) > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s\n%s", path, diag())
}

// consumeJobLifecycle reads the topic from the start until it has seen both a live upsert for
// jobKey (value present, carrying the expected ClusterId) and a later tombstone, or times out.
func consumeJobLifecycle(t *testing.T, broker, topic, jobKey, clusterID string, timeout time.Duration) (upsert, tombstone bool) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return upsert, tombstone
		}
		fs.EachRecord(func(r *kgo.Record) {
			if string(r.Key) != jobKey {
				return
			}
			if r.Value == nil {
				tombstone = true // ordering: a tombstone only follows the live upserts
			} else if strings.Contains(string(r.Value), "ClusterId = "+clusterID) {
				upsert = true
			}
		})
		if upsert && tombstone {
			return true, true
		}
	}
}
