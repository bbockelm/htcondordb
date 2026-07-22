package kafkasync

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestCapstoneE2E is the end-to-end capstone: a real personal HTCondor pool (condor_master
// and friends, via the golang-htcondor harness -- local binaries, no Docker), a real
// SASL-authenticated Kafka broker (Redpanda), the real htcondordb daemon running its
// schedd-sync job-queue watcher, and the real kafkasync exporter. A job is submitted and run
// to completion; the test asserts Kafka saw the lifecycle -- the job appears (upsert,
// carrying its ClusterId) while live, then becomes a tombstone once it completes and leaves
// job_queue.log.
//
//	condor_submit -> schedd job_queue.log -> htcondordb (schedd-sync) "jobs" table
//	             -> kafkasync exporter --SASL/SCRAM--> Kafka topic -> this test's consumer
//
// It authenticates to Kafka over SASL/SCRAM with a password read from a file (referenced,
// never stored in the exporter's catalog config), and runs in BOTH deployment modes:
//   - Unprivileged: everything runs as the invoking user. (Validated locally.)
//   - As root (the model under condor_master): the harness runs the pool root->condor,
//     htcondordb's daemon.New drops privilege to condor and reads the condor-owned
//     job_queue.log as condor, and kafkasync runs as root. (Validated in CI, which runs the
//     pre-built test/daemon binaries under sudo.)
//
// Skipped under -short, when HTCondor is not installed (the harness skips), when neither rpk
// nor docker can provide a broker, or when the binaries cannot be built.
func TestCapstoneE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping capstone E2E in -short mode")
	}
	// When run as root (the production model under condor_master), the harness runs the pool
	// root->condor, htcondordb's daemon.New drops privilege to condor and reads the
	// condor-owned job_queue.log, and kafkasync runs as root. Otherwise everything runs as
	// the invoking user.
	asRoot := os.Geteuid() == 0

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

	// 2. Real SASL-authenticated Kafka broker (native rpk, else docker; skips if neither).
	broker, saslUser, saslPass := startSASLBrokerE2E(t)

	// 3. The real binaries -- resolved AFTER the harness/broker skip checks so a skipped run
	// never builds. Use pre-built paths from HTCONDORDB_BIN / KAFKASYNC_BIN when set (CI
	// builds them once as the normal user and reuses them for both the unprivileged and root
	// runs, so `go` never runs privileged), else build them here.
	_, thisFile, _, _ := runtime.Caller(0)
	kafkasyncModDir := filepath.Dir(thisFile) // .../htcondordb/kafkasync
	htcondordbModDir := filepath.Dir(kafkasyncModDir)
	binDir := t.TempDir()
	htcondordbBin := binaryE2E(t, "HTCONDORDB_BIN", htcondordbModDir, "./cmd/htcondordb", filepath.Join(binDir, "htcondordb"))
	kafkasyncBin := binaryE2E(t, "KAFKASYNC_BIN", kafkasyncModDir, "./cmd/kafkasync", filepath.Join(binDir, "kafkasync"))

	// 4. The SASL password on disk, 0600, owned by the invoking user (the exporter runs as
	// that user here). The credential is referenced by path, never stored in the exporter's
	// catalog config.
	credDir := t.TempDir()
	passFile := filepath.Join(credDir, "kafka.pass")
	writeFileE2E(t, passFile, saslPass+"\n")
	if err := os.Chmod(passFile, 0o600); err != nil {
		t.Fatal(err)
	}

	// 5. htcondordb config: FS auth mapping the exporter's identity to DAEMON, schedd-sync
	// mirroring the harness's job_queue.log into the "jobs" table. As root, kafkasync (the
	// dbrpc client) runs as root, so DAEMON is granted to root; htcondordb itself drops to
	// condor, so its state/log dirs must be condor-writable.
	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	principal := me.Username
	var dbDir string
	if asRoot {
		principal = "root"
		dbDir = shallowTempDirE2E(t, "cap-hcdb")
		chownUserE2E(t, dbDir, "condor") // htcondordb drops to condor and writes LOG/HTCONDORDB_DIR
	} else {
		dbDir = t.TempDir()
	}
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
`, principal, dbDir, addrFile, jobLog))
	dbEnv := append(os.Environ(), "CONDOR_CONFIG="+hcdbCfg)

	// 6. Start the htcondordb daemon (real process; drops privilege itself when root) and wait
	// for its address file.
	dbLog := runProcessE2E(t, dbEnv, htcondordbBin, "-listen", listen)
	waitForFileE2E(t, addrFile, 30*time.Second, func() string { return dbLog.String() })

	// 7. Register + run the kafkasync exporter over SASL/SCRAM. Unique topic per run.
	topic := fmt.Sprintf("htc.jobs.capstone.%d", os.Getpid())
	createArgs := []string{"create", "-name", "jobs", "-table", "jobs", "-brokers", broker, "-topic", topic,
		"-sasl-user", saslUser, "-sasl-mechanism", SASLScramSHA256, "-sasl-password-file", passFile}
	if out, err := runToCompletionE2E(t, dbEnv, kafkasyncBin, createArgs...); err != nil {
		t.Fatalf("kafkasync create: %v\n%s", err, out)
	}
	runProcessE2E(t, dbEnv, kafkasyncBin, "run", "-name", "jobs")

	// 8. Submit a short job and run it to completion. HTCondor refuses to create/run a
	// root-owned job, so as root we submit as the condor user (jobs are a user action, not
	// root's); the job's working dir must then be condor-writable.
	jobDir := t.TempDir()
	if asRoot {
		jobDir = shallowTempDirE2E(t, "cap-job")
		chownUserE2E(t, jobDir, submitterE2E(t)) // the job runs as the submitting user
	}
	submit := fmt.Sprintf("universe = vanilla\nexecutable = /bin/sleep\narguments = 3\n"+
		"output = j.out\nerror = j.err\nlog = j.log\ntransfer_executable = false\ninitialdir = %s\nqueue\n", jobDir)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	clusterID := submitJobE2E(t, ctx, asRoot, h.GetConfigFile(), loc.Name, schedd, submit)
	t.Logf("submitted cluster %s; job_queue.log -> htcondordb -> kafka topic %s", clusterID, topic)
	jobKey := clusterID + ".0"

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

	// 9. Verify Kafka saw the lifecycle over the SASL-authenticated connection.
	sawUpsert, sawTombstone := consumeJobLifecycle(t, broker, saslUser, saslPass, topic, jobKey, clusterID, 60*time.Second)
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

// binaryE2E returns the binary named by envVar if set (pre-built by the caller), else builds
// pkg. The env path lets a root CI run avoid invoking `go` as root.
func binaryE2E(t *testing.T, envVar, moduleDir, pkg, out string) string {
	t.Helper()
	if p := os.Getenv(envVar); p != "" {
		return p
	}
	return buildBinaryE2E(t, moduleDir, pkg, out)
}

func buildBinaryE2E(t *testing.T, moduleDir, pkg, out string) string {
	t.Helper()
	// -buildvcs=false: the CI checkout layout (repo in a subdirectory) can make the VCS
	// stamp fail the build; the binary needs no version stamp here.
	cmd := exec.Command("go", "build", "-buildvcs=false", "-o", out, pkg)
	cmd.Dir = moduleDir
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot build %s in %s: %v\n%s", pkg, moduleDir, err, b)
	}
	return out
}

// shallowTempDirE2E makes a shallow /tmp directory (not the deeply-nested go test temp) and
// removes it on cleanup -- short paths keep well under the Unix-socket path limit and are
// simpler to chown to a service account.
func shallowTempDirE2E(t *testing.T, prefix string) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// lookupUserIDsE2E resolves a username to its numeric uid/gid.
func lookupUserIDsE2E(t *testing.T, username string) (int, int) {
	t.Helper()
	u, err := user.Lookup(username)
	if err != nil {
		t.Fatalf("root run needs the %q user: %v", username, err)
	}
	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(u.Gid)
	return uid, gid
}

// chownUserE2E hands an (empty, freshly created) directory to username, so a process running
// as that user can create files there. Called before anything is written into path, so a
// single os.Chown suffices -- no shelling out, no tree walk.
func chownUserE2E(t *testing.T, path, username string) {
	t.Helper()
	uid, gid := lookupUserIDsE2E(t, username)
	if err := os.Chown(path, uid, gid); err != nil {
		t.Fatalf("chown %s to %s: %v", path, username, err)
	}
}

// submitterE2E is the normal (non-root, non-condor) user a root run submits jobs as -- the
// user who invoked sudo. HTCondor refuses to own a job by root or the condor daemon account.
func submitterE2E(t *testing.T) string {
	t.Helper()
	u := os.Getenv("SUDO_USER")
	if u == "" || u == "root" {
		t.Fatal("root run needs SUDO_USER set to a normal user to submit jobs as (run via sudo)")
	}
	return u
}

func writeFileE2E(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// root mode so the dropped-to daemon can read/write its own dirs. Best-effort via chown(1).

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

func runToCompletionE2E(t *testing.T, env []string, name string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// submitJobE2E submits a job and returns its cluster id. Unprivileged, it submits directly
// via the schedd client. As root, HTCondor rejects a root-owned job, so it submits as the
// condor user through condor_submit (the submit file lives in a condor-owned dir).
func submitJobE2E(t *testing.T, ctx context.Context, asRoot bool, condorConfig, scheddName string, schedd *htcondor.Schedd, submit string) string {
	t.Helper()
	if !asRoot {
		id, err := schedd.Submit(ctx, submit)
		if err != nil {
			t.Fatalf("submit: %v", err)
		}
		return id
	}
	submitter := submitterE2E(t)
	uid, gid := lookupUserIDsE2E(t, submitter)
	dir := shallowTempDirE2E(t, "cap-sub")
	chownUserE2E(t, dir, submitter)
	sf := filepath.Join(dir, "job.sub")
	writeFileE2E(t, sf, submit)
	if err := os.Chmod(sf, 0o644); err != nil { // readable by the submitting user
		t.Fatal(err)
	}
	// Fork condor_submit as the submitting user via a uid/gid credential -- the test is
	// already root (never shell out to sudo). It finds the local schedd via its address file
	// (the harness makes its dirs traversable in root mode). scheddName is retained for a
	// collector-based fallback if ever needed.
	_ = scheddName
	cmd := exec.Command("condor_submit", sf)
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+condorConfig)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("condor_submit as %s: %v\n%s", submitter, err, out)
	}
	id := parseClusterE2E(string(out))
	if id == "" {
		t.Fatalf("could not parse cluster id from condor_submit output:\n%s", out)
	}
	return id
}

// parseClusterE2E extracts N from condor_submit's "... submitted to cluster N.".
func parseClusterE2E(out string) string {
	i := strings.LastIndex(out, "cluster ")
	if i < 0 {
		return ""
	}
	rest := out[i+len("cluster "):]
	n := 0
	for n < len(rest) && rest[n] >= '0' && rest[n] <= '9' {
		n++
	}
	return rest[:n]
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

// consumeJobLifecycle reads the topic (over SASL) from the start until it has seen both a
// live upsert for jobKey (carrying the expected ClusterId) and a later tombstone, or times out.
func consumeJobLifecycle(t *testing.T, broker, saslUser, saslPass, topic, jobKey, clusterID string, timeout time.Duration) (upsert, tombstone bool) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), saslConsumerOpt(saslUser, saslPass),
		kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
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
				tombstone = true
			} else if strings.Contains(string(r.Value), "ClusterId = "+clusterID) {
				upsert = true
			}
		})
		if upsert && tombstone {
			return true, true
		}
	}
}
