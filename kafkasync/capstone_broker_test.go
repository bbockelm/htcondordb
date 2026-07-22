package kafkasync

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// saslCapstoneUser/Pass are the SCRAM credentials the capstone provisions on the broker.
const (
	saslCapstoneUser = "capstone"
	saslCapstonePass = "s3cret-capstone"
)

// saslConsumerOpt is the kgo SASL option for the capstone's verification consumer.
func saslConsumerOpt(user, pass string) kgo.Opt {
	return kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha256Mechanism())
}

// startSASLBrokerE2E launches a Redpanda broker whose Kafka listener requires SASL
// (authentication_method=sasl), provisions a SCRAM-SHA-256 superuser, and returns the broker
// address plus its username/password. It prefers a native rpk broker and falls back to a
// docker container -- mirroring startBroker, but with an explicit SASL listener (the
// dev-container default listener does no auth) and a user bootstrapped over the admin API.
func startSASLBrokerE2E(t *testing.T) (broker, user, pass string) {
	t.Helper()
	if _, err := exec.LookPath("rpk"); err == nil {
		return startSASLNative(t)
	}
	if _, err := exec.LookPath("docker"); err == nil {
		if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
			t.Skipf("docker present but not usable: %v\n%s", err, out)
		}
		return startSASLDocker(t)
	}
	t.Skip("neither rpk (native redpanda) nor docker is available for a SASL broker")
	return "", "", ""
}

// saslStartArgs are the `... start ...` flags for a SASL-listener redpanda, shared by the
// native and docker launchers. kafkaPort is bound (container: inside; native: on localhost)
// and advertised at advertiseHost:kafkaPort.
func saslStartArgs(bindHost, advertiseHost string, kafkaPort, rpcPort, adminPort int, extra ...string) []string {
	args := []string{
		"start", "--smp", "1", "--overprovisioned", "--node-id", "0",
		"--set", "redpanda.enable_sasl=true",
		"--set", fmt.Sprintf(`redpanda.superusers=[%q]`, saslCapstoneUser),
		"--set", fmt.Sprintf(`redpanda.kafka_api=[{"address":%q,"port":%d,"name":"ext","authentication_method":"sasl"}]`, bindHost, kafkaPort),
		"--set", fmt.Sprintf(`redpanda.advertised_kafka_api=[{"address":%q,"port":%d,"name":"ext"}]`, advertiseHost, kafkaPort),
		"--set", "redpanda.empty_seed_starts_cluster=true",
		"--rpc-addr", fmt.Sprintf("127.0.0.1:%d", rpcPort),
		"--set", fmt.Sprintf(`redpanda.admin=[{"address":"127.0.0.1","port":%d}]`, adminPort),
	}
	return append(args, extra...)
}

func startSASLNative(t *testing.T) (string, string, string) {
	t.Helper()
	kport, rport, aport := freePort(t), freePort(t), freePort(t)
	dir := t.TempDir()
	broker := fmt.Sprintf("127.0.0.1:%d", kport)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "rpk", append([]string{"redpanda"},
		saslStartArgs("127.0.0.1", "127.0.0.1", kport, rport, aport,
			"--set", "redpanda.data_directory="+dir+"/data", "--config", dir+"/redpanda.yaml")...)...)
	logBuf := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = logBuf, logBuf
	if err := cmd.Start(); err != nil {
		cancel()
		t.Skipf("could not start native redpanda: %v", err)
	}
	t.Cleanup(func() { cancel(); _ = cmd.Wait() })
	adminURL := fmt.Sprintf("127.0.0.1:%d", aport)
	waitAdminHealthE2E(t, adminURL, func() string { return logBuf.String() },
		"rpk", "cluster", "health", "--api-urls", adminURL)
	mustRunE2E(t, "rpk", "security", "user", "create", saslCapstoneUser, "-p", saslCapstonePass,
		"--mechanism", "SCRAM-SHA-256", "--api-urls", adminURL)
	waitSASLReadyE2E(t, broker)
	return broker, saslCapstoneUser, saslCapstonePass
}

func startSASLDocker(t *testing.T) (string, string, string) {
	t.Helper()
	port := freePort(t)
	broker := fmt.Sprintf("localhost:%d", port)
	name := fmt.Sprintf("kafkasync-sasl-%d", port)
	_ = exec.Command("docker", "rm", "-f", name).Run()
	args := append([]string{
		"run", "-d", "--rm", "--name", name, "-p", fmt.Sprintf("127.0.0.1:%d:%d", port, port),
		redpandaImage, "redpanda",
	}, saslStartArgs("0.0.0.0", "localhost", port, 33145, 9644)...)
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Skipf("could not start SASL redpanda container: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	waitAdminHealthE2E(t, name, func() string { return dockerLogsE2E(name) },
		"docker", "exec", name, "rpk", "cluster", "health")
	mustRunE2E(t, "docker", "exec", name, "rpk", "security", "user", "create", saslCapstoneUser,
		"-p", saslCapstonePass, "--mechanism", "SCRAM-SHA-256")
	waitSASLReadyE2E(t, broker)
	return broker, saslCapstoneUser, saslCapstonePass
}

// waitAdminHealthE2E polls the admin health command until it succeeds (broker up).
func waitAdminHealthE2E(t *testing.T, who string, diag func() string, healthCmd ...string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if exec.Command(healthCmd[0], healthCmd[1:]...).Run() == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("redpanda (%s) admin never became healthy\n%s", who, diag())
}

// waitSASLReadyE2E confirms the Kafka listener authenticates the provisioned user.
func waitSASLReadyE2E(t *testing.T, broker string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		cl, err := kgo.NewClient(kgo.SeedBrokers(broker), saslConsumerOpt(saslCapstoneUser, saslCapstonePass))
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
	t.Fatalf("SASL broker at %s never authenticated %q", broker, saslCapstoneUser)
}

func mustRunE2E(t *testing.T, name string, args ...string) {
	t.Helper()
	if out, err := exec.Command(name, args...).CombinedOutput(); err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, out)
	}
}

func dockerLogsE2E(name string) string {
	out, _ := exec.Command("docker", "logs", name).CombinedOutput()
	return string(out)
}
