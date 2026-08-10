package locate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bbockelm/golang-htcondor/config"
)

// testConfig writes body to a throwaway condor_config and returns a Config read from it, with
// both knobs cleared from the environment so a developer's own exports cannot change what a
// test resolves. Tests that want an override set it explicitly.
func testConfig(t *testing.T, body string) *config.Config {
	t.Helper()
	t.Setenv(AddressFileKnob, "")
	t.Setenv(HostKnob, "")
	path := filepath.Join(t.TempDir(), "condor_config")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONDOR_CONFIG", path)
	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "TOOL"})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// addressFile writes an address file and returns its path.
func addressFile(t *testing.T, addr string) string {
	t.Helper()
	// Trailing blank lines are normal in a real address file; the first non-empty line is
	// the address.
	path := filepath.Join(t.TempDir(), "address")
	if err := os.WriteFile(path, []byte(addr+"\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDaemonFromConfig(t *testing.T) {
	t.Run("address file knob", func(t *testing.T) {
		file := addressFile(t, "<127.0.0.1:12345?sock=htcondordb>")
		cfg := testConfig(t, AddressFileKnob+" = "+file+"\n")
		if got, err := Daemon(cfg); err != nil || got != "<127.0.0.1:12345?sock=htcondordb>" {
			t.Fatalf("Daemon = %q, %v", got, err)
		}
	})

	// The case that matters most: an operator who configured nothing but LOG still gets a
	// working client, because that is where the daemon publishes by default.
	t.Run("log directory default", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, ".htcondordb_address"), []byte("127.0.0.1:9999\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg := testConfig(t, "LOG = "+dir+"\n")
		if got, err := Daemon(cfg); err != nil || got != "127.0.0.1:9999" {
			t.Fatalf("Daemon = %q, %v", got, err)
		}
	})

	// A daemon on another host publishes no local address file.
	t.Run("host knob", func(t *testing.T) {
		cfg := testConfig(t, HostKnob+" = db.example.edu:9618\n")
		if got, err := Daemon(cfg); err != nil || got != "db.example.edu:9618" {
			t.Fatalf("Daemon = %q, %v", got, err)
		}
	})

	t.Run("host knob covers an unreadable file", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "nonexistent")
		cfg := testConfig(t, AddressFileKnob+" = "+missing+"\n"+HostKnob+" = db.example.edu:9618\n")
		if got, err := Daemon(cfg); err != nil || got != "db.example.edu:9618" {
			t.Fatalf("Daemon = %q, %v", got, err)
		}
	})
}

func TestDaemonFromEnvironment(t *testing.T) {
	t.Run("host", func(t *testing.T) {
		cfg := testConfig(t, "")
		t.Setenv(HostKnob, "  db.example.edu:9618  ") // whitespace is not part of an address
		if got, err := Daemon(cfg); err != nil || got != "db.example.edu:9618" {
			t.Fatalf("Daemon = %q, %v", got, err)
		}
	})

	t.Run("address file", func(t *testing.T) {
		cfg := testConfig(t, "")
		t.Setenv(AddressFileKnob, addressFile(t, "127.0.0.1:9999"))
		if got, err := Daemon(cfg); err != nil || got != "127.0.0.1:9999" {
			t.Fatalf("Daemon = %q, %v", got, err)
		}
	})

	t.Run("address file beats host", func(t *testing.T) {
		cfg := testConfig(t, "")
		t.Setenv(AddressFileKnob, addressFile(t, "127.0.0.1:9999"))
		t.Setenv(HostKnob, "db.example.edu:9618")
		if got, err := Daemon(cfg); err != nil || got != "127.0.0.1:9999" {
			t.Fatalf("Daemon = %q, %v", got, err)
		}
	})

	t.Run("host covers an unreadable file", func(t *testing.T) {
		cfg := testConfig(t, "")
		t.Setenv(AddressFileKnob, filepath.Join(t.TempDir(), "nonexistent"))
		t.Setenv(HostKnob, "db.example.edu:9618")
		if got, err := Daemon(cfg); err != nil || got != "db.example.edu:9618" {
			t.Fatalf("Daemon = %q, %v", got, err)
		}
	})
}

// The environment has to win, and win as a pair. Overriding one variable while the
// configuration still supplies the other is exactly where a half-applied override would
// quietly resolve the wrong daemon.
func TestEnvironmentBeatsConfig(t *testing.T) {
	t.Run("env host beats a configured address file", func(t *testing.T) {
		local := addressFile(t, "127.0.0.1:11111")
		cfg := testConfig(t, AddressFileKnob+" = "+local+"\n")
		t.Setenv(HostKnob, "db.example.edu:9618")
		got, err := Daemon(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if got != "db.example.edu:9618" {
			t.Errorf("Daemon = %q; the configured address file beat the environment", got)
		}
	})

	t.Run("env address file beats a configured host", func(t *testing.T) {
		cfg := testConfig(t, HostKnob+" = db.example.edu:9618\n")
		t.Setenv(AddressFileKnob, addressFile(t, "127.0.0.1:9999"))
		if got, err := Daemon(cfg); err != nil || got != "127.0.0.1:9999" {
			t.Fatalf("Daemon = %q, %v", got, err)
		}
	})

	// An unreadable file named by the environment must not fall back to the configuration:
	// silently connecting to the local daemon when told to use another is worse than failing.
	t.Run("env address file does not fall back to config", func(t *testing.T) {
		cfg := testConfig(t, HostKnob+" = db.example.edu:9618\n")
		missing := filepath.Join(t.TempDir(), "nonexistent")
		t.Setenv(AddressFileKnob, missing)
		_, err := Daemon(cfg)
		if err == nil {
			t.Fatal("Daemon succeeded; it fell back to the configuration")
		}
		if !strings.Contains(err.Error(), "environment") || !strings.Contains(err.Error(), missing) {
			t.Errorf("error %q should name the file and say it came from the environment", err)
		}
	})
}

func TestDaemonUnlocatable(t *testing.T) {
	// LOG deliberately empty: with no knob and no log directory nothing names a daemon.
	_, err := Daemon(testConfig(t, "LOG =\n"))
	if err == nil {
		t.Fatal("Daemon succeeded with nothing configured")
	}
	for _, want := range []string{AddressFileKnob, HostKnob, "environment", "configuration"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestAddressFilePath(t *testing.T) {
	dir := t.TempDir()

	t.Run("environment wins", func(t *testing.T) {
		cfg := testConfig(t, AddressFileKnob+" = "+filepath.Join(dir, "configured")+"\n")
		t.Setenv(AddressFileKnob, filepath.Join(dir, "from-env"))
		if got := AddressFilePath(cfg); got != filepath.Join(dir, "from-env") {
			t.Errorf("AddressFilePath = %q", got)
		}
	})

	t.Run("then the knob", func(t *testing.T) {
		cfg := testConfig(t, AddressFileKnob+" = "+filepath.Join(dir, "configured")+"\n")
		if got := AddressFilePath(cfg); got != filepath.Join(dir, "configured") {
			t.Errorf("AddressFilePath = %q", got)
		}
	})

	t.Run("then the log directory", func(t *testing.T) {
		cfg := testConfig(t, "LOG = "+dir+"\n")
		if got := AddressFilePath(cfg); got != filepath.Join(dir, ".htcondordb_address") {
			t.Errorf("AddressFilePath = %q", got)
		}
	})

	// "" means the daemon publishes no address file, so it must not become a stray relative
	// path when nothing is configured.
	t.Run("otherwise empty", func(t *testing.T) {
		if got := AddressFilePath(testConfig(t, "LOG =\n")); got != "" {
			t.Errorf("AddressFilePath = %q, want \"\"", got)
		}
	})
}
