// Package locate resolves where the htcondordb daemon is: used by the clients that connect
// to it, and by the daemon itself when it publishes its own address.
//
// Two knobs name the daemon, and either can be set in the environment or in the HTCondor
// configuration:
//
//	HTCONDORDB_ADDRESS_FILE   the file the daemon publishes its address to,
//	                          by default $(LOG)/.htcondordb_address
//	HTCONDORDB_HOST           a static host:port, for a daemon on another machine
//
// The environment wins over the configuration, and wins as a pair: if either variable is
// set, the configuration's copies of both are ignored. Overriding one alone would otherwise
// silently lose -- exporting HTCONDORDB_HOST to reach another pool would still resolve the
// local daemon, because a configured address file is preferred over a host.
//
// This is deliberately not the `_CONDOR_HTCONDORDB_HOST` convention, which also works
// (HTCondor's configuration reads it, and it lands in the configuration layer below). Plain
// names exist because a client with no command line -- a Python report calling connect() --
// has nowhere else to say "not that daemon, this one".
package locate

import (
	"fmt"
	"os"
	"strings"

	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
)

// Subsystem is htcondordb's HTCondor subsystem name, which prefixes both knobs.
const Subsystem = "HTCONDORDB"

// AddressFileKnob and HostKnob are the two knob names, exported so callers can name them in
// their own diagnostics without spelling them again.
const (
	AddressFileKnob = Subsystem + "_ADDRESS_FILE"
	HostKnob        = Subsystem + "_HOST"
)

const hint = "set " + AddressFileKnob + " or " + HostKnob +
	", in the environment or the HTCondor configuration"

// AddressFilePath returns where the daemon's address file lives: AddressFileKnob from the
// environment, else from the configuration, else $(LOG)/.htcondordb_address. It returns ""
// when none of those resolves, which for the daemon means "publish no address file".
//
// The daemon uses this as well as its clients, so an operator (or a test) that redirects the
// address file redirects both halves at once instead of leaving a client reading a path
// nothing writes.
func AddressFilePath(cfg *config.Config) string {
	if path := strings.TrimSpace(os.Getenv(AddressFileKnob)); path != "" {
		return path
	}
	return htcondor.AddressFilePath(cfg, Subsystem)
}

// Daemon returns the daemon's command address -- a sinful string or host:port, ready for
// cedar's ConnectAndAuthenticate.
//
// The address file is preferred over HostKnob when it is present and readable, so a
// co-located daemon is followed across restarts (under shared port its address carries a
// per-run socket token). HostKnob is the fallback, both when no file is configured and when
// the configured one cannot be read.
//
// Resolution happens per call. A client that reconnects should call again rather than cache,
// so it follows a restarted daemon.
func Daemon(cfg *config.Config) (string, error) {
	envFile := strings.TrimSpace(os.Getenv(AddressFileKnob))
	envHost := strings.TrimSpace(os.Getenv(HostKnob))
	if envFile != "" || envHost != "" {
		return fromEnvironment(envFile, envHost)
	}

	resolve, source, err := htcondor.LocalDaemonAddress(cfg, Subsystem)
	if err != nil {
		// Nothing names a daemon anywhere.
		return "", fmt.Errorf("cannot locate the htcondordb daemon: %s", hint)
	}
	addr, err := resolve()
	if err != nil {
		return "", fmt.Errorf("cannot locate the htcondordb daemon via %s: %w (%s)", source, err, hint)
	}
	return addr, nil
}

// fromEnvironment applies the same file-then-host precedence within the environment pair.
func fromEnvironment(envFile, envHost string) (string, error) {
	if envFile != "" {
		addr, err := htcondor.ReadAddressFile(envFile)
		if err == nil {
			return addr, nil
		}
		if envHost == "" {
			return "", fmt.Errorf("cannot locate the htcondordb daemon via %s from the environment: %w (%s)",
				AddressFileKnob, err, hint)
		}
		// Fall through to the host: a stale or absent file should not strand a caller that
		// also gave a host.
	}
	return envHost, nil
}
