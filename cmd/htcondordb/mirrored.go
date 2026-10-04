package main

import (
	"bufio"
	"os"
	"os/user"
	"strconv"
	"strings"

	"github.com/bbockelm/golang-htcondor/config"
)

// mirroredSchedd is what a spoke says about the schedd its schedd-sync mirrors: the schedd's Name,
// as the schedd itself computes it, and where to read the schedd's command address. It is
// advertised (MirroredScheddName / MirroredScheddAddress) so a consumer -- a federation hub, or an
// API server picking a mirror -- pairs mirror and schedd by name rather than by host.
//
// Every field is comparable: it rides in scheddSyncSettings, which reconciles by equality.
type mirroredSchedd struct {
	name string
	// rule says which configuration produced name, for the startup log line.
	rule string
	// addrFile is the schedd's address file. It is read on use, not here: the schedd rewrites it
	// with a new port every time it restarts.
	addrFile string
}

// resolveMirroredSchedd works out which schedd this spoke mirrors.
//
// HTCONDORDB_MIRRORED_SCHEDD_NAME wins outright -- the escape hatch for a spoke whose config does
// not carry the schedd's (a schedd started with -name, or configured under a local name). Without
// it the name follows the schedd's own rule (schedd.cpp: build_valid_daemon_name(SCHEDD_NAME), or
// default_daemon_name() when SCHEDD_NAME is unset), read from SCHEDD.SCHEDD_NAME / SCHEDD_NAME.
func resolveMirroredSchedd(cfg *config.Config) mirroredSchedd {
	addrFile := firstNonEmpty(getStr(cfg, "SCHEDD.SCHEDD_ADDRESS_FILE"), getStr(cfg, "SCHEDD_ADDRESS_FILE"))
	if v := strings.TrimSpace(getStr(cfg, "HTCONDORDB_MIRRORED_SCHEDD_NAME")); v != "" {
		return mirroredSchedd{name: v, rule: "HTCONDORDB_MIRRORED_SCHEDD_NAME", addrFile: addrFile}
	}
	configured, knob := strings.TrimSpace(getStr(cfg, "SCHEDD.SCHEDD_NAME")), "SCHEDD.SCHEDD_NAME"
	if configured == "" {
		configured, knob = strings.TrimSpace(getStr(cfg, "SCHEDD_NAME")), "SCHEDD_NAME"
	}
	name, rule := scheddDaemonName(configured, strings.TrimSpace(getStr(cfg, "FULL_HOSTNAME")),
		strings.TrimSpace(getStr(cfg, "HOSTNAME")), runsAsCondor(cfg), currentUsername())
	if configured != "" {
		rule = knob + " (" + rule + ")"
	}
	return mirroredSchedd{name: name, rule: rule, addrFile: addrFile}
}

// scheddDaemonName applies HTCondor's daemon-name rule (get_daemon_name.cpp):
//
//   - a configured name containing '@' is used as is;
//   - a configured name that is this host's own name becomes the full hostname;
//   - any other configured name becomes name@FULL_HOSTNAME;
//   - with no configured name, a daemon running as root or as the condor user is named by the
//     full hostname, any other user's as user@FULL_HOSTNAME (a personal condor).
//
// Deviation: C++ decides "this host's own name" by resolving the configured name through DNS and
// comparing it with the local FQDN. This compares against FULL_HOSTNAME and HOSTNAME instead, so
// it never blocks on a resolver; a configured name that is a DNS alias of this host, and neither
// of those spellings, gets the '@' form here where the schedd would not. Set
// HTCONDORDB_MIRRORED_SCHEDD_NAME for that case.
func scheddDaemonName(configured, fullHost, shortHost string, asCondor bool, username string) (name, rule string) {
	if configured != "" {
		if strings.Contains(configured, "@") {
			return configured, "contains '@', used as is"
		}
		if strings.EqualFold(configured, fullHost) || (shortHost != "" && strings.EqualFold(configured, shortHost)) {
			return fullHost, "names this host"
		}
		return configured + "@" + fullHost, "name@FULL_HOSTNAME"
	}
	if asCondor || username == "" {
		return fullHost, "default: FULL_HOSTNAME"
	}
	return username + "@" + fullHost, "default for a non-condor user: user@FULL_HOSTNAME"
}

// runsAsCondor reports whether this process runs as root or as the condor user -- the case in which
// the schedd's default name is the bare hostname. Under condor_master the schedd runs as root and
// this daemon drops to the condor user; in a personal condor both run as the same ordinary user.
// Either way the answer here matches the schedd's.
func runsAsCondor(cfg *config.Config) bool {
	uid := os.Getuid()
	if uid == 0 {
		return true
	}
	if ids := strings.TrimSpace(getStr(cfg, "CONDOR_IDS")); ids != "" {
		if u, _, ok := strings.Cut(ids, "."); ok {
			if n, err := strconv.Atoi(strings.TrimSpace(u)); err == nil {
				return n == uid
			}
		}
	}
	if u, err := user.Lookup("condor"); err == nil {
		return u.Uid == strconv.Itoa(uid)
	}
	return false
}

func currentUsername() string {
	if u, err := user.Current(); err == nil {
		return u.Username
	}
	return ""
}

// address returns the schedd's command address: the first line of its address file, or "" when the
// file cannot be read (the schedd is down, or this daemon may not read it).
func (m mirroredSchedd) address() string {
	if m.addrFile == "" {
		return ""
	}
	f, err := os.Open(m.addrFile)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	if sc.Scan() {
		return strings.TrimSpace(sc.Text())
	}
	return ""
}
