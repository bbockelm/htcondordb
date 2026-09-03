package repl

import (
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"strings"
)

// The .version command answers "what is this, exactly?" from inside the
// shell -- the question anyone asks first when a pool behaves oddly and
// they are not sure which build they are talking to.
//
// It reads the build information the Go toolchain embeds rather than a
// version injected at link time. Those are not the same answer: the
// Makefile injects -X main.version, which names this daemon's release
// but says nothing about the ClassAd engine, the CEDAR wire library or
// the HTCondor protocol implementation it is built from -- and a bug in
// any of those is invisible in the daemon's own version. Build info
// carries all of them, plus the commit and whether the tree was dirty,
// with no cooperation from the build system.

// BuildVersion is the release string the binary was linked with
// (-ldflags "-X main.version=..."). Set by main; empty is fine.
//
// Kept as a package variable rather than a Run parameter because two
// different binaries embed this shell, and threading a string neither
// of them has at Run time through the signature would be noise. Build
// info supplies everything else.
var BuildVersion string

// versionComponents are the modules worth naming, in report order.
// Anything else in the dependency graph is noise at this prompt.
var versionComponents = []struct {
	label string
	path  string
}{
	{"golang-htcondor", "github.com/bbockelm/golang-htcondor"},
	{"classad", "github.com/PelicanPlatform/classad"},
	{"cedar", "github.com/bbockelm/cedar"},
}

// runVersion prints the running binary's identity and the versions of
// the components it is built from.
func runVersion(console io.Writer) {
	bi, ok := debug.ReadBuildInfo()
	writeVersion(console, bi, ok)
}

// writeVersion renders the report. Split from runVersion so it can be
// tested against a realistic build: a `go test` binary records no
// dependency modules at all -- only "mod <main> (devel)" -- so it
// cannot exercise the component lines that are the whole point.
func writeVersion(console io.Writer, bi *debug.BuildInfo, ok bool) {
	fmt.Fprintf(console, "htcondordb %s\n", firstNonEmpty(BuildVersion, "dev"))

	if !ok || bi == nil {
		// A binary built outside module mode carries none of this.
		// Saying so is better than printing an empty report that reads
		// like the components are missing.
		fmt.Fprintf(console, "  %-15s %s\n", "go", runtime.Version())
		fmt.Fprintln(console, "  (no embedded build info: built outside module mode)")
		return
	}

	if v := moduleVersion(bi); v != "" {
		fmt.Fprintf(console, "  %-15s %s %s\n", "module", bi.Main.Path, v)
	} else {
		fmt.Fprintf(console, "  %-15s %s\n", "module", bi.Main.Path)
	}

	var revision, modified string
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if revision != "" {
		short := revision
		if len(short) > 12 {
			short = short[:12]
		}
		if modified == "true" {
			short += " (dirty)"
		}
		fmt.Fprintf(console, "  %-15s %s\n", "revision", short)
	}

	deps := make(map[string]string, len(bi.Deps))
	for _, d := range bi.Deps {
		v := d.Version
		// A replaced module reports the replacement's version, which for
		// a local directory is "(devel)". Show where it came from
		// instead of a version that means nothing.
		if d.Replace != nil {
			v = d.Replace.Version
			if v == "" || v == "(devel)" {
				v = "(replaced by " + d.Replace.Path + ")"
			}
		}
		deps[d.Path] = v
	}
	for _, c := range versionComponents {
		if v, ok := deps[c.path]; ok {
			fmt.Fprintf(console, "  %-15s %s\n", c.label, v)
		}
	}
	fmt.Fprintf(console, "  %-15s %s\n", "go", runtime.Version())
	fmt.Fprintf(console, "  %-15s %s/%s\n", "platform", runtime.GOOS, runtime.GOARCH)
}

// moduleVersion returns the main module's version, or "" when the
// toolchain has nothing better than its placeholder for a local build.
func moduleVersion(bi *debug.BuildInfo) string {
	if bi.Main.Version == "" || bi.Main.Version == "(devel)" {
		return ""
	}
	return bi.Main.Version
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
