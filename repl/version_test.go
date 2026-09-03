package repl

import (
	"bytes"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
)

// TestVersionReportsTheStack: the point of .version is that a daemon's
// own release number does not identify the code in it. A build is
// assembled from independently versioned modules -- the ClassAd engine,
// the CEDAR wire library, the HTCondor protocol implementation -- and a
// bug in any of them is invisible in "htcondordb v0.18.6".
func TestVersionReportsTheStack(t *testing.T) {
	saved := BuildVersion
	defer func() { BuildVersion = saved }()
	BuildVersion = "v9.9.9-test"

	var out bytes.Buffer
	runVersion(&out)
	got := out.String()

	if !strings.Contains(got, "v9.9.9-test") {
		t.Errorf("the linked release is missing:\n%s", got)
	}
	// Always knowable, whatever the build.
	if !strings.Contains(got, runtime.Version()) {
		t.Errorf("Go toolchain missing:\n%s", got)
	}
	if !strings.Contains(got, runtime.GOOS+"/"+runtime.GOARCH) {
		t.Errorf("platform missing:\n%s", got)
	}
	// A test binary is a module build, so the module line is present
	// even though its version is the "(devel)" placeholder.
	if !strings.Contains(got, "github.com/bbockelm/htcondordb") {
		t.Errorf("main module missing:\n%s", got)
	}
	t.Logf("\n%s", got)
}

// TestVersionReportsRealComponents uses a synthetic BuildInfo, because a
// `go test` binary records no dependency modules at all -- just
// "mod <main> (devel)" -- so the component lines that justify this
// command cannot be exercised from inside a test any other way. The
// values below are what the real htcondordb-cli binary actually
// carries.
func TestVersionReportsRealComponents(t *testing.T) {
	saved := BuildVersion
	defer func() { BuildVersion = saved }()
	BuildVersion = "v0.18.6"

	var out bytes.Buffer
	writeVersion(&out, &debug.BuildInfo{
		Main: debug.Module{Path: "github.com/bbockelm/htcondordb", Version: "v0.18.6"},
		Deps: []*debug.Module{
			{Path: "github.com/PelicanPlatform/classad", Version: "v0.29.7"},
			{Path: "github.com/bbockelm/cedar", Version: "v0.6.11"},
			{Path: "github.com/bbockelm/golang-htcondor", Version: "v0.12.10"},
			{Path: "github.com/some/unrelated", Version: "v1.0.0"},
		},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "3954b36e31f7c1c2a017d9ff32171bb0666bf683"},
			{Key: "vcs.modified", Value: "true"},
		},
	}, true)
	got := out.String()

	for _, want := range []string{
		"classad         v0.29.7",
		"cedar           v0.6.11",
		"golang-htcondor v0.12.10",
		"3954b36e31f7 (dirty)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report omits %q:\n%s", want, got)
		}
	}
	// Everything else in the dependency graph is noise at this prompt.
	if strings.Contains(got, "unrelated") {
		t.Errorf("report names a module nobody asked about:\n%s", got)
	}
	t.Logf("\n%s", got)
}

// TestVersionShowsAReplacedComponent: a developer building against a
// local checkout has a replace directive, and the replacement's version
// is "(devel)", which as a version number is a lie. Say where it came
// from instead.
func TestVersionShowsAReplacedComponent(t *testing.T) {
	var out bytes.Buffer
	writeVersion(&out, &debug.BuildInfo{
		Main: debug.Module{Path: "github.com/bbockelm/htcondordb", Version: "(devel)"},
		Deps: []*debug.Module{{
			Path:    "github.com/bbockelm/golang-htcondor",
			Version: "v0.12.10",
			Replace: &debug.Module{Path: "/home/me/golang-htcondor", Version: "(devel)"},
		}},
	}, true)
	got := out.String()

	if !strings.Contains(got, "replaced by /home/me/golang-htcondor") {
		t.Errorf("a replaced module is reported as if it were a release:\n%s", got)
	}
	// The main module's placeholder must not be printed as a version.
	if strings.Contains(got, "module        github.com/bbockelm/htcondordb (devel)") {
		t.Errorf("placeholder printed as a version:\n%s", got)
	}
}

// TestVersionWithoutLinkedRelease: an unlinked build must still say
// something. "htcondordb" with a blank after it reads like the output
// is broken.
func TestVersionWithoutLinkedRelease(t *testing.T) {
	saved := BuildVersion
	defer func() { BuildVersion = saved }()
	BuildVersion = ""

	var out bytes.Buffer
	runVersion(&out)
	if !strings.Contains(out.String(), "htcondordb dev") {
		t.Errorf("no fallback for an unlinked build:\n%s", out.String())
	}
}

// TestVersionIsReachableFromTheShell guards the wiring, not the text:
// an unrecognized meta-command silently falls through to "unknown
// command", so a dispatch typo would leave the command undiscoverable
// while every unit test above still passed.
func TestVersionIsReachableFromTheShell(t *testing.T) {
	var out bytes.Buffer
	s := &session{base: &out, out: &out, format: FormatTable}

	if quit := s.runMeta(&out, ".version"); quit {
		t.Error(".version asked the shell to quit")
	}
	if strings.Contains(out.String(), "unknown command") {
		t.Errorf(".version did not reach a handler:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "htcondordb") {
		t.Errorf(".version produced no report:\n%s", out.String())
	}

	// And it is advertised, or nobody finds it.
	if !strings.Contains(helpText, ".version") {
		t.Error(".version is missing from .help")
	}
}
