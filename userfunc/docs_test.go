package userfunc

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// docs/user-functions.md carries four worked script functions. This test runs them out of the page
// itself rather than a copy: the script, the configuration, the sample rows and the stated answers
// are all read from the Markdown, so an example that stops being true breaks a test.
//
// The four names are used by this test alone, as everywhere else in this package, because classad's
// registry is process-wide and has no unregister.

var (
	docSection  = regexp.MustCompile(`(?s)\n## Worked examples\n(.*?)\n## Writing a module\n`)
	docFence    = regexp.MustCompile("(?s)```([a-z]*)\n(.*?)```")
	docPathLine = regexp.MustCompile(`(?m)^(HTCONDORDB_SCRIPT_[A-Z]+_PATH\s*=\s*).*$`)
	docAdTable  = regexp.MustCompile(`^#\s*FROM\s+(\w+)\s*$`)
	docExpTable = regexp.MustCompile(`^#\s*(\w+)\s*$`)
	docExpLine  = regexp.MustCompile(`^(.*?\S)\s{2,}(\S+)$`)
	docDef      = regexp.MustCompile(`(?m)^def `)
)

// docBlocks pulls the worked-examples section apart into the pieces the test needs: every script
// block (a plain fence holding a `def`), the configuration, a sample ad per table, and a list of
// expression/answer pairs with the table each is evaluated against.
type docBlocks struct {
	script string
	config string
	ads    map[string]string
	checks []docCheck
}

type docCheck struct {
	table, src, want string
}

func readDocBlocks(t *testing.T) docBlocks {
	t.Helper()
	page, err := os.ReadFile(filepath.Join("..", "docs", "user-functions.md"))
	if err != nil {
		t.Fatal(err)
	}
	sec := docSection.FindSubmatch(page)
	if sec == nil {
		t.Fatal("docs/user-functions.md no longer holds a Worked examples section")
	}

	out := docBlocks{ads: map[string]string{}}
	for _, m := range docFence.FindAllStringSubmatch(string(sec[1]), -1) {
		lang, body := m[1], m[2]
		switch {
		case lang == "" && docDef.MatchString(body):
			// One file backs every example, exactly as the configuration says, so the blocks
			// concatenate in the order the page shows them.
			out.script += body + "\n"
		case lang == "conf":
			if out.config != "" {
				t.Fatal("the section holds more than one configuration block")
			}
			out.config = body
		case lang == "classad":
			head, rest, ok := strings.Cut(body, "\n")
			tbl := docAdTable.FindStringSubmatch(strings.TrimSpace(head))
			if !ok || tbl == nil {
				t.Fatalf("a sample-row block does not open with `# FROM <table>`:\n%s", body)
			}
			out.ads[tbl[1]] = rest
		case lang == "text":
			head, rest, ok := strings.Cut(body, "\n")
			tbl := docExpTable.FindStringSubmatch(strings.TrimSpace(head))
			if !ok || tbl == nil {
				t.Fatalf("a results block does not open with `# <table>`:\n%s", body)
			}
			for _, line := range strings.Split(rest, "\n") {
				if strings.TrimSpace(line) == "" {
					continue
				}
				pair := docExpLine.FindStringSubmatch(line)
				if pair == nil {
					t.Fatalf("a results line is not `<expression>  <answer>`: %q", line)
				}
				out.checks = append(out.checks, docCheck{table: tbl[1], src: pair[1], want: pair[2]})
			}
		}
	}

	// Each piece earns its place, so an example silently dropped from the page fails here rather
	// than shrinking the test to nothing.
	if n := len(docDef.FindAllString(out.script, -1)); n < 4 {
		t.Fatalf("the section holds %d definitions, fewer than the four functions configured", n)
	}
	if out.config == "" {
		t.Fatal("the section holds no configuration block")
	}
	if len(out.ads) == 0 || len(out.checks) == 0 {
		t.Fatalf("the section holds %d sample rows and %d results", len(out.ads), len(out.checks))
	}
	return out
}

func TestUserFunctionsDocExamples(t *testing.T) {
	doc := readDocBlocks(t)

	path := filepath.Join(t.TempDir(), "policy.cas")
	if err := os.WriteFile(path, []byte(doc.script), 0o600); err != nil {
		t.Fatal(err)
	}
	// Only the paths move: the page names a file this test does not have, and nothing else about
	// the configuration is this test's to rewrite.
	body := docPathLine.ReplaceAllString(doc.config, "${1}"+path)
	if n := strings.Count(body, path); n != 4 {
		t.Fatalf("expected four rewritten _PATH lines, got %d:\n%s", n, body)
	}

	m := newWasmMgr(t)
	if err := m.Apply(wasmCfg(t, body)); err != nil {
		t.Fatalf("apply the page's configuration: %v", err)
	}

	ads := map[string]*classad.ClassAd{}
	for tbl, src := range doc.ads {
		ad, err := classad.Parse(src)
		if err != nil {
			t.Fatalf("parse the sample %s row: %v", tbl, err)
		}
		ads[tbl] = ad
	}

	for _, c := range doc.checks {
		ad, ok := ads[c.table]
		if !ok {
			t.Errorf("%s: no sample row for the %q table", c.src, c.table)
			continue
		}
		expr, err := classad.ParseExpr(c.src)
		if err != nil {
			t.Errorf("parse %s: %v", c.src, err)
			continue
		}
		if got := expr.Eval(ad).String(); got != c.want {
			t.Errorf("%s = %s, the page says %s", c.src, got, c.want)
		}
	}
}
