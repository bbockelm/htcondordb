package repl

import (
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// sqlStringLiteral renders v as a single-quoted SQL literal, the way a client binding a
// parameter does: only ' is escaped, by doubling.
func sqlStringLiteral(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

// newPersistentExec is newTestExec over an ON-DISK store, which is what the daemon runs.
// The distinction matters: the two render raw text through different code, and have
// diverged twice (classad #134, #135). Read paths worth trusting are tested against both.
func newPersistentExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	d, err := db.Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServer(d)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	return NewExecutor(c, ExecConfig{}), func() { c.Close(); s.Close(); d.Close() }
}

// insertAndRead writes v as an INSERT value and reads it back.
func insertAndRead(t *testing.T, e *Executor, v string) string {
	t.Helper()
	sql := "INSERT INTO ads (Key, Owner) VALUES ('k', " + sqlStringLiteral(v) + ")"
	if _, err := e.ExecString(sql); err != nil {
		t.Fatalf("INSERT %q: %v", v, err)
	}
	r, err := e.ExecString("SELECT Owner FROM ads")
	if err != nil {
		t.Fatalf("SELECT after %q: %v", v, err)
	}
	return r.Rows[0][0]
}

// An INSERT value is written into old-ClassAd text, whose tokenizer does no escape
// processing, so it must be quoted for that format and not for the new-ClassAd rules an
// expression takes. Quoting it the other way turned one backslash into two on every pass.
func TestInsertPreservesBackslashesAndTabs(t *testing.T) {
	for _, v := range []string{
		`back\slash`,
		`a\\b`,
		"tab\there",
		`C:\Users\alice`,
		`regexp("^\d+$", Name)`,
		`say "hi"`,
		`O'Brien`,
		`plain`,
	} {
		e, cleanup := newTestExec(t)
		if got := insertAndRead(t, e, v); got != v {
			t.Errorf("round trip of %q gave %q", v, got)
		}
		cleanup()
	}
}

// Old-ClassAd format cannot represent two shapes, and the parser reports them rather than
// writing an ad that fails to parse on the way back: a newline (the format is
// newline-separated) and a trailing backslash (it would land before the closing quote and
// make the quote part of the value).
func TestInsertRefusesValuesOldFormatCannotHold(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	for _, v := range []string{"a\nb", "a\r\nb", `trailing\`} {
		sql := "INSERT INTO ads (Key, Owner) VALUES ('k', " + sqlStringLiteral(v) + ")"
		_, err := e.ExecString(sql)
		if err == nil {
			t.Errorf("INSERT of %q was accepted; old-ClassAd format cannot hold it", v)
			continue
		}
		if !strings.Contains(err.Error(), "old-ClassAd format") {
			t.Errorf("INSERT of %q failed with an unhelpful error: %v", v, err)
		}
	}
}

// The quote-escaping half of old-format quoting is unchanged and still round-trips: a
// backslash directly before the closing quote makes that quote part of the value, which is
// how an embedded quote is written.
func TestInsertPreservesEmbeddedQuotes(t *testing.T) {
	for _, v := range []string{`say "hi"`, `"leading`, `trailing"`, `""`} {
		e2, cleanup2 := newTestExec(t)
		if got := insertAndRead(t, e2, v); got != v {
			t.Errorf("round trip of %q gave %q", v, got)
		}
		cleanup2()
	}
}

// Both storage representations round-trip a backslash or tab identically.
//
// They did not: the raw text a read renders was quoted for new-ClassAd rules and read back
// with old-ClassAd ones, which do no unescaping, so every pass doubled (classad #134). The
// in-memory store escaped this only because a top-level string took a separate,
// correctly-quoting fast path. Kept as a matrix because the two representations render
// through different code and have now diverged twice.
func TestBackslashRoundTripMatchesAcrossStores(t *testing.T) {
	for _, v := range []string{`back\slash`, "tab\there"} {
		mem, cleanupMem := newTestExec(t)
		disk, cleanupDisk := newPersistentExec(t)
		gotMem := insertAndRead(t, mem, v)
		gotDisk := insertAndRead(t, disk, v)
		cleanupMem()
		cleanupDisk()
		if gotMem != gotDisk {
			t.Errorf("%q: in-memory store gave %q but persistent store gave %q", v, gotMem, gotDisk)
		}
	}
}
