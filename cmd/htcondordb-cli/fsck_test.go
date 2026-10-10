package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"

	"github.com/bbockelm/htcondordb/dbdir"
)

// buildDB writes a database-shaped directory: a couple of tables and an archive,
// each a real persistent collection, so fsck is exercised against the layout the
// daemon actually produces rather than a stand-in.
func buildDB(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	mk := func(sub, name string, n int) string {
		dir := filepath.Join(root, sub, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		c, err := collections.Open(collections.Options{Dir: dir, Shards: 1, SegmentSize: 1 << 14})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			ad := classad.New()
			ad.InsertAttrString("Owner", fmt.Sprintf("user%d", i%5))
			ad.InsertAttr("QDate", int64(1790000000+i))
			if err := c.Put([]byte(fmt.Sprintf("k%05d", i)), ad); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	mk(dbdir.Tables, "jobs", 300)
	mk(dbdir.Tables, "machines", 50)
	mk(dbdir.Archives, "history", 200)
	return root
}

// TestFsckCleanDatabase is the control: a healthy database reports no damage and
// exits zero.
func TestFsckCleanDatabase(t *testing.T) {
	root := buildDB(t)
	var out bytes.Buffer
	damaged, err := runFsck(&out, root, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + out.String())
	if damaged != 0 {
		t.Errorf("a healthy database reported %d damaged table(s)", damaged)
	}
	for _, want := range []string{"jobs", "machines", "archives/history", "no damage found"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("report does not mention %q", want)
		}
	}
}

// TestFsckFindsDamageAndNamesTheTable: an operator needs to know WHICH table is
// damaged, not just that something is.
func TestFsckFindsDamageAndNamesTheTable(t *testing.T) {
	root := buildDB(t)
	jobs := filepath.Join(root, dbdir.Tables, "jobs")
	seg := firstSegment(t, jobs)
	corruptAt(t, seg, 200)

	var out bytes.Buffer
	damaged, err := runFsck(&out, root, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + out.String())
	if damaged != 1 {
		t.Errorf("want 1 damaged table, got %d", damaged)
	}
	s := out.String()
	if !strings.Contains(s, "jobs: DAMAGED") {
		t.Error("the report does not name jobs as damaged")
	}
	if !strings.Contains(s, "machines: ok") {
		t.Error("an undamaged table was not reported ok")
	}
	if !strings.Contains(s, "nothing was modified") {
		t.Error("the report should say it changed nothing")
	}
}

// TestFsckTableFilter: -table narrows the run to one table.
func TestFsckTableFilter(t *testing.T) {
	root := buildDB(t)
	var out bytes.Buffer
	if _, err := runFsck(&out, root, "machines"); err != nil {
		t.Fatal(err)
	}
	s := out.String()
	if !strings.Contains(s, "machines") {
		t.Error("the named table was not inspected")
	}
	if strings.Contains(s, "jobs:") {
		t.Error("-table did not narrow the run")
	}
	if _, err := runFsck(&out, root, "nosuchtable"); err == nil {
		t.Error("an unknown table name should be an error, not an empty report")
	}
}

// TestFsckNeedsADirectory: with no directory configured, say so instead of
// reporting a clean bill of health for nothing.
func TestFsckNeedsADirectory(t *testing.T) {
	var out bytes.Buffer
	if _, err := runFsck(&out, "", ""); err == nil {
		t.Fatal("an empty directory should be an error")
	}
	if _, err := runFsck(&out, t.TempDir(), ""); err == nil {
		t.Fatal("a directory with no tables should be an error, not 'no damage found'")
	}
}

func firstSegment(t *testing.T, tableDir string) string {
	t.Helper()
	var found string
	err := filepath.Walk(tableDir, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() || found != "" {
			return err
		}
		if strings.HasSuffix(p, ".dat") {
			found = p
		}
		return nil
	})
	if err != nil || found == "" {
		t.Fatalf("no segment file under %s (err %v)", tableDir, err)
	}
	return found
}

func corruptAt(t *testing.T, path string, off int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], int64(off)); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0x01
	if _, err := f.WriteAt(b[:], int64(off)); err != nil {
		t.Fatal(err)
	}
}
