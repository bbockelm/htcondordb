package server

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMemoryTablesNonPersistent checks the MemoryTables config: a listed table is
// created RAM-only in a persistent catalog -- no on-disk directory, and its data
// is gone after the service is reopened on the same dir, while the default table
// persists.
func TestMemoryTablesNonPersistent(t *testing.T) {
	dir := t.TempDir()

	svc, err := New(Config{
		Dir:          dir,
		Authorize:    allowAll,
		MemoryTables: []string{"ephemeral"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Write into both the default (persistent) table and the in-memory one.
	dtx := svc.DB().Begin()
	dtx.NewClassAd("d1", mustAd(t, `N = 1`))
	if err := dtx.Commit(); err != nil {
		t.Fatal(err)
	}
	mem, ok := svc.Catalog().Table("ephemeral")
	if !ok {
		t.Fatal("ephemeral table was not created")
	}
	mtx := mem.Begin()
	mtx.NewClassAd("m1", mustAd(t, `N = 2`))
	if err := mtx.Commit(); err != nil {
		t.Fatal(err)
	}

	// The in-memory table must not have an on-disk directory.
	memDir := filepath.Join(dir, "tables", "ephemeral")
	if _, err := os.Stat(memDir); !os.IsNotExist(err) {
		t.Fatalf("in-memory table created on-disk dir %s (err=%v)", memDir, err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen on the same dir: the default table's data survives; the in-memory
	// table reappears empty (it is re-created by the MemoryTables config).
	svc2, err := New(Config{
		Dir:          dir,
		Authorize:    allowAll,
		MemoryTables: []string{"ephemeral"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer svc2.Close()
	if got := svc2.DB().Len(); got != 1 {
		t.Fatalf("default table Len after reopen = %d, want 1 (should persist)", got)
	}
	mem2, ok := svc2.Catalog().Table("ephemeral")
	if !ok {
		t.Fatal("ephemeral table missing after reopen")
	}
	if got := mem2.Len(); got != 0 {
		t.Fatalf("in-memory table Len after reopen = %d, want 0 (should not persist)", got)
	}
	// Sanity: it still works as a table after reopen.
	if _, ok := mem2.LookupClassAd("m1"); ok {
		t.Fatal("in-memory table data survived restart (should be gone)")
	}
}
