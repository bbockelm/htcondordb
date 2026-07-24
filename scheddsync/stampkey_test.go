package scheddsync

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// TestStampKeyAddressable verifies each synced row carries its storage key as the "Key"
// attribute, so the REPL can address it for UPDATE/DELETE. Runs both the incremental and
// reconcile paths.
func TestStampKeyAddressable(t *testing.T) {
	for _, reconcile := range []bool{false, true} {
		dir := t.TempDir()
		logPath := filepath.Join(dir, "job_queue.log")
		writeFile(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n")
		target, err := db.Open("")
		if err != nil {
			t.Fatal(err)
		}
		s := NewJobSync(target, JobSyncConfig{Filename: logPath})
		if reconcile {
			err = s.reconcileReload(context.Background())
		} else {
			err = s.Poll(context.Background())
		}
		if err != nil {
			t.Fatalf("reconcile=%v: %v", reconcile, err)
		}
		ad, ok := target.LookupClassAd("1.0")
		if !ok {
			t.Fatalf("reconcile=%v: proc 1.0 missing", reconcile)
		}
		if v, _ := ad.EvaluateAttrString(KeyAttr); v != "1.0" {
			t.Errorf("reconcile=%v: %s = %q, want \"1.0\"", reconcile, KeyAttr, v)
		}
		target.Close()
	}
}
