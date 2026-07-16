package main

import "testing"

// TestScheddSyncGuardEUID is the "privileged" half of the schedd-sync security model:
// running as root must be refused (schedd files must never be read privileged), while any
// non-root effective uid is allowed. The full run-as-root drop path is covered by
// TestDaemonDropsPrivilege; here we assert the guard itself at both privilege levels
// without needing to actually be root.
func TestScheddSyncGuardEUID(t *testing.T) {
	if err := scheddSyncGuardEUID(0); err == nil {
		t.Error("schedd-sync must be refused when running as root (euid 0)")
	}
	for _, euid := range []int{1, 500, 1000} {
		if err := scheddSyncGuardEUID(euid); err != nil {
			t.Errorf("schedd-sync should be allowed at euid %d, got: %v", euid, err)
		}
	}
}
