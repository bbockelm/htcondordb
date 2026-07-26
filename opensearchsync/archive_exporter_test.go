package opensearchsync

import (
	"context"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

func mustAppend(t *testing.T, arch *db.ArchiveTable, adText string) {
	t.Helper()
	if err := arch.AppendOld(adText); err != nil {
		t.Fatal(err)
	}
}

// TestRunnerArchiveEndToEnd runs the full path against a history ARCHIVE (the primary
// condor_adstash use case): the exporter replays the retained completed-job records through the
// transform into the bulk uploader, then mirrors a live append -- exercising the archive watch
// added in classad v0.20.2.
func TestRunnerArchiveEndToEnd(t *testing.T) {
	c, cat := testServer(t)
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, arch, `GlobalJobId = "ap40#12.0#1700000000"; JobStatus = 4; JobUniverse = 5; CompletionDate = 1700000100; Owner = "alice"; ClusterId = 12; RequestCpus = 4`)
	mustAppend(t, arch, `GlobalJobId = "ap40#13.0#1700000000"; JobStatus = 4; JobUniverse = 5; CompletionDate = 1700000200; Owner = "bob"; ClusterId = 13`)

	cfg := newExporter(t, c, "hist-os", "history")
	rb := newRecordingBulk()
	r := NewRunner("hist-os", cfg, c, rb, testLaunch, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	idAlice := "ap40#12.0#1700000000#1700000100"
	waitFor(t, func() bool { return rb.count() == 2 })
	body, _, ok := rb.doc(idAlice)
	if !ok {
		t.Fatalf("alice history doc not indexed under %q; have %d docs", idAlice, rb.count())
	}
	for _, want := range []string{`"Owner":"alice"`, `"Status":"Completed"`, `"Universe":"Vanilla"`, `"RecordTime":1700000100`, `"ScheddName":"ap40"`} {
		if !strings.Contains(body, want) {
			t.Errorf("history doc missing %s:\n%s", want, body)
		}
	}

	// A newly-completed job appended to the archive flows through live.
	mustAppend(t, arch, `GlobalJobId = "ap40#14.0#1700000000"; JobStatus = 4; JobUniverse = 5; CompletionDate = 1700000300; Owner = "carol"; ClusterId = 14`)
	waitFor(t, func() bool { _, _, ok := rb.doc("ap40#14.0#1700000000#1700000300"); return ok })

	cancel()
	<-done
}
