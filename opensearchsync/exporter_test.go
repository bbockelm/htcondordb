package opensearchsync

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// recordingBulk is a fake BulkClient that parses each _bulk body and records the latest
// document (and its external version) per id — the OpenSearch-side effect a consumer sees.
type recordingBulk struct {
	mu       sync.Mutex
	docs     map[string][]byte
	versions map[string]uint64
}

func newRecordingBulk() *recordingBulk {
	return &recordingBulk{docs: map[string][]byte{}, versions: map[string]uint64{}}
}

func (r *recordingBulk) Bulk(_ context.Context, _ string, ndjson []byte) (BulkOutcome, error) {
	lines := bytes.Split(ndjson, []byte("\n"))
	n := 0
	for i := 0; i+1 < len(lines); i += 2 {
		if len(lines[i]) == 0 {
			continue
		}
		var a bulkAction
		if err := json.Unmarshal(lines[i], &a); err != nil {
			continue
		}
		r.mu.Lock()
		r.docs[a.Index.ID] = append([]byte(nil), lines[i+1]...)
		r.versions[a.Index.ID] = a.Index.Version
		r.mu.Unlock()
		n++
	}
	return BulkOutcome{Indexed: n}, nil
}

func (r *recordingBulk) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.docs)
}

func (r *recordingBulk) doc(id string) (string, uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.docs[id]
	return string(b), r.versions[id], ok
}

func testServer(t *testing.T) (*dbrpc.Client, *db.Catalog) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	t.Cleanup(func() { c.Close(); s.Close(); cat.Close() })
	return c, cat
}

func putJob(t *testing.T, d *db.DB, key, adText string) {
	t.Helper()
	ad, err := classad.ParseOld(adText)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put(key, ad); err != nil {
		t.Fatal(err)
	}
}

func newExporter(t *testing.T, c *dbrpc.Client, name, table string) Config {
	t.Helper()
	cfg := Config{Table: table, Addresses: []string{"unused:9200"}, BatchSize: 1, FlushInterval: Duration(20 * time.Millisecond)}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateExporter(context.Background(), db.ExporterDef{Name: name, Kind: Kind, Config: raw}); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// TestRunnerEndToEnd runs the full path: a fresh exporter replays a table's job ads through the
// adstash transform into the (fake) bulk uploader, checkpoints the acked cursor, and mirrors a
// later live upsert. It validates document ids, that the transform ran (typed fields present),
// distinct external versions, and durable checkpointing.
func TestRunnerEndToEnd(t *testing.T) {
	c, cat := testServer(t)
	jobs, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	putJob(t, jobs, "12.0", `GlobalJobId = "ap40#12.0#1700000000"; JobStatus = 4; JobUniverse = 5; CompletionDate = 1700000100; Owner = "alice"; RequestCpus = 4`)
	putJob(t, jobs, "13.0", `GlobalJobId = "ap40#13.0#1700000000"; JobStatus = 4; JobUniverse = 5; CompletionDate = 1700000200; Owner = "bob"; RequestCpus = 8`)

	cfg := newExporter(t, c, "jobs-os", "jobs")
	rb := newRecordingBulk()
	r := NewRunner("jobs-os", cfg, c, rb, testLaunch, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Initial replay: both jobs indexed under adstash doc ids.
	idAlice := "ap40#12.0#1700000000#1700000100"
	idBob := "ap40#13.0#1700000000#1700000200"
	waitFor(t, func() bool { return rb.count() == 2 })

	body, vAlice, ok := rb.doc(idAlice)
	if !ok {
		t.Fatalf("alice doc not indexed under %q; have %d docs", idAlice, rb.count())
	}
	// The transform ran: derived + typed fields are present.
	for _, want := range []string{`"Owner":"alice"`, `"Status":"Completed"`, `"Universe":"Vanilla"`, `"RecordTime":1700000100`, `"ScheddName":"ap40"`} {
		if !strings.Contains(body, want) {
			t.Errorf("alice doc missing %s:\n%s", want, body)
		}
	}
	_, vBob, _ := rb.doc(idBob)
	if vAlice == vBob {
		t.Errorf("external versions should differ per document (got %d for both)", vAlice)
	}

	// State was checkpointed with a resume cursor.
	waitFor(t, func() bool {
		blob, ok, _ := c.GetExporterState(ctx, "jobs-os")
		if !ok {
			return false
		}
		st, _ := decodeState(blob)
		return len(st.WireCursor) > 0 && st.ExportSeq >= 2
	})

	// Live upsert: a newly completed job flows through.
	putJob(t, jobs, "14.0", `GlobalJobId = "ap40#14.0#1700000000"; JobStatus = 4; JobUniverse = 5; CompletionDate = 1700000300; Owner = "carol"`)
	waitFor(t, func() bool { _, _, ok := rb.doc("ap40#14.0#1700000000#1700000300"); return ok })

	cancel()
	<-done
}
