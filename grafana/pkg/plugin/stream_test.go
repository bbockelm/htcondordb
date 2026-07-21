package plugin

import (
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"
)

func TestStreamPathRoundTrip(t *testing.T) {
	in := streamSpec{Table: "jobs", Columns: []string{"Owner", "JobStatus"}}
	out, err := decodeStreamPath(encodeStreamPath(in))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Table != in.Table || len(out.Columns) != 2 || out.Columns[0] != "Owner" {
		t.Errorf("round-trip mismatch: %+v -> %+v", in, out)
	}
}

func TestDecodeStreamPathErrors(t *testing.T) {
	for _, p := range []string{"", "!!!not-base64!!!", encodeStreamPath(streamSpec{})} {
		if _, err := decodeStreamPath(p); err == nil {
			t.Errorf("decodeStreamPath(%q) = nil error, want error", p)
		}
	}
}

func TestWatchKindString(t *testing.T) {
	cases := map[uint8]string{0: "upsert", 1: "delete", 2: "reset", 3: "synced", 4: "resync", 9: "unknown"}
	for k, want := range cases {
		if got := watchKindString(k); got != want {
			t.Errorf("watchKindString(%d) = %q, want %q", k, got, want)
		}
	}
}

func TestEventRow(t *testing.T) {
	spec := streamSpec{Table: "machines", Columns: []string{"Cpus", "Name"}}
	ev := dbrpc.WatchEvent{Kind: 0, Key: "slot1@h", AdText: "Name = \"slot1@h\"\nCpus = 8\n"}
	row := eventRow(spec, ev)
	if row.kind != "upsert" || row.key != "slot1@h" {
		t.Errorf("row kind/key = %q/%q, want upsert/slot1@h", row.kind, row.key)
	}
	if len(row.cols) != 2 {
		t.Fatalf("cols len = %d, want 2", len(row.cols))
	}
	if row.cols[0] != "8" {
		t.Errorf("Cpus rendered as %q, want 8", row.cols[0])
	}
}

func TestEventRowDelete(t *testing.T) {
	// A delete has no ad text; columns are present but empty.
	spec := streamSpec{Table: "jobs", Columns: []string{"Owner"}}
	row := eventRow(spec, dbrpc.WatchEvent{Kind: 1, Key: "1.0"})
	if row.kind != "delete" || row.key != "1.0" {
		t.Errorf("row = %+v, want delete/1.0", row)
	}
	if len(row.cols) != 1 || row.cols[0] != "" {
		t.Errorf("delete cols = %v, want [\"\"]", row.cols)
	}
}

func TestStreamFrameSchema(t *testing.T) {
	spec := streamSpec{Table: "jobs", Columns: []string{"Owner", "JobStatus"}}
	// Schema frame: time, key, kind + 2 columns = 5 fields, 0 rows.
	schema := streamFrame("", spec, nil)
	if len(schema.Fields) != 5 {
		t.Fatalf("schema fields = %d, want 5", len(schema.Fields))
	}
	if schema.Fields[0].Len() != 0 {
		t.Errorf("schema frame should have 0 rows, got %d", schema.Fields[0].Len())
	}
	// Data frame: same fields, 1 row.
	dataFrame := streamFrame("", spec, &streamRow{key: "k", kind: "upsert", cols: []string{"alice", "2"}})
	if len(dataFrame.Fields) != 5 || dataFrame.Fields[0].Len() != 1 {
		t.Errorf("data frame = %d fields / %d rows, want 5/1", len(dataFrame.Fields), dataFrame.Fields[0].Len())
	}
}
