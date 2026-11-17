package dropboxframe

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// jobRecord builds a record with the shape and redundancy of a real job ClassAd, so compression
// numbers in these tests mean something.
func jobRecord(i int) []byte {
	return []byte(fmt.Sprintf(
		"GlobalJobId = \"ap2001.chtc.wisc.edu#%d.0#1763000000\"\nOwner = \"someuser\"\n"+
			"ClusterId = %d\nProcId = 0\nJobStatus = 4\nRequestCpus = 1\nRequestMemory = 2048\n"+
			"Cmd = \"/home/someuser/run.sh\"\nIwd = \"/home/someuser/work\"\nQDate = 1763000000\n"+
			"CompletionDate = 1763003600\nRemoteWallClockTime = 3600.0\n", i, i))
}

func writeStream(t *testing.T, n, minFrame int) ([]byte, int) {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, minFrame)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := w.Write(jobRecord(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	frames, _, _ := w.Stats()
	return buf.Bytes(), frames
}

func readAll(t *testing.T, b []byte) ([][]byte, ReadResult) {
	t.Helper()
	var got [][]byte
	res, err := Read(b, func(r []byte) error {
		cp := make([]byte, len(r))
		copy(cp, r)
		got = append(got, cp)
		return nil
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return got, res
}

func TestRoundTrip(t *testing.T) {
	stream, frames := writeStream(t, 500, MinFrameBytesFloor)
	if frames < 2 {
		t.Fatalf("500 records made %d frames; the test needs several", frames)
	}
	got, res := readAll(t, stream)
	if len(got) != 500 || res.Records != 500 {
		t.Fatalf("recovered %d records (result says %d), want 500", len(got), res.Records)
	}
	for i, r := range got {
		if !bytes.Equal(r, jobRecord(i)) {
			t.Fatalf("record %d came back changed", i)
		}
	}
	if res.Damaged != 0 {
		t.Errorf("an undamaged stream reported %d damaged frames", res.Damaged)
	}
}

// TestFrameSizeBoundsRecordsPerFrame: the knob has to actually bound how much a frame holds,
// because that is what a damaged frame costs.
func TestFrameSizeBoundsRecordsPerFrame(t *testing.T) {
	small, smallFrames := writeStream(t, 2000, MinFrameBytesFloor) // 4 KiB
	large, largeFrames := writeStream(t, 2000, 256<<10)            // 256 KiB
	if smallFrames <= largeFrames {
		t.Fatalf("4KiB frames=%d, 256KiB frames=%d -- a smaller frame size must make more frames", smallFrames, largeFrames)
	}
	// And the bigger frames must compress better, which is the whole reason not to frame per record.
	if len(large) >= len(small) {
		t.Errorf("256KiB framing produced %d bytes, 4KiB framing %d -- larger frames should compress better", len(large), len(small))
	}
	for _, s := range [][]byte{small, large} {
		if got, _ := readAll(t, s); len(got) != 2000 {
			t.Errorf("recovered %d records, want 2000", len(got))
		}
	}
}

// TestRecordLargerThanTheFrameIsNotSplit: rule 1 outranks the size target.
func TestRecordLargerThanTheFrameIsNotSplit(t *testing.T) {
	big := bytes.Repeat([]byte("x"), MinFrameBytesFloor*3)
	var buf bytes.Buffer
	w, err := NewWriter(&buf, MinFrameBytesFloor)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][]byte{jobRecord(1), big, jobRecord(2)} {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, _ := readAll(t, buf.Bytes())
	if len(got) != 3 {
		t.Fatalf("recovered %d records, want 3", len(got))
	}
	if !bytes.Equal(got[1], big) {
		t.Errorf("the oversized record came back changed (len %d, want %d)", len(got[1]), len(big))
	}
}

// TestCorruptFrameCostsOnlyThatFrame is the property the format exists for.
func TestCorruptFrameCostsOnlyThatFrame(t *testing.T) {
	stream, frames := writeStream(t, 1200, MinFrameBytesFloor)
	if frames < 3 {
		t.Fatalf("need at least 3 frames, got %d", frames)
	}
	// Flip a byte in the middle of the stream: squarely inside some frame's payload.
	damaged := append([]byte(nil), stream...)
	mid := len(damaged) / 2
	damaged[mid] ^= 0xFF

	got, res := readAll(t, damaged)
	if res.Damaged == 0 && len(res.Problems) == 0 {
		t.Fatal("a flipped byte was not detected at all")
	}
	if len(got) == 0 {
		t.Fatal("a one-byte flip cost every record in the stream")
	}
	// Most of the stream must survive: one bad frame out of several.
	if len(got) < 1200/2 {
		t.Errorf("recovered only %d of 1200 records from a one-byte flip; damage should cost one frame", len(got))
	}
	t.Logf("recovered %d/1200 records across %d frames after a 1-byte flip (%d damaged)", len(got), res.Frames, res.Damaged)
}

// TestTruncatedStreamKeepsEverythingBeforeTheCut: the realistic failure for a file still being
// written, or a copy that was interrupted.
func TestTruncatedStreamKeepsEverythingBeforeTheCut(t *testing.T) {
	stream, frames := writeStream(t, 1200, MinFrameBytesFloor)
	if frames < 3 {
		t.Fatalf("need at least 3 frames, got %d", frames)
	}
	cut := stream[:len(stream)*2/3]
	got, res := readAll(t, cut)
	if len(got) == 0 {
		t.Fatal("a truncated stream yielded nothing")
	}
	if len(got) >= 1200 {
		t.Fatalf("a truncated stream yielded %d records, more than were written before the cut", len(got))
	}
	if len(res.Problems) == 0 {
		t.Error("truncation was not reported")
	}
	t.Logf("recovered %d records from a stream cut at 2/3", len(got))
}

// TestGarbageInsertedMidStreamResyncs: damage that changes the length of the file, which is the
// case a length-prefixed format cannot simply step over.
func TestGarbageInsertedMidStreamResyncs(t *testing.T) {
	stream, frames := writeStream(t, 1200, MinFrameBytesFloor)
	if frames < 3 {
		t.Fatalf("need at least 3 frames, got %d", frames)
	}
	mid := len(stream) / 2
	var damaged []byte
	damaged = append(damaged, stream[:mid]...)
	damaged = append(damaged, bytes.Repeat([]byte("GARBAGE!"), 500)...)
	damaged = append(damaged, stream[mid:]...)

	got, res := readAll(t, damaged)
	if len(got) == 0 {
		t.Fatal("inserted garbage cost the whole stream")
	}
	if res.SkipBytes == 0 {
		t.Error("the reader did not report skipping the garbage")
	}
	// Records from after the garbage must be recovered, which only resync can do.
	last := jobRecord(1199)
	found := false
	for _, r := range got {
		if bytes.Equal(r, last) {
			found = true
		}
	}
	if !found {
		t.Errorf("the final record was not recovered after resync; got %d records", len(got))
	}
	t.Logf("recovered %d/1200 records after 4000 bytes of inserted garbage (skipped %d bytes)", len(got), res.SkipBytes)
}

// TestNotAFramedStreamIsAnError keeps the reader from pretending a tar.gz is a framed stream.
func TestNotAFramedStreamIsAnError(t *testing.T) {
	if _, err := Read([]byte("\x1f\x8b\x08 this is gzip"), func([]byte) error { return nil }); err == nil {
		t.Fatal("a non-framed stream was accepted")
	} else if !strings.Contains(err.Error(), "stream magic") {
		t.Errorf("err = %v, want it to name the stream magic", err)
	}
}

// TestFrameSizeFloorIsEnforced: below the floor the format degenerates toward per-record frames.
func TestFrameSizeFloorIsEnforced(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, 1)
	if err != nil {
		t.Fatal(err)
	}
	if w.minFrameBytes != MinFrameBytesFloor {
		t.Errorf("minFrameBytes = %d, want it raised to the floor %d", w.minFrameBytes, MinFrameBytesFloor)
	}
	w2, err := NewWriter(&bytes.Buffer{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if w2.minFrameBytes != DefaultMinFrameBytes {
		t.Errorf("minFrameBytes = %d, want the default %d", w2.minFrameBytes, DefaultMinFrameBytes)
	}
}
