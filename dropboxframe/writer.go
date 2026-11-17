package dropboxframe

import (
	"io"
)

// Writer builds a framed stream. Records are buffered until they reach MinFrameBytes, then written
// as one frame. Close flushes whatever is left.
//
// The size trigger is on the records as handed in, not on the compressed result: a bound an
// operator can reason about ("a damaged frame costs about this many records") is worth more than
// hitting an exact on-disk size, and measuring the compressed size per record would mean flushing
// the compressor after every record, which is what destroys the ratio in the first place.
type Writer struct {
	w             io.Writer
	minFrameBytes int

	pending    [][]byte
	pendingLen int

	frames  int
	records int
	written int64
}

// NewWriter starts a framed stream on w, emitting the stream header immediately. A minFrameBytes
// of 0 uses DefaultMinFrameBytes; anything below MinFrameBytesFloor is raised to it.
func NewWriter(w io.Writer, minFrameBytes int) (*Writer, error) {
	if minFrameBytes <= 0 {
		minFrameBytes = DefaultMinFrameBytes
	}
	if minFrameBytes < MinFrameBytesFloor {
		minFrameBytes = MinFrameBytesFloor
	}
	if _, err := w.Write(StreamMagic[:]); err != nil {
		return nil, err
	}
	return &Writer{w: w, minFrameBytes: minFrameBytes, written: int64(len(StreamMagic))}, nil
}

// Write adds one record. The record is copied, so the caller may reuse its buffer.
//
// A record on its own larger than the frame size is not split: it is flushed as a frame of its
// own. Rule 1 (a frame holds whole records) outranks the size target.
func (fw *Writer) Write(record []byte) error {
	cp := make([]byte, len(record))
	copy(cp, record)
	fw.pending = append(fw.pending, cp)
	fw.pendingLen += len(cp)
	fw.records++
	if fw.pendingLen >= fw.minFrameBytes {
		return fw.Flush()
	}
	return nil
}

// Flush writes any buffered records as a frame. It is a no-op when nothing is buffered, so it is
// safe to call at a roll boundary.
func (fw *Writer) Flush() error {
	if len(fw.pending) == 0 {
		return nil
	}
	raw := encodePayload(fw.pending)
	comp, err := compress(raw)
	if err != nil {
		return err
	}
	hdr := make([]byte, HeaderLen)
	putHeader(hdr, frameHeader{
		Records: uint32(len(fw.pending)),
		RawLen:  uint32(len(raw)),
		CompLen: uint32(len(comp)),
		CRC:     crc32Checksum(comp),
	})
	if _, err := fw.w.Write(hdr); err != nil {
		return err
	}
	if _, err := fw.w.Write(comp); err != nil {
		return err
	}
	fw.written += int64(len(hdr) + len(comp))
	fw.frames++
	fw.pending, fw.pendingLen = nil, 0
	return nil
}

// Close flushes the final partial frame. It does not close the underlying writer.
func (fw *Writer) Close() error { return fw.Flush() }

// Stats reports what has been written so far.
func (fw *Writer) Stats() (frames, records int, bytes int64) {
	return fw.frames, fw.records, fw.written
}

func crc32Checksum(b []byte) uint32 {
	return crc32Sum(b)
}
