// Package dropboxframe implements the framed archive-backup stream: a sequence of
// independently-decompressible, self-checking frames, each holding whole records.
//
// It exists because a .tar.gz is one gzip stream: a truncation or a flipped bit costs everything
// after the damage, so a 2000-record batch can be lost to one bad byte. A framed stream bounds
// that at one frame, and a reader that hits a bad frame resyncs by scanning for the next frame
// magic instead of giving up on the rest of the file.
//
// The rules the format keeps, in priority order:
//
//  1. A record is never split across frames, and a frame holds only whole records. A record
//     larger than the frame size gets a frame of its own rather than being cut.
//  2. Records are stored verbatim, one after another, with their attributes inline. Nothing in a
//     frame refers to anything outside it -- no shared dictionary, no interning, no back
//     references. A frame is readable with nothing but this file.
//  3. Frames are size-bounded, not per-record. Compressing one job ClassAd alone wastes most of
//     the ratio, because zstd has no history to work with; letting a frame reach a few hundred
//     kilobytes of records gives it the redundancy between neighbouring job ads to exploit, while
//     still bounding damage at that much data.
package dropboxframe

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/klauspost/compress/zstd"
)

// StreamMagic opens a framed stream. The trailing byte is the stream format version.
var StreamMagic = [8]byte{'C', 'A', 'D', 'B', 'O', 'X', 0x01, 0x00}

// FrameMagic opens each frame. A reader scans for it to resync past damage, so it must be
// distinctive and fixed-width.
var FrameMagic = [4]byte{'F', 'R', 'M', 0x01}

// HeaderLen is the fixed size of a frame header: magic + records + rawLen + compLen + crc.
const HeaderLen = 4 + 4 + 4 + 4 + 4

// DefaultMinFrameBytes is how many bytes of records a frame accumulates before it is closed.
//
// It is measured on the records as handed in, not on the compressed result, so the bound is one
// an operator can reason about: this many records is what a damaged frame costs.
//
// 256 KiB, not 1 MiB, and the reason is the batch size. The exporter's default batch is 2000
// records, which is only ~823 KB of typical job ClassAds -- so a 1 MiB frame swallows a whole
// batch and leaves one frame per file, the same blast radius as the gzip tarball it replaces, for
// about 3% less space than 256 KiB. A frame size only bounds damage if it is meaningfully smaller
// than a batch. At 256 KiB a default batch is four frames of ~500 records, and the file is still
// ~37% smaller than the tarball because the comparison is zstd against gzip. Sites that value
// recovery over space should set 64 KiB (thirteen frames, ~153 records each, still ~32% smaller
// than a tarball).
const DefaultMinFrameBytes = 256 << 10

// MinFrameBytesFloor is the smallest configurable frame size. Below this the format degenerates
// towards per-record frames and the compression ratio collapses.
const MinFrameBytesFloor = 4 << 10

// crcTable is Castagnoli: hardware-accelerated on the platforms this runs on.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// frameHeader is the parsed fixed-size header.
type frameHeader struct {
	Records uint32
	RawLen  uint32
	CompLen uint32
	CRC     uint32
}

// putHeader encodes a frame header into b, which must be at least HeaderLen long.
func putHeader(b []byte, h frameHeader) {
	copy(b[0:4], FrameMagic[:])
	binary.LittleEndian.PutUint32(b[4:8], h.Records)
	binary.LittleEndian.PutUint32(b[8:12], h.RawLen)
	binary.LittleEndian.PutUint32(b[12:16], h.CompLen)
	binary.LittleEndian.PutUint32(b[16:20], h.CRC)
}

// parseHeader decodes a frame header. It returns false when b does not start with the frame magic.
func parseHeader(b []byte) (frameHeader, bool) {
	if len(b) < HeaderLen {
		return frameHeader{}, false
	}
	if string(b[0:4]) != string(FrameMagic[:]) {
		return frameHeader{}, false
	}
	return frameHeader{
		Records: binary.LittleEndian.Uint32(b[4:8]),
		RawLen:  binary.LittleEndian.Uint32(b[8:12]),
		CompLen: binary.LittleEndian.Uint32(b[12:16]),
		CRC:     binary.LittleEndian.Uint32(b[16:20]),
	}, true
}

// encodePayload packs records into the uncompressed frame payload: each record length-prefixed.
func encodePayload(records [][]byte) []byte {
	n := 0
	for _, r := range records {
		n += 4 + len(r)
	}
	out := make([]byte, 0, n)
	var lenBuf [4]byte
	for _, r := range records {
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(r)))
		out = append(out, lenBuf[:]...)
		out = append(out, r...)
	}
	return out
}

// decodePayload unpacks a frame payload back into records.
//
// A payload that ends mid-record returns the records decoded so far along with an error: the CRC
// already proved the bytes are intact, so a short read here means the writer was interrupted, and
// the whole records before the cut are still good.
func decodePayload(p []byte) ([][]byte, error) {
	var out [][]byte
	for off := 0; off < len(p); {
		if off+4 > len(p) {
			return out, fmt.Errorf("dropboxframe: payload ends mid-length after %d records", len(out))
		}
		n := int(binary.LittleEndian.Uint32(p[off : off+4]))
		off += 4
		if n < 0 || off+n > len(p) {
			return out, fmt.Errorf("dropboxframe: record %d claims %d bytes, %d remain", len(out), n, len(p)-off)
		}
		rec := make([]byte, n)
		copy(rec, p[off:off+n])
		out = append(out, rec)
		off += n
	}
	return out, nil
}

// compress zstd-encodes a payload with no trained dictionary, so the frame depends on nothing
// outside itself.
func compress(raw []byte) ([]byte, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	return enc.EncodeAll(raw, nil), nil
}

// decompress reverses compress.
func decompress(comp []byte, rawLen uint32) ([]byte, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	out, err := dec.DecodeAll(comp, make([]byte, 0, rawLen))
	if err != nil {
		return nil, err
	}
	return out, nil
}

// crc32Sum is the frame checksum over the compressed payload. Checksumming the compressed bytes
// rather than the raw ones means damage is caught before the decompressor is handed bad input,
// which is both faster and safer than letting zstd fail in its own way.
func crc32Sum(b []byte) uint32 { return crc32.Checksum(b, crcTable) }
