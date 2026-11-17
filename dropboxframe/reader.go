package dropboxframe

import (
	"bytes"
	"fmt"
)

// ReadResult is the outcome of reading a framed stream.
type ReadResult struct {
	Frames    int      // frames decoded successfully
	Records   int      // records recovered
	Damaged   int      // frames skipped because their checksum or header was bad
	SkipBytes int      // bytes skipped while resyncing
	Problems  []string // one line per damaged region
}

// Read decodes a whole framed stream from b, calling yield for each record in order.
//
// This is the function the format exists for: a damaged frame costs that frame and nothing else.
// On a bad header, a bad checksum, or a truncated frame, the reader scans forward for the next
// frame magic and carries on. The records before the damage have already been yielded and the
// records after it are recovered, so a single bad byte cannot cost the file.
//
// It returns an error only when the stream does not begin with the stream magic -- i.e. when this
// is not a framed stream at all. Damage within the stream is reported in ReadResult, not as an
// error, because the recovered records are the point.
func Read(b []byte, yield func(record []byte) error) (ReadResult, error) {
	var res ReadResult
	if len(b) < len(StreamMagic) || !bytes.Equal(b[:len(StreamMagic)], StreamMagic[:]) {
		return res, fmt.Errorf("dropboxframe: not a framed stream (bad stream magic)")
	}
	off := len(StreamMagic)

	for off < len(b) {
		hdr, ok := parseHeader(b[off:])
		if !ok {
			// Not at a frame boundary: resync.
			n := resync(b, off+1)
			if n < 0 {
				res.SkipBytes += len(b) - off
				res.Problems = append(res.Problems, fmt.Sprintf("trailing %d bytes at offset %d are not a frame", len(b)-off, off))
				break
			}
			res.SkipBytes += n - off
			res.Problems = append(res.Problems, fmt.Sprintf("skipped %d bytes of damage at offset %d", n-off, off))
			off = n
			continue
		}
		start := off
		payloadStart := off + HeaderLen
		payloadEnd := payloadStart + int(hdr.CompLen)
		if payloadEnd > len(b) {
			// A truncated final frame: the stream was cut mid-write.
			res.Damaged++
			res.Problems = append(res.Problems, fmt.Sprintf("frame at offset %d is truncated (wants %d bytes, %d remain)", start, hdr.CompLen, len(b)-payloadStart))
			break
		}
		comp := b[payloadStart:payloadEnd]
		if crc32Sum(comp) != hdr.CRC {
			res.Damaged++
			res.Problems = append(res.Problems, fmt.Sprintf("frame at offset %d failed its checksum; %d records in it are lost", start, hdr.Records))
			// Trust the length enough to step past it, but verify we land on a frame; if not,
			// resync. A corrupt header length would otherwise send us into the weeds -- and
			// damage that CHANGES the length of the stream (inserted or dropped bytes) makes the
			// declared length meaningless, so the scan is the only way back.
			next := payloadEnd
			if _, ok := parseHeader(b[min(next, len(b)):]); !ok && next < len(b) {
				if n := resync(b, start+1); n >= 0 {
					next = n
				} else {
					next = len(b)
				}
				// Everything between this frame's header and where we resynced is unreadable,
				// not just the declared payload: count all of it as skipped.
				res.SkipBytes += next - start
			}
			off = next
			continue
		}
		raw, err := decompress(comp, hdr.RawLen)
		if err != nil {
			res.Damaged++
			res.Problems = append(res.Problems, fmt.Sprintf("frame at offset %d did not decompress: %v", start, err))
			off = payloadEnd
			continue
		}
		recs, perr := decodePayload(raw)
		if perr != nil {
			res.Problems = append(res.Problems, fmt.Sprintf("frame at offset %d: %v", start, perr))
		}
		for _, r := range recs {
			if err := yield(r); err != nil {
				return res, err
			}
			res.Records++
		}
		res.Frames++
		off = payloadEnd
	}
	return res, nil
}

// resync scans forward from off for the next plausible frame header: the frame magic followed by a
// header whose declared payload either fits in what remains or runs to the end of the stream.
// It returns -1 when there is no further frame.
func resync(b []byte, off int) int {
	for off < len(b) {
		idx := bytes.Index(b[off:], FrameMagic[:])
		if idx < 0 {
			return -1
		}
		cand := off + idx
		if h, ok := parseHeader(b[cand:]); ok {
			// A plausible frame: its payload must fit. This rejects the frame magic appearing by
			// chance inside compressed data, which it will, given enough frames.
			if cand+HeaderLen+int(h.CompLen) <= len(b) {
				comp := b[cand+HeaderLen : cand+HeaderLen+int(h.CompLen)]
				if crc32Sum(comp) == h.CRC {
					return cand
				}
			}
		}
		off = cand + 1
	}
	return -1
}
