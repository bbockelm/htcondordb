// Wire-buffer encoding for the queue-query entry points: turning a row of
// old-ClassAd text into the exact bytes an HTCondor daemon puts on a CEDAR stream
// for one ad, so the daemon's main thread copies a buffer instead of building and
// unparsing a ClassAd.
//
// An ad on the wire (C++ _putClassAd in classad_oldnew.cpp) is
//
//	int64 numExprs            8 bytes, network byte order
//	"Attr = Value" x numExprs each NUL-terminated
//	"" ""                     the trailing MyType/TargetType strings
//
// with one deviation under encryption: CEDAR prefixes each STRING with its length
// when the channel is encrypting (Stream::put(char const *)). The integer and the
// string bytes are identical either way, so the buffer here is the unencrypted
// framing and the consumer re-frames the strings itself on an encrypted channel --
// it can, because every string is NUL-terminated and the count is in the first
// eight bytes. Nothing has to be parsed to do it.
//
// Records are concatenated into a chunk, each behind a 4-byte little-endian length
// so the consumer can walk them:
//
//	uint32 payloadLen | payload (the bytes above) | uint32 payloadLen | payload | ...
package main

import (
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/wire"
)

// recordHeaderLen is the per-ad length prefix inside a chunk.
const recordHeaderLen = 4

// A chunk can instead hold complete CEDAR message frames, which is what the
// response writer in queuefd.go wants: the bytes it builds go to the client's
// socket unaltered, with nothing to reframe on the way. A frame header is a
// one-byte end flag and a four-byte big-endian payload length, exactly as
// HTCondor's ReliSock writes it.
const (
	cedarHeaderLen    = 5
	cedarPartial      = 0 // more frames follow
	cedarEndOfMessage = 1 // last frame of this message
	// maxFrameBody is the largest payload one frame may carry; HTCondor's
	// ReliSock rejects a bigger one (MAX_MESSAGE_SIZE in reli_sock.cpp).
	maxFrameBody = 1024 * 1024
)

// The two attributes a job query's summary counts are bucketed by. They are read
// out of every row as it goes past -- the same thing the schedd does per job ad --
// rather than asked for in a second, grouped query, because grouping by two
// columns at once drops the database off its columnar aggregate path and onto a
// full record scan.
const (
	attrJobStatus   = "JobStatus"
	attrJobUniverse = "JobUniverse"
)

// adRecordBuilder appends ad records to a chunk buffer. It is reused across the
// rows of one query; its per-row outputs are read after each appendRecord.
type adRecordBuilder struct {
	// emit is false for a summary-only query, which reads the rows for their
	// counts and produces no bytes.
	emit bool

	// projection, when non-nil, keeps only these attributes (matched
	// case-insensitively, folded to lower case). It is for the fallback against a
	// database that cannot project; the normal path projects at the source and
	// leaves this nil. MyType/TargetType are never filtered out by it, matching
	// putClassAd's whitelist handling, which always sends the trailing types.
	projection map[string]bool

	// suppress names attributes to read and then drop, folded to lower case. It
	// holds whichever of JobStatus and JobUniverse were added to the caller's
	// projection for counting's sake: the query has to see them, and the client
	// did not ask for them.
	//
	// A slice, and not the set it reads like, because the wire path tests every
	// attribute of every ad against it -- millions of times for one condor_q --
	// and starting a Go map iterator costs an order of magnitude more than
	// comparing against the two names it would have yielded. Measured inside a
	// schedd answering 20000-ad queries, ranging the map was 8.5% of everything
	// this library did, and all but a fortieth of that was mapIterStart.
	suppress []string

	// wireProj is the projection applied by appendWireRecord, which receives whole
	// ads: the wire-row stream cannot expand a projection to the attributes its
	// expressions reference, so a projected query asks for everything and filters
	// here. nil on the text path and for a query with no projection.
	wireProj *wireProjector

	// cedarFrame makes each record a complete CEDAR message frame rather than a
	// length-delimited record for the consumer to reframe.
	cedarFrame bool

	// serverTime, when non-empty, is appended to every ad as one more expression.
	// HTCondor's PUT_CLASSAD_SERVER_TIME does the same, so a client can compute
	// elapsed times against the server's clock rather than its own.
	serverTime string

	// Per-row outputs, rewritten by each appendRecord. haveStatus is false for a
	// row with no integer JobStatus, which cannot be bucketed and is not counted --
	// the same ad the schedd's own counter declines to place.
	universe   int
	status     int
	haveStatus bool
}

// appendRecord appends one ad record for row (old-ClassAd text: "Attr = Value"
// lines) to dst and returns the extended buffer. Private attributes are dropped,
// which is what the schedd's PUT_CLASSAD_NO_PRIVATE does and is a name-for-name
// match with it (both use the V1 list plus the _condor_priv prefix).
//
// A row with no usable line still produces a record: an ad with zero expressions
// is well formed on the wire, and dropping it would make the ads the client
// receives disagree with the ads the caller thinks it sent.
func (b *adRecordBuilder) appendRecord(dst []byte, row string) []byte {
	b.universe, b.status, b.haveStatus = 0, 0, false

	start := len(dst)
	if b.emit {
		dst = b.startRecord(dst)
	}

	n := 0
	for len(row) > 0 {
		var line string
		if i := strings.IndexByte(row, '\n'); i >= 0 {
			line, row = row[:i], row[i+1:]
		} else {
			line, row = row, ""
		}
		if line == "" {
			continue
		}
		name := attrNameOf(line)
		if name == "" {
			continue
		}
		if classad.IsPrivateAttribute(name) {
			continue
		}

		// Counting happens before any filtering, so a projection that leaves
		// JobStatus out of the response still produces accurate counts.
		switch {
		case strings.EqualFold(name, attrJobStatus):
			if v, err := strconv.Atoi(valueOf(line)); err == nil {
				b.status, b.haveStatus = v, true
			}
		case strings.EqualFold(name, attrJobUniverse):
			if v, err := strconv.Atoi(valueOf(line)); err == nil {
				b.universe = v
			}
		}

		if !b.emit {
			continue
		}
		lower := strings.ToLower(name)
		if b.suppressed(lower) {
			continue
		}
		if b.projection != nil && !b.projection[lower] && !isTypeAttr(name) {
			continue
		}
		dst = append(dst, line...)
		dst = append(dst, 0)
		n++
	}
	if !b.emit {
		return dst
	}
	if b.serverTime != "" {
		dst = append(dst, b.serverTime...)
		dst = append(dst, 0)
		n++
	}

	return b.endRecord(dst, start, n)
}

// endRecord closes a record begun at offset start in dst with n expressions
// already appended: the two trailing type strings, then the two headers that had
// to wait for the body's length and count.
//
// HTCondor always sends the type strings empty and carries the real
// MyType/TargetType as ordinary attributes in the body (which is how they arrive
// here), so matching it means two empty strings.
func (b *adRecordBuilder) endRecord(dst []byte, start, n int) []byte {
	dst = append(dst, 0, 0)
	hdrLen := b.headerLen()
	binary.BigEndian.PutUint64(dst[start+hdrLen:], uint64(n))
	payloadLen := len(dst) - start - hdrLen
	if !b.cedarFrame {
		binary.LittleEndian.PutUint32(dst[start:], uint32(payloadLen))
		return dst
	}
	if payloadLen > maxFrameBody {
		return reframeOversize(dst, start)
	}
	binary.BigEndian.PutUint32(dst[start+1:], uint32(payloadLen))
	return dst
}

// headerLen is the length of the per-record header this builder writes.
func (b *adRecordBuilder) headerLen() int {
	if b.cedarFrame {
		return cedarHeaderLen
	}
	return recordHeaderLen
}

// startRecord opens a record at the end of dst: the header, zeroed, and the
// expression count, both filled in by endRecord once the body's size is known.
func (b *adRecordBuilder) startRecord(dst []byte) []byte {
	if b.cedarFrame {
		dst = append(dst, cedarEndOfMessage, 0, 0, 0, 0)
	} else {
		dst = append(dst, 0, 0, 0, 0)
	}
	return append(dst, 0, 0, 0, 0, 0, 0, 0, 0)
}

// reframeOversize rewrites the single frame beginning at start as a run of
// partial frames and a final one, for the pathological ad whose wire form will
// not fit in one CEDAR frame. ReliSock splits such an ad the same way when it
// overflows its send buffer, so the receiver needs nothing it does not already
// do. Cold path: allocating here costs nothing on any real ad.
func reframeOversize(dst []byte, start int) []byte {
	payload := append([]byte(nil), dst[start+cedarHeaderLen:]...)
	out := dst[:start]
	for off := 0; off < len(payload); off += maxFrameBody {
		end := min(off+maxFrameBody, len(payload))
		flag := byte(cedarPartial)
		if end == len(payload) {
			flag = cedarEndOfMessage
		}
		var h [cedarHeaderLen]byte
		h[0] = flag
		binary.BigEndian.PutUint32(h[1:], uint32(end-off))
		out = append(out, h[:]...)
		out = append(out, payload[off:end]...)
	}
	return out
}

// suppressedName is the suppress test for raw name bytes. The set holds at most
// the two counting attributes, so a scan beats folding the name into a string to
// look it up.
func (b *adRecordBuilder) suppressedName(name []byte) bool {
	for _, s := range b.suppress {
		if wire.FoldEqualBytes(name, s) {
			return true
		}
	}
	return false
}

// suppressed is the same test for a name already folded to lower case, which is
// the form the text path has in hand.
func (b *adRecordBuilder) suppressed(lower string) bool {
	for _, s := range b.suppress {
		if s == lower {
			return true
		}
	}
	return false
}

// attrNameOf returns the attribute name of an old-ClassAd assignment line, or ""
// if the line does not look like one. The renderer always writes exactly
// "Name = Value", so the separator is found without scanning the value.
func attrNameOf(line string) string {
	i := strings.Index(line, " = ")
	if i <= 0 {
		return ""
	}
	return line[:i]
}

// valueOf returns the right-hand side of such a line.
func valueOf(line string) string {
	i := strings.Index(line, " = ")
	if i < 0 {
		return ""
	}
	return line[i+3:]
}

// isTypeAttr reports the two attributes a projection never removes.
func isTypeAttr(name string) bool {
	return strings.EqualFold(name, "MyType") || strings.EqualFold(name, "TargetType")
}

// serverTimeExpr renders the ServerTime expression HTCondor appends to a query
// result's ads.
func serverTimeExpr(now int64) string {
	return "ServerTime = " + strconv.FormatInt(now, 10)
}

// lowerSet folds an attribute list to the set appendRecord matches against.
func lowerSet(attrs []string) map[string]bool {
	if len(attrs) == 0 {
		return nil
	}
	m := make(map[string]bool, len(attrs))
	for _, a := range attrs {
		if a = strings.TrimSpace(a); a != "" {
			m[strings.ToLower(a)] = true
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

// withCountingAttrs returns the projection to ask the database for and the set of
// attributes to drop again on the way out: a projected query still has to see
// JobStatus and JobUniverse to count by them, and the client still must not
// receive attributes it did not ask for.
//
// An empty projection means every attribute, so there is nothing to add and
// nothing to drop.
func withCountingAttrs(attrs []string) (request []string, suppress []string) {
	if len(attrs) == 0 {
		return nil, nil
	}
	have := lowerSet(attrs)
	request = attrs
	for _, extra := range []string{attrJobStatus, attrJobUniverse} {
		if !have[strings.ToLower(extra)] {
			request = append(request, extra)
			suppress = append(suppress, strings.ToLower(extra))
		}
	}
	return request, suppress
}

// splitList splits a comma-separated attribute list, ignoring empty entries.
func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
