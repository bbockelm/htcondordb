package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/stream"
)

// decodeCedarFrames splits a response the writer produced into one message per
// ad, reading the CEDAR framing an HTCondor client reads: a one-byte end flag
// and a four-byte big-endian payload length per frame, with a message running
// until a frame carries the end flag.
func decodeCedarFrames(t *testing.T, raw []byte) [][]byte {
	t.Helper()
	var msgs [][]byte
	var cur []byte
	for len(raw) > 0 {
		if len(raw) < cedarHeaderLen {
			t.Fatalf("trailing %d bytes are too short for a frame header", len(raw))
		}
		end := raw[0]
		n := int(binary.BigEndian.Uint32(raw[1:5]))
		if end != cedarPartial && end != cedarEndOfMessage {
			t.Fatalf("frame end flag %d is neither partial nor end-of-message", end)
		}
		if n > maxFrameBody {
			t.Fatalf("frame payload %d exceeds what HTCondor will read (%d)", n, maxFrameBody)
		}
		if len(raw) < cedarHeaderLen+n {
			t.Fatalf("frame says %d bytes of payload but only %d remain", n, len(raw)-cedarHeaderLen)
		}
		cur = append(cur, raw[cedarHeaderLen:cedarHeaderLen+n]...)
		raw = raw[cedarHeaderLen+n:]
		if end == cedarEndOfMessage {
			msgs = append(msgs, cur)
			cur = nil
		}
	}
	if cur != nil {
		t.Fatalf("the response ends part way through a message")
	}
	return msgs
}

// adExprs reads one ad's payload the way putClassAd wrote it.
func adExprs(t *testing.T, payload []byte) []string {
	t.Helper()
	if len(payload) < 8 {
		t.Fatalf("ad payload too short: %d bytes", len(payload))
	}
	count := int(binary.BigEndian.Uint64(payload))
	var strs []string
	for body := payload[8:]; len(body) > 0; {
		i := bytes.IndexByte(body, 0)
		if i < 0 {
			t.Fatalf("unterminated string in an ad payload")
		}
		strs = append(strs, string(body[:i]))
		body = body[i+1:]
	}
	if len(strs) != count+2 {
		t.Fatalf("ad holds %d strings, want %d expressions plus 2 trailing types", len(strs), count)
	}
	return strs[:count]
}

// newPlainSink is a response writer for a session in the clear.
func newPlainSink(t *testing.T, fd int) *fdSink {
	t.Helper()
	s, err := newFDSink(fd, nil)
	if err != nil {
		t.Fatalf("building the response writer: %v", err)
	}
	return s
}

// socketPair is the client's socket, split in two: the descriptor handed to the
// writer, and a reader standing in for condor_q.
func socketPair(t *testing.T) (writerFD int, client *os.File) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	client = os.NewFile(uintptr(fds[1]), "client")
	t.Cleanup(func() { _ = client.Close() })
	return fds[0], client
}

// runSink drives one response-writing request to completion and returns
// everything the client saw.
func runSink(t *testing.T, cl *dbrpc.Client, constraint string, attrs []string, limit int,
	b *adRecordBuilder) (raw []byte, r *queueRequest) {
	t.Helper()
	fd, client := socketPair(t)
	sink := newPlainSink(t, fd)
	r = &queueRequest{
		client: &queueClient{notifyFD: -1},
		chunks: make(chan queueChunk, 16),
		sink:   sink,
	}
	b.cedarFrame = true

	// Read the client's side while the writer runs: with a socket buffer smaller
	// than the response, a writer nobody is reading would deadlock the test the
	// same way it would stall a real query.
	var wg sync.WaitGroup
	var got []byte
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 64<<10)
		for {
			n, err := client.Read(buf)
			got = append(got, buf[:n]...)
			if err != nil {
				return
			}
		}
	}()

	r.wg.Add(1)
	go r.stream(context.Background(), cl, dbrpc.DefaultTable, constraint, attrs, limit, b, true)
	r.wg.Wait()
	sink.close()
	wg.Wait()
	return got, r
}

func TestSinkWritesCedarFramedAds(t *testing.T) {
	cl := newTestServer(t, map[string]string{
		"1.0": "ClusterId = 1\nProcId = 0\nOwner = \"alice\"\nJobStatus = 1\nJobUniverse = 5\nClaimId = \"secret\"",
		"1.1": "ClusterId = 1\nProcId = 1\nOwner = \"alice\"\nJobStatus = 2\nJobUniverse = 5",
		"2.0": "ClusterId = 2\nProcId = 0\nOwner = \"bob\"\nJobStatus = 1\nJobUniverse = 7",
	})

	raw, r := runSink(t, cl, "true", nil, -1, &adRecordBuilder{emit: true})
	if r.err != nil {
		t.Fatalf("the response failed: %v", r.err)
	}
	msgs := decodeCedarFrames(t, raw)
	if len(msgs) != 3 {
		t.Fatalf("the client saw %d messages, want one per ad (3)", len(msgs))
	}
	if got := r.written.Load(); got != 3 {
		t.Errorf("the request reports %d ads written, want 3", got)
	}
	if r.sink.conn.dirty.Load() {
		t.Error("a response that completed left the socket marked unusable")
	}
	for _, m := range msgs {
		for _, e := range adExprs(t, m) {
			if strings.HasPrefix(e, "ClaimId ") {
				t.Errorf("a private attribute reached the wire: %q", e)
			}
		}
	}
	if got := sortedLines(r.counts); len(got) != 3 {
		t.Errorf("counts = %q, want three buckets", got)
	}
}

// The response the writer produces must be the same ads, in the same framing,
// as the chunked path the caller writes itself -- that path is what condor_q
// reads today, and the two must not be allowed to drift apart.
func TestSinkMatchesChunkedPath(t *testing.T) {
	ads := map[string]string{}
	for i := range 40 {
		ads[fmt.Sprintf("%d.0", i)] = fmt.Sprintf(
			"ClusterId = %d\nProcId = 0\nOwner = \"u%d\"\nJobStatus = %d\nJobUniverse = 5\nArgs = \"%s\"",
			i, i%3, (i%5)+1, strings.Repeat("x", i*7))
	}
	cl := newTestServer(t, ads)

	chunked, _ := runStream(t, cl, "true", nil, -1, &adRecordBuilder{emit: true})
	raw, r := runSink(t, cl, "true", nil, -1, &adRecordBuilder{emit: true})
	if r.err != nil {
		t.Fatalf("the response failed: %v", r.err)
	}
	framed := decodeCedarFrames(t, raw)
	if len(framed) != len(chunked) {
		t.Fatalf("the writer sent %d ads, the chunked path produced %d", len(framed), len(chunked))
	}
	for i := range framed {
		got := adExprs(t, framed[i])
		if len(got) != len(chunked[i]) {
			t.Fatalf("ad %d has %d expressions, the chunked path gave %d", i, len(got), len(chunked[i]))
		}
		for j := range got {
			if got[j] != chunked[i][j] {
				t.Fatalf("ad %d expression %d: writer %q, chunked path %q", i, j, got[j], chunked[i][j])
			}
		}
	}
}

// A client that goes away mid-response: the error must reach the request rather
// than the response ending quietly, and the socket must be reported unusable so
// the caller closes the connection instead of appending a summary ad to half an
// ad.
func TestSinkReportsAWriteFailure(t *testing.T) {
	ads := map[string]string{}
	for i := range 4000 {
		ads[fmt.Sprintf("%d.0", i)] = fmt.Sprintf(
			"ClusterId = %d\nProcId = 0\nJobStatus = 1\nJobUniverse = 5\nPad = \"%s\"",
			i, strings.Repeat("y", 900))
	}
	cl := newTestServer(t, ads)

	fd, client := socketPair(t)
	sink := newPlainSink(t, fd)
	r := &queueRequest{
		client: &queueClient{notifyFD: -1},
		chunks: make(chan queueChunk, 16),
		sink:   sink,
	}
	// Read nothing and hang up, so the writer meets a closed peer part way through.
	_ = client.Close()

	r.wg.Add(1)
	r.stream(context.Background(), cl, dbrpc.DefaultTable, "true", nil, -1,
		&adRecordBuilder{emit: true, cedarFrame: true}, true)
	sink.close()

	if r.err == nil {
		t.Fatal("writing to a hung-up client finished without an error")
	}
	if !errors.Is(r.err, syscall.EPIPE) && !errors.Is(r.err, syscall.ECONNRESET) {
		t.Logf("write failed with %v", r.err)
	}
}

// Abandoning a request must not take the socket down: the caller holds its own
// descriptor for the same connection and is about to report the failure on it.
func TestSinkAbortLeavesTheSocketOpen(t *testing.T) {
	fd, client := socketPair(t)
	sink := newPlainSink(t, fd)
	sink.abort()

	if err := sink.writeChunk(context.Background(), []byte("unsent")); !errors.Is(err, errWriterStopped) {
		t.Fatalf("a stopped writer returned %v, want errWriterStopped", err)
	}
	if sink.conn.dirty.Load() {
		t.Error("stopping before any byte went out marked the socket unusable")
	}
	// The caller's end is still a working socket.
	if _, err := client.Write([]byte("still here")); err != nil {
		t.Fatalf("the caller's end of the socket was broken by abort: %v", err)
	}
	sink.close()
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 16)
	if n, err := client.Read(buf); n != 0 || err == nil {
		t.Fatalf("read %d bytes (%v); the writer should have sent nothing", n, err)
	}
}

// A non-blocking socket is the case the write loop's poll exists for: the
// descriptor's flags are shared with the caller's copy, so the writer takes them
// as it finds them.
func TestSinkWritesThroughANonBlockingSocket(t *testing.T) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	if err := syscall.SetNonblock(fds[0], true); err != nil {
		t.Fatalf("setting the socket non-blocking: %v", err)
	}
	client := os.NewFile(uintptr(fds[1]), "client")
	defer func() { _ = client.Close() }()
	sink := newPlainSink(t, fds[0])
	defer sink.close()

	// More than any socket buffer, so the write must wait for the reader at least
	// once rather than completing in one call.
	payload := bytes.Repeat([]byte("z"), 4<<20)
	done := make(chan error, 1)
	go func() { done <- sink.writeChunk(context.Background(), payload) }()

	got := 0
	buf := make([]byte, 64<<10)
	for got < len(payload) {
		n, err := client.Read(buf)
		got += n
		if err != nil {
			t.Fatalf("reading the response: %v", err)
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("writing through a non-blocking socket: %v", err)
	}
	if sink.conn.dirty.Load() {
		t.Error("a complete write marked the socket unusable")
	}
}

// An ad too big for one CEDAR frame goes out as a run of partial frames and a
// final one, because HTCondor's reader rejects anything larger in a single frame.
func TestOversizeAdIsSplitAcrossFrames(t *testing.T) {
	b := &adRecordBuilder{emit: true, cedarFrame: true}
	huge := strings.Repeat("q", 3<<20)
	raw := b.appendRecord(nil, "ClusterId = 1\nPad = \""+huge+"\"")

	frames := 0
	for rest := raw; len(rest) > 0; {
		n := int(binary.BigEndian.Uint32(rest[1:5]))
		if n > maxFrameBody {
			t.Fatalf("a frame carries %d bytes, more than HTCondor will read", n)
		}
		frames++
		rest = rest[cedarHeaderLen+n:]
	}
	if frames < 2 {
		t.Fatalf("a %d-byte ad went out in %d frame(s)", len(huge), frames)
	}
	msgs := decodeCedarFrames(t, raw)
	if len(msgs) != 1 {
		t.Fatalf("the split ad arrived as %d messages, want 1", len(msgs))
	}
	if exprs := adExprs(t, msgs[0]); len(exprs) != 2 {
		t.Fatalf("the reassembled ad has %d expressions, want 2", len(exprs))
	}
}

// The writer never closes the connection, only its own descriptor, so the
// caller can still finish the response on its own copy.
func TestSinkCloseLeavesTheConnectionUp(t *testing.T) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	caller := fds[0]
	defer func() { _ = syscall.Close(caller) }()
	dup, err := syscall.Dup(caller)
	if err != nil {
		t.Fatalf("dup: %v", err)
	}
	client := os.NewFile(uintptr(fds[1]), "client")
	defer func() { _ = client.Close() }()

	sink := newPlainSink(t, dup)
	if err := sink.writeChunk(context.Background(), []byte("ads")); err != nil {
		t.Fatalf("writing the ads: %v", err)
	}
	sink.close()

	// The caller appends on its own descriptor after the writer is gone.
	if _, err := syscall.Write(caller, []byte("|summary")); err != nil {
		t.Fatalf("the caller could not append after the writer closed: %v", err)
	}
	buf := make([]byte, 64)
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := client.Read(buf)
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	if string(buf[:n]) != "ads|summary" {
		t.Fatalf("the client saw %q, want the writer's bytes then the caller's", buf[:n])
	}
}

// The encrypted path, end to end against the CEDAR implementation itself: an
// established AES-GCM session is handed to the writer mid-stream, the writer
// puts the ads on it, the session's final position comes back, and the caller
// continues the same session with its own last message -- which is exactly what
// the schedd does with the summary ad.
func TestSinkResumesAndHandsBackAnEncryptedSession(t *testing.T) {
	ads := map[string]string{}
	for i := range 25 {
		ads[fmt.Sprintf("%d.0", i)] = fmt.Sprintf(
			"ClusterId = %d\nProcId = 0\nOwner = \"u%d\"\nJobStatus = %d\nJobUniverse = 5\nPad = \"%s\"",
			i, i%3, (i%5)+1, strings.Repeat("p", i*11))
	}
	cl := newTestServer(t, ads)
	want, _ := runStream(t, cl, "true", nil, -1, &adRecordBuilder{emit: true})

	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	server := &fdConn{fd: fds[0]}
	defer func() { _ = server.Close() }()
	clientFile := os.NewFile(uintptr(fds[1]), "client")
	clientConn, err := net.FileConn(clientFile)
	if err != nil {
		t.Fatalf("wrapping the client socket: %v", err)
	}
	_ = clientFile.Close()
	defer func() { _ = clientConn.Close() }()

	srv := stream.NewStream(server)
	cli := stream.NewStream(clientConn)
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("key: %v", err)
	}
	if err := srv.SetSymmetricKey(key); err != nil {
		t.Fatalf("server key: %v", err)
	}
	if err := cli.SetSymmetricKey(key); err != nil {
		t.Fatalf("client key: %v", err)
	}
	// Both directions past counter zero and past the handshake-digest frame,
	// which is where a schedd's session stands when a query arrives on it.
	exchange(t, server, srv, cli, []byte("server hello"))
	exchange(t, nil, cli, srv, []byte("client hello"))

	blob, err := srv.ExportCryptoState()
	if err != nil {
		t.Fatalf("exporting the session: %v", err)
	}

	dup, err := syscall.Dup(fds[0])
	if err != nil {
		t.Fatalf("dup: %v", err)
	}
	sink, err := newFDSink(dup, blob)
	if err != nil {
		t.Fatalf("resuming the session in the writer: %v", err)
	}
	if !sink.encrypted() {
		t.Fatal("a writer given a crypto-state blob is not encrypting")
	}
	r := &queueRequest{
		client: &queueClient{notifyFD: -1},
		chunks: make(chan queueChunk, 16),
		sink:   sink,
	}

	// Read the response on the client while the writer produces it.
	type result struct {
		exprs [][]string
		last  string
		err   error
	}
	done := make(chan result, 1)
	go func() {
		var out result
		for range len(want) {
			m := message.NewMessageFromStream(cli)
			text, rerr := m.GetClassAdRaw(context.Background())
			if rerr != nil {
				out.err = rerr
				done <- out
				return
			}
			var one []string
			for _, line := range strings.Split(text, "\n") {
				if line != "" {
					one = append(one, line)
				}
			}
			out.exprs = append(out.exprs, one)
		}
		tail, rerr := cli.ReceiveCompleteMessage(context.Background())
		out.last, out.err = string(tail), rerr
		done <- out
	}()

	r.wg.Add(1)
	r.stream(context.Background(), cl, dbrpc.DefaultTable, "true", nil, -1,
		&adRecordBuilder{emit: true}, true)
	if err := sink.finish(); err != nil {
		t.Fatalf("finishing the response: %v", err)
	}
	if r.err != nil {
		t.Fatalf("the response failed: %v", r.err)
	}
	if len(sink.final) == 0 {
		t.Fatal("a finished encrypted response left no session to resume")
	}

	// The caller continues the same session with its own last message, which is
	// only possible if the position handed back is right.
	resumed, err := stream.NewStreamWithCryptoState(server, sink.final)
	if err != nil {
		t.Fatalf("resuming the session in the caller: %v", err)
	}
	if err := resumed.SendMessage(context.Background(), []byte("summary ad")); err != nil {
		t.Fatalf("sending the caller's last message: %v", err)
	}
	if err := server.Flush(); err != nil {
		t.Fatalf("flushing the caller's last message: %v", err)
	}

	got := <-done
	if got.err != nil {
		t.Fatalf("the client could not read the response: %v", got.err)
	}
	if len(got.exprs) != len(want) {
		t.Fatalf("the client read %d ads, want %d", len(got.exprs), len(want))
	}
	for i := range want {
		// GetClassAdRaw appends MyType/TargetType lines of its own; compare the
		// expressions the writer sent, which are the leading ones.
		if len(got.exprs[i]) < len(want[i]) {
			t.Fatalf("ad %d: client read %d expressions, want at least %d", i, len(got.exprs[i]), len(want[i]))
		}
		for j := range want[i] {
			if got.exprs[i][j] != want[i][j] {
				t.Fatalf("ad %d expression %d: client read %q, chunked path gave %q",
					i, j, got.exprs[i][j], want[i][j])
			}
		}
	}
	if got.last != "summary ad" {
		t.Fatalf("after the hand-back the client read %q, want the caller's own message", got.last)
	}
}

// exchange sends one message and checks it arrives, to move a session past
// counter zero the way a real handshake does. buffered, when set, is the
// sender's fdConn, whose writes do not reach the socket until they are flushed.
func exchange(t *testing.T, buffered *fdConn, from, to *stream.Stream, msg []byte) {
	t.Helper()
	errc := make(chan error, 1)
	go func() {
		err := from.SendMessage(context.Background(), msg)
		if err == nil && buffered != nil {
			err = buffered.Flush()
		}
		errc <- err
	}()
	got, err := to.ReceiveFrame(context.Background())
	if err != nil {
		t.Fatalf("receiving: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("sending: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("got %q, sent %q", got, msg)
	}
}

// A blob the writer cannot make sense of must stop the query before anything is
// written, so the caller still owns an untouched socket and can answer another
// way. Silently writing the response in the clear on an encrypted session would
// be the alternative, and the client would read noise.
func TestSinkRefusesAnUnusableCryptoState(t *testing.T) {
	for _, tc := range []struct {
		name string
		blob []byte
	}{
		{"wrong magic", append([]byte("XXXX"), make([]byte, 120)...)},
		{"truncated", []byte("CDRX\x00\x01")},
		{"empty-but-present", []byte{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
			if err != nil {
				t.Fatalf("socketpair: %v", err)
			}
			defer func() { _ = syscall.Close(fds[1]) }()
			s, err := newFDSink(fds[0], tc.blob)
			if err == nil {
				s.close()
				t.Fatal("a writer accepted a crypto state it cannot have understood")
			}
			if s.encrypted() {
				t.Error("a writer that refused the state still thinks it is encrypting")
			}
			s.close()
		})
	}
}
