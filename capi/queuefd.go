// Writing a query's response to the client's socket from this library's own
// goroutine, instead of handing the bytes back for the caller to write.
//
// hcdb_queue_begin collects a result into chunks the caller copies to its client.
// That copying is cheap per ad, but in the condor_schedd it happens on the single
// thread that also runs every other thing the daemon does, so a handful of
// concurrent condor_q commands serialise behind each other and behind the rest of
// the event loop. The schedd's own job-queue path does not have this problem
// because it forks: the child burns MORE total CPU than the parent would, and
// wins anyway, because none of it lands on the event loop.
//
// hcdb_queue_begin_fd is the same trade without the fork. The caller hands over a
// duplicate of its client's descriptor at a point where nothing has been written
// to it, and a goroutine writes every ad. The caller's thread does nothing
// between dispatching the query and being told it is finished.
//
// What goes out is the response's ads and nothing else: no summary ad, no error
// ad, no end-of-stream of any kind. The response's final ad stays the caller's to
// write on its own descriptor once this writer reports it is done. Keeping it
// there means the counters, the "MyName" and the error codes in that ad are
// produced by exactly the code that produced them before, rather than
// reimplemented here where the two could drift.
//
// Two writers can take turns on one socket without ceremony while the session is
// in the clear, because each CEDAR frame is self-contained. An encrypted session
// advances a counter and an IV with every frame, so the caller passes the
// session's position in at the start (hcdb_queue_begin_fd's state argument) and
// reads it back out at the end (hcdb_queue_crypto_state) before writing anything
// itself.
package main

/*
#include <stdlib.h>
// stdint.h for uintptr_t: glibc does not pull it in via stdlib.h the way macOS does, so
// without this the package builds on a Mac and fails on Linux with "could not determine
// what C.uintptr_t refers to".
#include <stdint.h>
*/
import "C"

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime/cgo"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/stream"

	"golang.org/x/sys/unix"
)

// fdPollTimeoutMS bounds how long a write to a descriptor with no room waits
// before the stop flag is re-read. It costs nothing in the normal case, where
// the socket is writable and poll is never reached, and it is what lets a
// shutdown get past a client that has stopped reading.
const fdPollTimeoutMS = 250

// fdWriteBuffer is how much response accumulates before it goes to the socket.
// One ad is a couple of kilobytes and the protocol ends a message per ad, so
// writing each as it is framed would be a syscall per ad; the bytes on the wire
// are identical either way, only the number of write calls changes.
const fdWriteBuffer = 256 << 10

// errWriterStopped ends a response that was abandoned rather than one that
// failed: the session is going away, and nothing is left to deliver an error to.
var errWriterStopped = errors.New("response writing was stopped")

// errRestartAfterSend ends a query whose read had to start over after part of
// the response was already on the client's socket. The fallbacks that cause a
// restart all declare themselves before the first row, so this says the database
// changed format mid-stream -- a disagreement to report, never to paper over by
// sending the ads twice.
var errRestartAfterSend = errors.New("the database restarted the stream after the response had begun")

// errCryptoStateLost says an encrypted session's final position could not be
// described, so the caller cannot continue the session and must drop it.
var errCryptoStateLost = errors.New("the session's final crypto state could not be exported")

/*
fdConn is the client's socket as a net.Conn, with the response buffered on its
way out. Nothing reaches the socket until Flush, which callers do on a frame
boundary.

The descriptor is a duplicate of the caller's. Writing through a duplicate is
what makes the ownership statable in one sentence -- each side closes the
descriptor it holds, and neither closing one ends the connection -- and it
leaves the caller able to append the response's final ad. The flip side is that
dup duplicates the descriptor and not the open file description, so the two
share status flags. Nothing here touches them: the write loop handles a blocking
and a non-blocking socket alike. That is also why this is a hand-written net.Conn
and not one from net.FileConn, which would put the descriptor in Go's poller and
so make the CALLER's socket non-blocking too.

Deadlines are the one thing that is not real: setting one would touch the shared
descriptor, and the CEDAR stream layer does not set one on a send-only stream.
*/
type fdConn struct {
	fd  int
	buf []byte

	// stop asks the writer to put no more bytes on the wire. dirty records that
	// it stopped part way through a frame, which is the one outcome after which
	// the caller must not write to the socket at all: what is on it is half an ad.
	stop  atomic.Bool
	dirty atomic.Bool

	closed atomic.Bool
}

func (c *fdConn) Write(p []byte) (int, error) {
	if len(c.buf)+len(p) > fdWriteBuffer {
		if err := c.Flush(); err != nil {
			return 0, err
		}
	}
	if len(p) >= fdWriteBuffer {
		// Bigger than the buffer is worth holding: send it straight out. The
		// buffer was just drained, so ordering is preserved.
		if err := c.writeAll(p); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	c.buf = append(c.buf, p...)
	return len(p), nil
}

// Flush puts everything buffered on the socket. Callers flush on a frame
// boundary, so a Flush that returns nil leaves the stream at a point the caller
// may append to.
func (c *fdConn) Flush() error {
	if len(c.buf) == 0 {
		return nil
	}
	err := c.writeAll(c.buf)
	c.buf = c.buf[:0]
	return err
}

// writeAll puts all of p on the socket, or reports why it could not. A write
// that fails having put nothing out leaves the stream where it was; one that
// fails part way does not, and says so.
func (c *fdConn) writeAll(p []byte) error {
	wrote := 0
	fail := func(err error) error {
		if wrote > 0 {
			c.dirty.Store(true)
		}
		return err
	}
	for len(p) > 0 {
		if c.stop.Load() {
			return fail(errWriterStopped)
		}
		n, err := syscall.Write(c.fd, p)
		if n > 0 {
			wrote += n
			p = p[n:]
			continue
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			// A non-blocking socket with a full send buffer. Waiting for room beats
			// spinning, and the timeout is what lets stop be noticed while a client
			// is not reading.
			fds := []unix.PollFd{{Fd: int32(c.fd), Events: unix.POLLOUT}}
			if _, perr := unix.Poll(fds, fdPollTimeoutMS); perr != nil && !errors.Is(perr, syscall.EINTR) {
				return fail(perr)
			}
			continue
		}
		if err == nil {
			// write(2) returning zero without an error is not something a socket
			// does, but treating it as success would spin forever.
			err = io.ErrShortWrite
		}
		return fail(err)
	}
	return nil
}

func (c *fdConn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	return syscall.Close(c.fd)
}

// Read completes the net.Conn, and is here because a Conn that cannot be read
// is a trap for the next caller rather than a simplification. A response writer
// never uses it: it sends, and the client says nothing back.
func (c *fdConn) Read(p []byte) (int, error) {
	for {
		n, err := syscall.Read(c.fd, p)
		if n > 0 {
			return n, nil
		}
		if n == 0 && err == nil {
			return 0, io.EOF
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			fds := []unix.PollFd{{Fd: int32(c.fd), Events: unix.POLLIN}}
			if _, perr := unix.Poll(fds, fdPollTimeoutMS); perr != nil && !errors.Is(perr, syscall.EINTR) {
				return 0, perr
			}
			continue
		}
		return 0, err
	}
}

func (c *fdConn) LocalAddr() net.Addr              { return nil }
func (c *fdConn) RemoteAddr() net.Addr             { return nil }
func (c *fdConn) SetDeadline(time.Time) error      { return nil }
func (c *fdConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fdConn) SetWriteDeadline(time.Time) error { return nil }

// fdSink is a query's response writer.
type fdSink struct {
	conn *fdConn

	// cs is the caller's CEDAR session, resumed here, and is set only when that
	// session is encrypted. With it the ads are framed and encrypted here;
	// without it the record builder has already produced the frames and the
	// bytes go straight out.
	cs    *stream.Stream
	msg   *message.Message // reused across ads; it holds only the frame being built
	exprs [][]byte         // reused per-ad expression list, encrypted path only

	// final is where the session stands once the last ad is written, for the
	// caller to resume from. Empty on the unencrypted path, which has no such
	// position to lose.
	final []byte
}

func newFDSink(fd int, state []byte) (*fdSink, error) {
	s := &fdSink{conn: &fdConn{fd: fd, buf: make([]byte, 0, fdWriteBuffer)}}
	if len(state) == 0 {
		return s, nil
	}
	cs, err := stream.NewStreamWithCryptoState(s.conn, state)
	if err != nil {
		return s, err
	}
	s.cs = cs
	s.msg = message.NewMessageForStream(cs)
	return s, nil
}

func (s *fdSink) encrypted() bool { return s.cs != nil }

// writeChunk puts one chunk of the response on the socket. A chunk always holds
// whole ads, so it ends on a frame boundary either way.
func (s *fdSink) writeChunk(ctx context.Context, buf []byte) error {
	if s.cs == nil {
		// The record builder wrote CEDAR frames; these bytes are the response.
		if _, err := s.conn.Write(buf); err != nil {
			return err
		}
		return s.conn.Flush()
	}
	// Encrypted: each record holds one ad's expressions in the unencrypted
	// framing, and CEDAR length-prefixes a string on an encrypted channel, so
	// the ad is re-framed through the message layer rather than copied. Nothing
	// is parsed to do it -- the strings are walked, not read.
	// The frame writes get a context that cannot be cancelled: cancelling one
	// closes the connection underneath the stream, and stopping this writer is
	// the socket's own stop flag's job, which leaves the caller's copy intact.
	wctx := context.WithoutCancel(ctx)
	for off := 0; off < len(buf); {
		payload, next, ok := nextChunkRecord(buf, off)
		if !ok {
			return errors.New("truncated record while writing an encrypted response")
		}
		off = next
		if err := s.putEncryptedAd(wctx, payload); err != nil {
			return err
		}
	}
	return s.conn.Flush()
}

// nextChunkRecord returns the record beginning at off and where the next one
// starts, in the framing adRecordBuilder writes when it is not emitting CEDAR
// frames.
func nextChunkRecord(buf []byte, off int) (payload []byte, next int, ok bool) {
	if off+recordHeaderLen > len(buf) {
		return nil, 0, false
	}
	n := int(binary.LittleEndian.Uint32(buf[off:]))
	start := off + recordHeaderLen
	if start+n > len(buf) {
		return nil, 0, false
	}
	return buf[start : start+n], start + n, true
}

// putEncryptedAd writes one ad from its unencrypted-framing payload: an 8-byte
// big-endian expression count, then that many NUL-terminated expressions and the
// two trailing type strings.
func (s *fdSink) putEncryptedAd(ctx context.Context, payload []byte) error {
	if len(payload) < 8 {
		return errors.New("ad record too short to hold an expression count")
	}
	count := int(binary.BigEndian.Uint64(payload))
	s.exprs = s.exprs[:0]
	body := payload[8:]
	for range count {
		i := indexZero(body)
		if i < 0 {
			return errors.New("ad record ends part way through an expression")
		}
		s.exprs = append(s.exprs, body[:i])
		body = body[i+1:]
	}
	// The two trailing strings are always empty: HTCondor carries the real
	// MyType/TargetType as ordinary attributes in the body, and the builder
	// reproduces that.
	if err := s.msg.PutClassAdRawBytes(ctx, s.exprs, "", ""); err != nil {
		return err
	}
	return s.msg.FlushFrame(ctx, true)
}

func indexZero(b []byte) int {
	for i, c := range b {
		if c == 0 {
			return i
		}
	}
	return -1
}

// finish is called once the last ad is written: it records where an encrypted
// session now stands, so the caller can continue it, and gives up the socket.
func (s *fdSink) finish() error {
	err := s.conn.Flush()
	if s.cs != nil && err == nil && !s.conn.dirty.Load() {
		final, ferr := s.cs.ExportCryptoState()
		if ferr != nil {
			// The caller cannot write its last ad on a session whose position it
			// does not know, and must not guess: say the socket is unusable.
			s.conn.dirty.Store(true)
			err = errCryptoStateLost
		} else {
			s.final = final
		}
	}
	s.close()
	return err
}

// abort asks the writer to stop. It deliberately does not close or shut down the
// descriptor: the caller holds a duplicate of the same socket and is going to
// report the failure on it, and shutting the socket down here would take that
// away. A writer already blocked inside one write finishes or fails that write
// first, which is why the result says whether the stream was left mid-frame.
func (s *fdSink) abort() { s.conn.stop.Store(true) }

func (s *fdSink) close() { _ = s.conn.Close() }

// Starts a query whose ads this library writes to fd itself, rather than
// collecting them for the caller.
//
// The arguments up to flags are hcdb_queue_begin's, and mean the same. fd is a
// descriptor for the client's socket -- the caller's own duplicate, which this
// library closes when the response is finished -- positioned at a point where
// nothing of the response has been written yet. The library takes the descriptor
// on every return, successful or not.
//
// state describes an encrypted CEDAR session's current position, in the format
// the Go CEDAR implementation's ExportCryptoState writes; pass NULL and 0 for a
// session in the clear. When it is given, read the session's final position back
// with hcdb_queue_crypto_state before writing anything to the socket yourself.
//
// Nothing but the ads is written: no summary ad, no error ad, no end-of-stream of
// any kind. Ask hcdb_queue_result for the outcome and write the response's final
// ad on your own descriptor.
//
// Completion is announced on the session's notification descriptor exactly as it
// is for hcdb_queue_begin, with this request's handle as the token, and the
// handle is released with hcdb_queue_free either way.
//
//export hcdb_queue_begin_fd
func hcdb_queue_begin_fd(h C.uintptr_t, constraint, projection *C.char, limit C.longlong, flags C.int,
	fd C.int, state *C.uchar, state_len C.size_t, err **C.char) C.uintptr_t {

	var blob []byte
	if state != nil && state_len > 0 {
		blob = C.GoBytes(unsafe.Pointer(state), C.int(state_len))
	}
	sink, serr := newFDSink(int(fd), blob)
	if serr != nil {
		sink.close()
		*err = C.CString("resuming the client's encrypted session: " + serr.Error())
		return 0
	}
	rh := guardHandle("hcdb_queue_begin_fd", err, func() C.uintptr_t {
		return hcdb_queue_beginImpl(h, constraint, projection, limit, flags, sink, err)
	})
	if rh == 0 {
		// The descriptor is this library's on every return, so a query that did not
		// start -- including one a panic stopped -- still gives it back. close is
		// idempotent, so a failure that already closed it loses nothing here.
		sink.close()
	}
	return rh
}

// Reports a response-writing query's outcome.
//
// Returns HCDB_QUEUE_PENDING while the response is still going out, HCDB_OK when
// every matching ad reached the socket, or HCDB_ERR with the reason in *errout.
//
// *ads is how many ads were written. *clean says whether the socket is still at
// an ad boundary: 1 means the caller may write the response's final ad on its own
// descriptor, 0 means a write stopped part way through one and the caller must
// close the connection instead -- a client that loses its connection reports a
// failure, where a client handed half an ad followed by a well-formed summary
// would report a short queue and no error at all.
//
// Only meaningful for a request begun with hcdb_queue_begin_fd.
//
//export hcdb_queue_result
func hcdb_queue_result(rh C.uintptr_t, ads *C.longlong, clean *C.int, errout **C.char) C.int {
	return guardStatus("hcdb_queue_result", errout, func() C.int {
		r := handleQueueRequest(rh)
		if r == nil {
			*errout = C.CString("invalid query handle")
			return hcdbErr
		}
		if ads != nil {
			*ads = C.longlong(r.written.Load())
		}
		if clean != nil {
			*clean = 0
			if r.sink != nil && !r.sink.conn.dirty.Load() {
				*clean = 1
			}
		}
		if r.sink == nil {
			*errout = C.CString("this query is not writing its own response")
			return hcdbErr
		}
		if !r.closed.Load() {
			return hcdbQueuePending
		}
		r.mu.Lock()
		rerr := r.err
		r.mu.Unlock()
		if rerr != nil {
			if errout != nil {
				*errout = C.CString(rerr.Error())
			}
			return hcdbErr
		}
		return hcdbOK
	})
}

// Writes where a response-writing query left the client's encrypted session, in
// the format hcdb_queue_begin_fd's state argument takes, to *out (a C allocation
// the caller frees with hcdb_free) and its length to *length.
//
// Import it before writing anything to the socket: this library wrote frames on
// that session, and its counter and IV have moved.
//
// Returns HCDB_OK, or HCDB_ERR when the query was not encrypted, has not
// finished, or ended somewhere it could not describe -- in which case the
// session cannot be continued and the connection should be dropped.
//
//export hcdb_queue_crypto_state
func hcdb_queue_crypto_state(rh C.uintptr_t, out *unsafe.Pointer, length *C.size_t, errout **C.char) C.int {
	return guardStatus("hcdb_queue_crypto_state", errout, func() C.int {
		*out = nil
		*length = 0
		r := handleQueueRequest(rh)
		if r == nil || r.sink == nil {
			*errout = C.CString("invalid query handle")
			return hcdbErr
		}
		if !r.closed.Load() {
			*errout = C.CString("the response is still being written")
			return hcdbErr
		}
		if len(r.sink.final) == 0 {
			*errout = C.CString("this query left no resumable session")
			return hcdbErr
		}
		*out = C.CBytes(r.sink.final)
		*length = C.size_t(len(r.sink.final))
		return hcdbOK
	})
}

// detachSink releases a response-writing request without waiting for its writer.
//
// The writer may be blocked putting bytes on a socket whose client has stopped
// reading, and that write cannot be interrupted: the descriptor's flags are
// shared with the caller's own copy of the socket, and shutting the socket down
// would break the connection the caller is about to report the failure on. So the
// request is detached instead. The goroutine owns nothing shared -- its own
// descriptor and its own buffer -- so it can unwind in its own time, and it is
// told not to announce a completion for a handle that no longer exists.
func detachSink(r *queueRequest, rh C.uintptr_t) {
	r.detached.Store(true)
	r.sink.abort()
	r.cancel()
	// Drain, so a producer parked handing a chunk to a writer that is not coming
	// back sees the cancellation instead of waiting forever.
	go func() {
		for range r.chunks {
		}
	}()
	cgo.Handle(rh).Delete()
}
