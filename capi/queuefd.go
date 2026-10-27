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
// What goes out is the response's bytes and nothing else: each ad as one complete
// CEDAR message frame (see queuewire.go), with no trailing ad of any kind. The
// response's final ad -- the schedd's summary, or an error ad -- stays the
// caller's to write on its own descriptor once this writer reports it is done.
// Keeping it there means the counters, the "MyName" and the error codes in that
// ad are produced by exactly the code that produced them before, rather than
// reimplemented here where the two could drift.
//
// That hand-back is only sound while the session is unencrypted. An encrypted
// CEDAR session has a per-frame counter and IV that advance with every frame
// written, so a writer here and a writer in the caller cannot alternate without
// the caller importing this stream's crypto state. The caller is expected to keep
// an encrypted query on its own path; nothing here can detect the difference.
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
	"errors"
	"io"
	"runtime/cgo"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

// fdPollTimeoutMS bounds how long a write to a descriptor with no room waits
// before the stop flag is re-read. It costs nothing in the normal case, where
// the socket is writable and poll is never reached, and it is what lets a
// shutdown get past a client that has stopped reading.
const fdPollTimeoutMS = 250

// errWriterStopped ends a response that was abandoned rather than one that
// failed: the session is going away, and nothing is left to deliver an error to.
var errWriterStopped = errors.New("response writing was stopped")

// errRestartAfterSend ends a query whose read had to start over after part of
// the response was already on the client's socket. The fallbacks that cause a
// restart all declare themselves before the first row, so this says the database
// changed format mid-stream -- a disagreement to report, never to paper over by
// sending the ads twice.
var errRestartAfterSend = errors.New("the database restarted the stream after the response had begun")

// fdSink is a query's response writer: the client's socket, and enough state for
// the caller to know afterwards whether it may still write to that socket itself.
//
// The descriptor is a duplicate of the caller's. Writing through a duplicate is
// what makes the ownership statable in one sentence -- each side closes the
// descriptor it holds, and neither closing one ends the connection -- and it
// leaves the caller able to append the response's final ad. The flip side is that
// dup duplicates the descriptor and not the open file description, so the two
// share status flags: nothing here touches them, and the write loop handles a
// blocking and a non-blocking socket alike.
type fdSink struct {
	fd int

	// stop asks the writer to put no more bytes on the wire. dirty records that
	// it stopped part way through a frame, which is the one outcome after which
	// the caller must not write to the socket at all: what is on it is half an ad.
	stop  atomic.Bool
	dirty atomic.Bool

	closeOnce atomic.Bool
}

// write puts all of p on the socket, or reports why it could not.
//
// p always ends on a frame boundary, so a write that returns nil leaves the
// stream at a point the caller may append to. A write that fails having put
// nothing out leaves it there too; one that fails part way does not, and says so.
func (s *fdSink) write(ctx context.Context, p []byte) error {
	wrote := 0
	fail := func(err error) error {
		if wrote > 0 {
			s.dirty.Store(true)
		}
		return err
	}
	for len(p) > 0 {
		if s.stop.Load() {
			return fail(errWriterStopped)
		}
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		n, err := syscall.Write(s.fd, p)
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
			fds := []unix.PollFd{{Fd: int32(s.fd), Events: unix.POLLOUT}}
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

// abort asks the writer to stop. It deliberately does not close or shut down the
// descriptor: the caller holds a duplicate of the same socket and is going to
// report the failure on it, and shutting the socket down here would take that
// away. A writer already blocked inside one write finishes or fails that write
// first, which is why the result says whether the stream was left mid-frame.
func (s *fdSink) abort() { s.stop.Store(true) }

func (s *fdSink) close() {
	if s.closeOnce.Swap(true) {
		return
	}
	_ = syscall.Close(s.fd)
}

// Starts a query whose ads this library writes to fd itself, rather than
// collecting them for the caller.
//
// The arguments up to flags are hcdb_queue_begin's, and mean the same. fd is a
// descriptor for the client's socket -- the caller's own duplicate, which this
// library closes when the response is finished -- positioned at a point where
// nothing of the response has been written yet. It must name an UNENCRYPTED
// CEDAR session: see the note at the top of this file.
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
func hcdb_queue_begin_fd(h C.uintptr_t, constraint, projection *C.char, limit C.longlong, flags C.int, fd C.int, err **C.char) C.uintptr_t {
	sink := &fdSink{fd: int(fd)}
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
			if r.sink != nil && !r.sink.dirty.Load() {
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
