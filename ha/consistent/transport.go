package consistent

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/bbockelm/cedar/stream"
	"github.com/hashicorp/raft"
)

// The raft NetworkTransport speaks a msgpack byte stream over a net.Conn. CEDAR
// is message-oriented and encrypted, so we tunnel raft's byte stream over CEDAR
// messages: each Write becomes one CEDAR message and Read reassembles the byte
// stream from messages. Because the transport wraps the conn in a bufio.Writer,
// Writes arrive already batched, keeping message overhead low. Tunneling through
// CEDAR (rather than the raw socket under it) is the whole point: raft
// replication then inherits HTCondor authentication and encryption.

var errLayerClosed = errors.New("consistent: raft stream layer closed")

// streamConn adapts a CEDAR stream to net.Conn for the raft transport.
type streamConn struct {
	s         *stream.Stream
	local     net.Addr
	remote    net.Addr
	closeOnce sync.Once
	done      chan struct{} // closed on Close, so a command handler can wait

	rmu  sync.Mutex
	rbuf []byte // leftover bytes from the last message

	dmu           sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

func newStreamConn(s *stream.Stream, local, remote net.Addr) *streamConn {
	return &streamConn{s: s, local: local, remote: remote, done: make(chan struct{})}
}

// Done is closed when the connection closes (raft is finished with it).
func (c *streamConn) Done() <-chan struct{} { return c.done }

func (c *streamConn) Read(p []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	if len(c.rbuf) == 0 {
		ctx, cancel := c.ctxFor(c.deadline(false))
		defer cancel()
		msg, err := c.s.ReceiveCompleteMessage(ctx)
		if err != nil {
			return 0, err
		}
		c.rbuf = msg
	}
	n := copy(p, c.rbuf)
	c.rbuf = c.rbuf[n:]
	return n, nil
}

func (c *streamConn) Write(p []byte) (int, error) {
	ctx, cancel := c.ctxFor(c.deadline(true))
	defer cancel()
	// Copy: SendMessage may retain p until the write completes, and the caller's
	// bufio buffer is reused after Write returns.
	buf := append([]byte(nil), p...)
	if err := c.s.SendMessage(ctx, buf); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *streamConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.done)
		err = c.s.Close()
	})
	return err
}

func (c *streamConn) LocalAddr() net.Addr  { return c.local }
func (c *streamConn) RemoteAddr() net.Addr { return c.remote }

func (c *streamConn) SetDeadline(t time.Time) error {
	c.dmu.Lock()
	c.readDeadline, c.writeDeadline = t, t
	c.dmu.Unlock()
	return nil
}

func (c *streamConn) SetReadDeadline(t time.Time) error {
	c.dmu.Lock()
	c.readDeadline = t
	c.dmu.Unlock()
	return nil
}

func (c *streamConn) SetWriteDeadline(t time.Time) error {
	c.dmu.Lock()
	c.writeDeadline = t
	c.dmu.Unlock()
	return nil
}

func (c *streamConn) deadline(write bool) time.Time {
	c.dmu.Lock()
	defer c.dmu.Unlock()
	if write {
		return c.writeDeadline
	}
	return c.readDeadline
}

// ctxFor builds a context honoring an absolute deadline (zero = no deadline).
func (c *streamConn) ctxFor(deadline time.Time) (context.Context, context.CancelFunc) {
	if deadline.IsZero() {
		return context.WithCancel(context.Background())
	}
	return context.WithDeadline(context.Background(), deadline)
}

// stringAddr is a trivial net.Addr wrapping a sinful/host:port string.
type stringAddr string

func (a stringAddr) Network() string { return "cedar" }
func (a stringAddr) String() string  { return string(a) }

// DialFunc opens a CEDAR connection to a peer's raft command and returns the
// established stream. The coordinator supplies it (it holds the client security
// config and the DBRaft command int).
type DialFunc func(ctx context.Context, addr string, timeout time.Duration) (*stream.Stream, error)

// StreamLayer is the raft StreamLayer over CEDAR. Incoming raft connections are
// delivered by the daemon's DBRaft command handler via Deliver; outgoing ones
// are dialed with the injected DialFunc.
type StreamLayer struct {
	advertise net.Addr
	dial      DialFunc

	accept    chan net.Conn
	closed    chan struct{}
	closeOnce sync.Once
}

// NewStreamLayer builds a stream layer that advertises the given address and
// dials peers with dial.
func NewStreamLayer(advertise string, dial DialFunc) *StreamLayer {
	return &StreamLayer{
		advertise: stringAddr(advertise),
		dial:      dial,
		accept:    make(chan net.Conn),
		closed:    make(chan struct{}),
	}
}

// Accept returns the next inbound raft connection.
func (l *StreamLayer) Accept() (net.Conn, error) {
	select {
	case c := <-l.accept:
		return c, nil
	case <-l.closed:
		return nil, errLayerClosed
	}
}

// Close stops the layer; pending and future Accepts fail.
func (l *StreamLayer) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

// Addr returns the advertised address.
func (l *StreamLayer) Addr() net.Addr { return l.advertise }

// Dial opens an outbound raft connection to address.
func (l *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	if l.dial == nil {
		return nil, errors.New("consistent: no dialer configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	s, err := l.dial(ctx, string(address), timeout)
	if err != nil {
		return nil, err
	}
	return newStreamConn(s, l.advertise, stringAddr(string(address))), nil
}

// DeliverWait hands an accepted CEDAR raft stream to the transport and returns a
// channel closed once raft is done with the connection. The DBRaft command
// handler waits on it (and the request context) so it does not return -- and let
// the server close the socket -- while raft is still using the stream.
func (l *StreamLayer) DeliverWait(s *stream.Stream) (<-chan struct{}, error) {
	conn := newStreamConn(s, l.advertise, stringAddr(s.GetPeerAddr()))
	select {
	case l.accept <- conn:
		return conn.Done(), nil
	case <-l.closed:
		_ = conn.Close()
		return nil, errLayerClosed
	}
}
