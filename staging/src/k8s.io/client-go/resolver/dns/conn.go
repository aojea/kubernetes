package dns

import (
	"bytes"
	"io"
	"os"
	"sync"
	"time"
)

// packetHandlerFunc signature for the function used to process the connection packets
type packetHandlerFunc func(b []byte) []byte

// conn implements a synchronous, half-duplex in memory connection. Writes on
// the connection are processed by the packetHandler hook, if exist, and send
// back to the same connection.
type conn struct {
	wrMu sync.Mutex // Serialize Write operations
	rdMu sync.Mutex // Serialize Read operation

	readCh     chan []byte // Used to communicate Write and Read
	readBuffer bytes.Buffer

	once sync.Once // Protects closing the connection
	done chan struct{}

	readDeadline  connDeadline
	writeDeadline connDeadline

	packetHandler packetHandlerFunc // packet handler hook
}

func newConn(fn packetHandlerFunc) conn {
	return conn{
		readCh:        make(chan []byte, 1), // Serialize
		readBuffer:    bytes.Buffer{},
		done:          make(chan struct{}),
		readDeadline:  makeConnDeadline(),
		writeDeadline: makeConnDeadline(),
		packetHandler: fn,
	}
}

// connection parameters (same as net.Pipe)
// https://cs.opensource.google/go/go/+/refs/tags/go1.17:src/net/pipe.go;bpv=0;bpt=1

// connDeadline is an abstraction for handling timeouts.
type connDeadline struct {
	mu     sync.Mutex // Guards timer and cancel
	timer  *time.Timer
	cancel chan struct{} // Must be non-nil
}

func makeConnDeadline() connDeadline {
	return connDeadline{cancel: make(chan struct{})}
}

// set sets the point in time when the deadline will time out.
// A timeout event is signaled by closing the channel returned by waiter.
// Once a timeout has occurred, the deadline can be refreshed by specifying a
// t value in the future.
//
// A zero value for t prevents timeout.
func (c *connDeadline) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.timer != nil && !c.timer.Stop() {
		<-c.cancel // Wait for the timer callback to finish and close cancel
	}
	c.timer = nil

	// Time is zero, then there is no deadline.
	closed := isClosedChan(c.cancel)
	if t.IsZero() {
		if closed {
			c.cancel = make(chan struct{})
		}
		return
	}

	// Time in the future, setup a timer to cancel in the future.
	if dur := time.Until(t); dur > 0 {
		if closed {
			c.cancel = make(chan struct{})
		}
		c.timer = time.AfterFunc(dur, func() {
			close(c.cancel)
		})
		return
	}

	// Time in the past, so close immediately.
	if !closed {
		close(c.cancel)
	}
}

// wait returns a channel that is closed when the deadline is exceedec.
func (c *connDeadline) wait() chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cancel
}

func isClosedChan(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

func (c *conn) SetDeadline(t time.Time) error {
	if isClosedChan(c.done) {
		return io.ErrClosedPipe
	}
	c.readDeadline.set(t)
	c.writeDeadline.set(t)
	return nil
}

func (c *conn) SetReadDeadline(t time.Time) error {
	if isClosedChan(c.done) {
		return io.ErrClosedPipe
	}
	c.readDeadline.set(t)
	return nil
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	if isClosedChan(c.done) {
		return io.ErrClosedPipe
	}
	c.writeDeadline.set(t)
	return nil
}

func (c *conn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

func (c *conn) Read(b []byte) (n int, err error) {
	c.rdMu.Lock()
	defer c.rdMu.Unlock()

	if len(b) == 0 {
		return 0, io.EOF
	}

	switch {
	case isClosedChan(c.done):
		return 0, io.ErrClosedPipe
	case isClosedChan(c.readDeadline.wait()):
		return 0, os.ErrDeadlineExceeded
	}

	// if the buffer was drained wait for new data
	if c.readBuffer.Len() == 0 {
		select {
		case <-c.done:
			return 0, io.EOF
		case <-c.readDeadline.wait():
			return 0, os.ErrDeadlineExceeded
		case bw := <-c.readCh:
			c.readBuffer.Write(bw)
		}
	}
	return c.readBuffer.Read(b)
}

func (c *conn) Write(b []byte) (n int, err error) {
	switch {
	case isClosedChan(c.done):
		return 0, io.ErrClosedPipe
	case isClosedChan(c.writeDeadline.wait()):
		return 0, os.ErrDeadlineExceeded
	}

	// ensure the buffer is processed together
	c.wrMu.Lock()
	defer c.wrMu.Unlock()
	buf := make([]byte, len(b))
	nr := copy(buf, b)

	select {
	case <-c.done:
		return n, io.ErrClosedPipe
	case <-c.writeDeadline.wait():
		return n, os.ErrDeadlineExceeded
	case c.readCh <- c.handler()(buf):
		return nr, nil
	}

}

func (c *conn) handler() packetHandlerFunc {
	if c.packetHandler != nil {
		return c.packetHandler
	}
	// if no packet handler function was defined the connection
	// sends back the packets directly
	return func(b []byte) []byte { return b }
}
