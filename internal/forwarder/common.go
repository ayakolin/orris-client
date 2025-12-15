package forwarder

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// isClosedError checks if the error is due to closed connection.
func isClosedError(err error) bool {
	return errors.Is(err, net.ErrClosed)
}

// bufferSize is the size of the buffer used for copying data.
// Using 64KB to match WebSocket buffer size for better throughput.
const bufferSize = 64 * 1024

// UDP constants
const (
	udpMaxPacketSize   = 65535
	udpIdleTimeout     = 2 * time.Minute
	udpCleanupInterval = 30 * time.Second
)

// trafficWriter wraps an io.Writer to count bytes written.
// This allows using io.Copy which can leverage splice(2) for zero-copy.
type trafficWriter struct {
	w         io.Writer
	trafficFn func(int64)
}

func (tw *trafficWriter) Write(p []byte) (n int, err error) {
	n, err = tw.w.Write(p)
	if n > 0 && tw.trafficFn != nil {
		tw.trafficFn(int64(n))
	}
	return
}

// copyWithTraffic copies data from src to dst using io.Copy.
// On Linux, this leverages splice(2) for zero-copy when both src and dst are sockets,
// which provides better backpressure propagation in multi-hop forwarding chains.
func copyWithTraffic(dst io.Writer, src io.Reader, trafficFn func(int64)) (int64, error) {
	tw := &trafficWriter{w: dst, trafficFn: trafficFn}
	return io.Copy(tw, src)
}

// bufPool is a pool of buffers used for copying data to reduce GC pressure.
var bufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, bufferSize)
		return &buf
	},
}

// copyBuffer copies data from src to dst using a pooled buffer.
// It calls trafficFn with the number of bytes written after each write.
// It respects context cancellation by checking ctx.Done() between reads.
// NOTE: For multi-hop forwarding chains, consider using copyWithTraffic instead
// for better backpressure propagation via splice(2).
func copyBuffer(ctx context.Context, dst io.Writer, src io.Reader, trafficFn func(int64)) (int64, error) {
	bufp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufp)

	var written int64
	for {
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}

		nr, rerr := src.Read(*bufp)
		if nr > 0 {
			nw, werr := dst.Write((*bufp)[:nr])
			if nw > 0 {
				written += int64(nw)
				if trafficFn != nil {
					trafficFn(int64(nw))
				}
			}
			if werr != nil {
				return written, werr
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				return written, nil
			}
			return written, rerr
		}
	}
}

// Forwarder is an interface for all forwarder types.
type Forwarder interface {
	Start(ctx context.Context) error
	Stop() error
	Traffic() *TrafficCounter
	RuleID() string
}

// writeQueueSize is the buffer size for async write queue.
// Large enough to absorb bursts without blocking the tunnel read loop.
// 2048 entries should handle most speed test scenarios.
const writeQueueSize = 2048

// maxBatchSize is the maximum number of buffers to batch in a single writev call.
const maxBatchSize = 64

// ErrQueueFull is returned when the write queue is full.
var ErrQueueFull = errors.New("write queue full")

// connState manages async write queue for a connection.
// It completely decouples the tunnel read loop from connection writes.
// The tunnel read loop dispatches data to the queue (non-blocking),
// and an independent goroutine processes the queue.
type connState struct {
	conn      net.Conn
	queue     chan []byte
	closed    atomic.Bool
	closeOnce sync.Once
	trafficFn func(int64)
}

// newConnState creates a new connection state with async write loop.
func newConnState(conn net.Conn, trafficFn func(int64)) *connState {
	cs := &connState{
		conn:      conn,
		queue:     make(chan []byte, writeQueueSize),
		trafficFn: trafficFn,
	}
	go cs.writeLoop()
	return cs
}

// writeLoop processes the write queue with batch optimization.
// It uses writev (via net.Buffers) to combine multiple small writes
// into a single system call for better performance.
func (cs *connState) writeLoop() {
	bufs := make(net.Buffers, 0, maxBatchSize)

	for {
		// Wait for first data
		data, ok := <-cs.queue
		if !ok {
			return
		}
		bufs = append(bufs, data)

		// Non-blocking batch: collect more data if available
	drain:
		for len(bufs) < maxBatchSize {
			select {
			case data, ok := <-cs.queue:
				if !ok {
					break drain
				}
				bufs = append(bufs, data)
			default:
				break drain
			}
		}

		// Batch write using writev
		n, err := bufs.WriteTo(cs.conn)
		if cs.trafficFn != nil && n > 0 {
			cs.trafficFn(n)
		}
		if err != nil {
			cs.closed.Store(true)
			return
		}

		// Reset for next batch
		bufs = bufs[:0]
	}
}

// Write queues data for async write. Non-blocking to prevent head-of-line blocking.
// With a large queue (2048), this should handle most burst scenarios.
func (cs *connState) Write(data []byte) error {
	if cs.closed.Load() {
		return net.ErrClosed
	}

	// Copy data since the original buffer may be reused
	buf := make([]byte, len(data))
	copy(buf, data)

	// Non-blocking send - never block the tunnel read loop
	select {
	case cs.queue <- buf:
		return nil
	default:
		return ErrQueueFull
	}
}

// Close closes the connection and write queue.
func (cs *connState) Close() {
	cs.closeOnce.Do(func() {
		cs.closed.Store(true)
		close(cs.queue)
		cs.conn.Close()
	})
}

// IsClosed returns true if the connection is closed.
func (cs *connState) IsClosed() bool {
	return cs.closed.Load()
}

// TrafficCounter tracks upload and download bytes.
type TrafficCounter struct {
	uploadBytes   atomic.Int64
	downloadBytes atomic.Int64
}

// AddUpload adds to upload bytes counter.
func (t *TrafficCounter) AddUpload(n int64) {
	t.uploadBytes.Add(n)
}

// AddDownload adds to download bytes counter.
func (t *TrafficCounter) AddDownload(n int64) {
	t.downloadBytes.Add(n)
}

// GetAndReset returns current values and resets counters.
func (t *TrafficCounter) GetAndReset() (upload, download int64) {
	upload = t.uploadBytes.Swap(0)
	download = t.downloadBytes.Swap(0)
	return
}

// udpClient tracks UDP client state for bidirectional forwarding.
type udpClient struct {
	clientAddr     *net.UDPAddr
	upstream       *net.UDPConn
	lastActiveNano atomic.Int64 // Unix nanoseconds, safe for concurrent access
}
