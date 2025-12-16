package forwarder

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orris-inc/orris-client/internal/logger"
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

// ReadFrom implements io.ReaderFrom to enable zero-copy optimization.
// When both src and dst are TCP sockets, Go's runtime uses splice(2) on Linux
// to transfer data directly in kernel space without copying to userspace.
// This provides significant performance improvements for high-throughput scenarios.
func (tw *trafficWriter) ReadFrom(r io.Reader) (n int64, err error) {
	// Delegate to underlying writer's ReadFrom if available (e.g., *net.TCPConn)
	if rf, ok := tw.w.(io.ReaderFrom); ok {
		n, err = rf.ReadFrom(r)
		if n > 0 && tw.trafficFn != nil {
			tw.trafficFn(n)
		}
		return
	}

	// Fallback: underlying writer doesn't support ReadFrom
	// This happens for non-TCP connections (e.g., TLS, pipes, files)
	return io.Copy(tw.w, r)
}

// isZeroCopySupported checks if zero-copy is supported for the given connection.
// Zero-copy via splice(2) requires both connections to be TCP sockets.
// Returns true if both dst and src are *net.TCPConn, false otherwise.
func isZeroCopySupported(dst io.Writer, src io.Reader) bool {
	_, dstIsTCP := dst.(*net.TCPConn)
	_, srcIsTCP := src.(*net.TCPConn)
	return dstIsTCP && srcIsTCP
}

// copyWithTraffic copies data from src to dst using io.Copy with context support.
// On Linux, this leverages splice(2) for zero-copy when both src and dst are TCP sockets,
// which provides better backpressure propagation in multi-hop forwarding chains.
// The trafficWriter's ReadFrom implementation enables this optimization automatically.
//
// Context cancellation is handled by relying on connection closure from the caller.
// When ctx is cancelled, the caller should close the connections, which will cause
// io.Copy to return with an error. This approach maintains zero-copy performance
// while still supporting graceful shutdown.
func copyWithTraffic(ctx context.Context, dst io.Writer, src io.Reader, trafficFn func(int64)) (int64, error) {
	// Check context before starting
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	tw := &trafficWriter{w: dst, trafficFn: trafficFn}

	// Log when zero-copy is expected (debug mode only)
	if isZeroCopySupported(dst, src) {
		logger.Debug("zero-copy enabled for TCP socket transfer")
	}

	return io.Copy(tw, src)
}

// bufPool is a pool of buffers used for copying data to reduce GC pressure.
var bufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, bufferSize)
		return &buf
	},
}

// Forwarder is an interface for all forwarder types.
type Forwarder interface {
	Start(ctx context.Context) error
	Stop() error
	Traffic() *TrafficCounter
	RuleID() string
}

// writeQueueSize is the buffer size for async write queue.
// Large enough to absorb bursts while providing backpressure.
// Increased from 2048 to 4096 to handle slow client scenarios.
const writeQueueSize = 4096

// maxBatchSize is the maximum number of buffers to batch in a single writev call.
// Increased from 64 to 128 to better handle small packet scenarios (e.g., Shadowsocks).
const maxBatchSize = 128

// writeTimeout is the maximum time to wait for queue space when full.
// This provides backpressure to upstream without immediately failing.
const writeTimeout = 5 * time.Second

// ErrQueueFull is returned when the write queue is full.
var ErrQueueFull = errors.New("write queue full")

// pooledBuffer holds buffer data for async write queue.
// Buffers are obtained from writeBufferPool and returned after write.
type pooledBuffer struct {
	data []byte
}

// writeBufferPool is a pool for async write buffers.
// This reduces memory allocations in the async write queue path.
var writeBufferPool = sync.Pool{
	New: func() any {
		return &pooledBuffer{
			data: make([]byte, 0, bufferSize),
		}
	},
}

// connState manages async write queue for a connection.
// It completely decouples the tunnel read loop from connection writes.
// The tunnel read loop dispatches data to the queue (non-blocking),
// and an independent goroutine processes the queue.
type connState struct {
	conn      net.Conn
	queue     chan *pooledBuffer
	closed    atomic.Bool
	closeOnce sync.Once
	trafficFn func(int64)
}

// newConnState creates a new connection state with async write loop.
func newConnState(conn net.Conn, trafficFn func(int64)) *connState {
	cs := &connState{
		conn:      conn,
		queue:     make(chan *pooledBuffer, writeQueueSize),
		trafficFn: trafficFn,
	}
	go cs.writeLoop()
	return cs
}

// writeLoop processes the write queue with batch optimization.
// It uses writev (via net.Buffers) to combine multiple small writes
// into a single system call for better performance.
// Buffers are returned to the pool after successful write.
func (cs *connState) writeLoop() {
	bufs := make(net.Buffers, 0, maxBatchSize)
	pbs := make([]*pooledBuffer, 0, maxBatchSize)

	for {
		// Wait for first data
		pb, ok := <-cs.queue
		if !ok {
			return
		}
		bufs = append(bufs, pb.data)
		pbs = append(pbs, pb)

		// Non-blocking batch: collect more data if available
	drain:
		for len(bufs) < maxBatchSize {
			select {
			case pb, ok := <-cs.queue:
				if !ok {
					break drain
				}
				bufs = append(bufs, pb.data)
				pbs = append(pbs, pb)
			default:
				break drain
			}
		}

		// Batch write using writev
		n, err := bufs.WriteTo(cs.conn)
		if cs.trafficFn != nil && n > 0 {
			cs.trafficFn(n)
		}

		// Return all buffers to pool after write
		for _, pb := range pbs {
			writeBufferPool.Put(pb)
		}

		if err != nil {
			cs.closed.Store(true)
			return
		}

		// Reset for next batch
		bufs = bufs[:0]
		pbs = pbs[:0]
	}
}

// Write queues data for async write with timeout-based backpressure.
// When queue is full, it waits up to writeTimeout before failing.
// This prevents immediate connection closure during slow client scenarios.
// Uses pooled buffers to reduce memory allocations.
func (cs *connState) Write(data []byte) error {
	if cs.closed.Load() {
		return net.ErrClosed
	}

	// Get buffer from pool
	pb := writeBufferPool.Get().(*pooledBuffer)
	if cap(pb.data) < len(data) {
		pb.data = make([]byte, len(data))
	}
	pb.data = pb.data[:len(data)]
	copy(pb.data, data)

	// Blocking send with timeout - provides backpressure
	select {
	case cs.queue <- pb:
		return nil
	case <-time.After(writeTimeout):
		// Timeout waiting for queue space
		writeBufferPool.Put(pb)
		return ErrQueueFull
	}
}

// Close closes the connection and write queue.
// Drains remaining buffers and returns them to the pool.
func (cs *connState) Close() {
	cs.closeOnce.Do(func() {
		cs.closed.Store(true)
		close(cs.queue)
		// Drain remaining buffers and return to pool
		for pb := range cs.queue {
			writeBufferPool.Put(pb)
		}
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
