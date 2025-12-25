package tunnel

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orris-inc/orris-client/internal/logger"
)

// TLSClient is a TLS tunnel client for Entry agents.
// It connects to an Exit agent and forwards data through the tunnel.
type TLSClient struct {
	endpointMu sync.RWMutex
	endpoint   string // host:port format
	token      string
	ruleID     string
	conn       net.Conn
	started    atomic.Bool // prevents duplicate Start() calls
	connected  atomic.Bool // tracks actual connection state

	writeMu   sync.Mutex
	handlerMu sync.RWMutex
	handler   DataHandler

	backoff              *Backoff
	heartbeatInterval    time.Duration
	refreshAfterAttempts int // refresh endpoint after this many failed reconnect attempts
	initialRetryMax      int // max retries for initial connection (0 = no retry)
	endpointRefresher    EndpointRefresher

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// TLSClientOption configures TLSClient.
type TLSClientOption func(*TLSClient)

// WithTLSBackoff sets the backoff configuration for reconnection.
func WithTLSBackoff(b *Backoff) TLSClientOption {
	return func(c *TLSClient) {
		c.backoff = b
	}
}

// WithTLSHeartbeatInterval sets the heartbeat interval.
func WithTLSHeartbeatInterval(d time.Duration) TLSClientOption {
	return func(c *TLSClient) {
		c.heartbeatInterval = d
	}
}

// WithTLSEndpointRefresher sets the endpoint refresher callback.
// When reconnection fails after refreshAfterAttempts, the refresher is called
// to get a new endpoint (e.g., when the exit agent restarts with a new port).
func WithTLSEndpointRefresher(refresher EndpointRefresher, refreshAfterAttempts int) TLSClientOption {
	return func(c *TLSClient) {
		c.endpointRefresher = refresher
		c.refreshAfterAttempts = refreshAfterAttempts
	}
}

// WithTLSInitialRetry sets the maximum number of retries for initial connection.
// If set to 0 (default), initial connection failure returns error immediately.
// If set to > 0, will retry with backoff and endpoint refresh before failing.
func WithTLSInitialRetry(maxRetries int) TLSClientOption {
	return func(c *TLSClient) {
		c.initialRetryMax = maxRetries
	}
}

// NewTLSClient creates a new TLS tunnel client.
// endpoint should be in "host:port" format.
func NewTLSClient(endpoint, token, ruleID string, opts ...TLSClientOption) *TLSClient {
	c := &TLSClient{
		endpoint:          endpoint,
		token:             token,
		ruleID:            ruleID,
		backoff:           DefaultBackoff(),
		heartbeatInterval: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// SetHandler sets the data handler.
// Thread-safe: can be called before or during Start.
func (c *TLSClient) SetHandler(h DataHandler) {
	c.handlerMu.Lock()
	c.handler = h
	c.handlerMu.Unlock()
}

// Start starts the TLS tunnel client with auto-reconnect.
// Returns error if already started.
func (c *TLSClient) Start(ctx context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return fmt.Errorf("tls client already started")
	}

	c.ctx, c.cancel = context.WithCancel(ctx)

	if err := c.connectWithRetry(); err != nil {
		c.started.Store(false) // allow retry after failure
		return fmt.Errorf("initial connection failed: %w", err)
	}

	c.wg.Add(2)
	go c.readLoop()
	go c.heartbeatLoop()

	return nil
}

// Stop stops the TLS tunnel client.
func (c *TLSClient) Stop() error {
	if c.cancel != nil {
		c.cancel()
	}

	// Mark connection as disconnected before closing
	c.connected.Store(false)

	// Hold write lock before closing to prevent concurrent write
	c.writeMu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.writeMu.Unlock()

	c.wg.Wait()
	logger.Info("tls tunnel client stopped")
	return nil
}

// IsConnected returns true if the tunnel is connected.
// It uses atomic operation to check the connection state without holding locks,
// providing thread-safe and accurate connection status.
func (c *TLSClient) IsConnected() bool {
	return c.connected.Load()
}
