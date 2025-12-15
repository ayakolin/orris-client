package tunnel

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/orris-inc/orris-client/internal/logger"
)

// DataHandler handles data received from tunnel.
type DataHandler interface {
	HandleData(connID uint64, data []byte)
	HandleClose(connID uint64)
}

// EndpointRefresher refreshes the tunnel endpoint when reconnection fails.
// It returns the new endpoint URL and token, or an error if refresh fails.
type EndpointRefresher func() (endpoint, token string, err error)

// Client is a WebSocket tunnel client for Entry agents.
// It connects to an Exit agent and forwards data through the tunnel.
type Client struct {
	endpointMu sync.RWMutex
	endpoint   string
	token      string
	ruleID     string
	conn       *websocket.Conn
	connected  atomic.Bool // tracks actual connection state

	writeMu sync.Mutex
	handler DataHandler

	backoff              *Backoff
	heartbeatInterval    time.Duration
	refreshAfterAttempts int // refresh endpoint after this many failed reconnect attempts
	initialRetryMax      int // max retries for initial connection (0 = no retry)
	endpointRefresher    EndpointRefresher

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// ClientOption configures Client.
type ClientOption func(*Client)

// WithBackoff sets the backoff configuration for reconnection.
func WithBackoff(b *Backoff) ClientOption {
	return func(c *Client) {
		c.backoff = b
	}
}

// WithHeartbeatInterval sets the heartbeat interval.
func WithHeartbeatInterval(d time.Duration) ClientOption {
	return func(c *Client) {
		c.heartbeatInterval = d
	}
}

// WithEndpointRefresher sets the endpoint refresher callback.
// When reconnection fails after refreshAfterAttempts, the refresher is called
// to get a new endpoint (e.g., when the exit agent restarts with a new port).
func WithEndpointRefresher(refresher EndpointRefresher, refreshAfterAttempts int) ClientOption {
	return func(c *Client) {
		c.endpointRefresher = refresher
		c.refreshAfterAttempts = refreshAfterAttempts
	}
}

// WithInitialRetry sets the maximum number of retries for initial connection.
// If set to 0 (default), initial connection failure returns error immediately.
// If set to > 0, will retry with backoff and endpoint refresh before failing.
func WithInitialRetry(maxRetries int) ClientOption {
	return func(c *Client) {
		c.initialRetryMax = maxRetries
	}
}

// NewClient creates a new tunnel client.
func NewClient(endpoint, token, ruleID string, opts ...ClientOption) *Client {
	c := &Client{
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
func (c *Client) SetHandler(h DataHandler) {
	c.handler = h
}

// Start starts the tunnel client with auto-reconnect.
func (c *Client) Start(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)

	if err := c.connectWithRetry(); err != nil {
		return fmt.Errorf("initial connection failed: %w", err)
	}

	c.wg.Add(2)
	go c.readLoop()
	go c.heartbeatLoop()

	return nil
}

// Stop stops the tunnel client.
func (c *Client) Stop() error {
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
	logger.Info("tunnel client stopped")
	return nil
}

// IsConnected returns true if the tunnel is connected.
// It uses atomic operation to check the connection state without holding locks,
// providing thread-safe and accurate connection status.
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}
