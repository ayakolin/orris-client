package tunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	tls "github.com/refraction-networking/utls"
	"github.com/xtaci/smux"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
)

// SmuxClient is a SMUX tunnel client for Entry agents.
// It establishes a single connection (TLS or WebSocket) and multiplexes streams over it.
type SmuxClient struct {
	endpointMu sync.RWMutex
	endpoint   string // host:port for TLS, wss://host:port/path for WS
	token      string
	ruleID     string
	useTLS     bool // true for tls_smux, false for ws_smux

	conn      net.Conn      // underlying connection (TLS conn or wsConn)
	session   *smux.Session // SMUX session over the connection
	sessionMu sync.RWMutex  // protects session

	started   atomic.Bool
	connected atomic.Bool

	smuxConfig           *SmuxConfig
	backoff              *Backoff
	refreshAfterAttempts int
	initialRetryMax      int
	endpointRefresher    EndpointRefresher

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// SmuxClientOption configures SmuxClient.
type SmuxClientOption func(*SmuxClient)

// WithSmuxConfig sets the SMUX configuration.
func WithSmuxConfig(cfg *SmuxConfig) SmuxClientOption {
	return func(c *SmuxClient) {
		c.smuxConfig = cfg
	}
}

// WithSmuxBackoff sets the backoff configuration.
func WithSmuxBackoff(b *Backoff) SmuxClientOption {
	return func(c *SmuxClient) {
		c.backoff = b
	}
}

// WithSmuxEndpointRefresher sets the endpoint refresher callback.
func WithSmuxEndpointRefresher(refresher EndpointRefresher, refreshAfterAttempts int) SmuxClientOption {
	return func(c *SmuxClient) {
		c.endpointRefresher = refresher
		c.refreshAfterAttempts = refreshAfterAttempts
	}
}

// WithSmuxInitialRetry sets the maximum retries for initial connection.
func WithSmuxInitialRetry(maxRetries int) SmuxClientOption {
	return func(c *SmuxClient) {
		c.initialRetryMax = maxRetries
	}
}

// NewSmuxClient creates a new SMUX tunnel client.
// For TLS: endpoint is "host:port"
// For WS: endpoint is "wss://host:port/tunnel"
func NewSmuxClient(endpoint, token, ruleID string, useTLS bool, opts ...SmuxClientOption) *SmuxClient {
	c := &SmuxClient{
		endpoint:   endpoint,
		token:      token,
		ruleID:     ruleID,
		useTLS:     useTLS,
		smuxConfig: DefaultSmuxConfig(),
		backoff:    DefaultBackoff(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Start starts the SMUX client and establishes the underlying connection.
func (c *SmuxClient) Start(ctx context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return fmt.Errorf("smux client already started")
	}

	c.ctx, c.cancel = context.WithCancel(ctx)

	if err := c.connectWithRetry(); err != nil {
		c.started.Store(false)
		return fmt.Errorf("initial connection failed: %w", err)
	}

	// Start session monitor goroutine to handle reconnection
	c.wg.Add(1)
	go c.sessionMonitor()

	return nil
}

// Stop stops the SMUX client.
func (c *SmuxClient) Stop() error {
	if c.cancel != nil {
		c.cancel()
	}

	c.connected.Store(false)

	c.sessionMu.Lock()
	if c.session != nil {
		c.session.Close()
		c.session = nil
	}
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.sessionMu.Unlock()

	c.wg.Wait()
	logger.Info("smux tunnel client stopped")
	return nil
}

// IsConnected returns true if the SMUX session is established.
func (c *SmuxClient) IsConnected() bool {
	return c.connected.Load()
}

// OpenStream opens a new multiplexed stream.
func (c *SmuxClient) OpenStream() (net.Conn, error) {
	c.sessionMu.RLock()
	session := c.session
	c.sessionMu.RUnlock()

	if session == nil {
		return nil, fmt.Errorf("smux session not connected")
	}

	stream, err := session.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}

	return stream, nil
}

// connectWithRetry attempts connection with retries and endpoint refresh.
func (c *SmuxClient) connectWithRetry() error {
	err := c.connect()
	if err == nil {
		return nil
	}

	if c.initialRetryMax <= 0 {
		return err
	}

	logger.Warn("initial smux connection failed, will retry", "error", err, "max_retries", c.initialRetryMax)

	for attempt := 1; attempt <= c.initialRetryMax; attempt++ {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		if c.endpointRefresher != nil && c.refreshAfterAttempts > 0 &&
			attempt%c.refreshAfterAttempts == 0 {
			logger.Info("refreshing smux endpoint before retry", "attempt", attempt)
			if newEndpoint, newToken, refreshErr := c.endpointRefresher(); refreshErr != nil {
				logger.Error("smux endpoint refresh failed", "error", refreshErr)
			} else {
				c.endpointMu.Lock()
				if newEndpoint != c.endpoint {
					logger.Info("smux endpoint updated", "old", c.endpoint, "new", newEndpoint)
					c.endpoint = newEndpoint
					c.token = newToken
				}
				c.endpointMu.Unlock()
			}
		}

		interval := c.backoff.Next()
		logger.Info("retrying initial smux connection",
			"attempt", attempt,
			"max_retries", c.initialRetryMax,
			"interval", interval.Round(time.Millisecond))

		timer := time.NewTimer(interval)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return c.ctx.Err()
		case <-timer.C:
		}

		err = c.connect()
		if err == nil {
			c.backoff.Reset()
			logger.Info("initial smux connection succeeded after retries", "attempts", attempt)
			return nil
		}

		logger.Warn("initial smux connection retry failed", "attempt", attempt, "error", err)
	}

	return fmt.Errorf("max retries (%d) exceeded: %w", c.initialRetryMax, err)
}

// connect establishes the underlying connection and SMUX session.
func (c *SmuxClient) connect() error {
	c.endpointMu.RLock()
	endpoint := c.endpoint
	token := c.token
	ruleID := c.ruleID
	useTLS := c.useTLS
	c.endpointMu.RUnlock()

	tunnelType := "ws_smux"
	if useTLS {
		tunnelType = "tls_smux"
	}
	logger.Info("connecting to exit agent via smux", "endpoint", endpoint, "rule_id", ruleID, "type", tunnelType)

	var conn net.Conn
	var err error

	if useTLS {
		conn, err = c.dialTLS(endpoint)
	} else {
		conn, err = c.dialWS(endpoint, token)
	}
	if err != nil {
		return err
	}

	// Perform tunnel handshake
	if err := c.performHandshake(conn, token, ruleID); err != nil {
		conn.Close()
		return err
	}

	// Create SMUX session as client
	session, err := smux.Client(conn, c.smuxConfig.ToSmuxConfig())
	if err != nil {
		conn.Close()
		return fmt.Errorf("create smux session: %w", err)
	}

	// Update session
	c.sessionMu.Lock()
	oldSession := c.session
	oldConn := c.conn
	c.session = session
	c.conn = conn
	c.sessionMu.Unlock()

	c.connected.Store(true)

	// Close old session/conn if exists (during reconnect)
	if oldSession != nil {
		oldSession.Close()
	}
	if oldConn != nil {
		oldConn.Close()
	}

	logger.Info("smux tunnel established", "rule_id", ruleID)
	return nil
}

// dialTLS establishes a TLS connection.
func (c *SmuxClient) dialTLS(endpoint string) (net.Conn, error) {
	dialCtx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()

	var d net.Dialer
	tcpConn, err := d.DialContext(dialCtx, "tcp", endpoint)
	if err != nil {
		return nil, fmt.Errorf("dial tcp: %w", err)
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.UClient(tcpConn, tlsConfig, tls.HelloChrome_Auto)

	if err := tlsConn.Handshake(); err != nil {
		tcpConn.Close()
		return nil, fmt.Errorf("tls handshake: %w", err)
	}

	return tlsConn, nil
}

// dialWS establishes a WebSocket connection and wraps it as net.Conn.
func (c *SmuxClient) dialWS(endpoint, token string) (net.Conn, error) {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		ReadBufferSize:   64 * 1024,
		WriteBufferSize:  64 * 1024,
		NetDialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var d net.Dialer
			tcpConn, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			tlsConfig := &tls.Config{
				InsecureSkipVerify: true,
			}
			tlsConn := tls.UClient(tcpConn, tlsConfig, tls.HelloChrome_Auto)
			if err := tlsConn.Handshake(); err != nil {
				tcpConn.Close()
				return nil, err
			}
			return tlsConn, nil
		},
	}

	header := make(map[string][]string)
	header["Authorization"] = []string{"Bearer " + token}

	wsConn, _, err := dialer.DialContext(c.ctx, endpoint, header)
	if err != nil {
		return nil, fmt.Errorf("dial websocket: %w", err)
	}

	return NewWSConn(wsConn), nil
}

// performHandshake sends tunnel handshake and waits for response.
func (c *SmuxClient) performHandshake(conn net.Conn, token, ruleID string) error {
	handshake := &forward.TunnelHandshake{
		AgentToken: token,
		RuleID:     ruleID,
		EnableSmux: true, // Enable SMUX multiplexing
	}

	tokenDbg := token
	if len(tokenDbg) > 15 {
		tokenDbg = tokenDbg[:15] + "..."
	}
	tokenParts := strings.SplitN(token, "_", 3)
	logger.Debug("sending smux tunnel handshake",
		"rule_id", ruleID,
		"token_prefix", tokenDbg,
		"token_len", len(token),
		"token_parts", len(tokenParts))

	handshakeData, err := json.Marshal(handshake)
	if err != nil {
		return fmt.Errorf("marshal handshake: %w", err)
	}

	// Set write deadline to prevent blocking if server doesn't read
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := writeLengthPrefixedData(conn, handshakeData); err != nil {
		conn.SetWriteDeadline(time.Time{})
		return fmt.Errorf("send handshake: %w", err)
	}
	conn.SetWriteDeadline(time.Time{})

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	resultData, err := readLengthPrefixedData(conn)
	if err != nil {
		conn.SetReadDeadline(time.Time{})
		return fmt.Errorf("read handshake result: %w", err)
	}
	conn.SetReadDeadline(time.Time{})

	var result forward.TunnelHandshakeResult
	if err := json.Unmarshal(resultData, &result); err != nil {
		return fmt.Errorf("unmarshal handshake result: %w", err)
	}
	if !result.Success {
		return fmt.Errorf("handshake failed: %s", result.Error)
	}

	logger.Info("smux tunnel handshake successful", "entry_agent_id", result.EntryAgentID)
	return nil
}

// sessionMonitor monitors the SMUX session and handles reconnection.
func (c *SmuxClient) sessionMonitor() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		c.sessionMu.RLock()
		session := c.session
		c.sessionMu.RUnlock()

		if session == nil || session.IsClosed() {
			c.connected.Store(false)
			logger.Warn("smux session closed, attempting reconnection")

			if !c.reconnect() {
				return
			}
			continue
		}

		// Check session health periodically
		time.Sleep(5 * time.Second)
	}
}

// reconnect attempts to reconnect with exponential backoff.
func (c *SmuxClient) reconnect() bool {
	for {
		interval := c.backoff.Next()
		attempt := c.backoff.Attempt()

		logger.Info("reconnecting smux with backoff",
			"attempt", attempt,
			"interval", interval.Round(time.Millisecond))

		timer := time.NewTimer(interval)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}

		if c.endpointRefresher != nil && c.refreshAfterAttempts > 0 &&
			attempt%c.refreshAfterAttempts == 0 {
			logger.Info("refreshing smux endpoint after failed reconnect attempts", "attempts", attempt)
			if newEndpoint, newToken, err := c.endpointRefresher(); err != nil {
				logger.Error("smux endpoint refresh failed", "error", err)
			} else {
				c.endpointMu.Lock()
				if newEndpoint != c.endpoint {
					logger.Info("smux endpoint updated", "old", c.endpoint, "new", newEndpoint)
					c.endpoint = newEndpoint
					c.token = newToken
				}
				c.endpointMu.Unlock()
			}
		}

		if err := c.connect(); err != nil {
			logger.Error("smux reconnect failed", "error", err, "attempt", attempt)
			continue
		}

		c.backoff.Reset()
		logger.Info("smux reconnected successfully after attempts", "attempts", attempt)
		return true
	}
}
