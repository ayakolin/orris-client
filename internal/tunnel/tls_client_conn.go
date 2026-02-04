package tunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	tls "github.com/refraction-networking/utls"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
)

// connectWithRetry attempts to connect with retries and endpoint refresh.
func (c *TLSClient) connectWithRetry() error {
	err := c.connect()
	if err == nil {
		return nil
	}

	// If no retry configured, fail immediately
	if c.initialRetryMax <= 0 {
		return err
	}

	logger.Warn("initial tls connection failed, will retry", "error", err, "max_retries", c.initialRetryMax)

	for attempt := 1; attempt <= c.initialRetryMax; attempt++ {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		// Try to refresh endpoint before retry
		if c.endpointRefresher != nil && c.refreshAfterAttempts > 0 &&
			attempt%c.refreshAfterAttempts == 0 {
			logger.Info("refreshing tls endpoint before retry", "attempt", attempt)
			if newEndpoint, newToken, refreshErr := c.endpointRefresher(); refreshErr != nil {
				logger.Error("tls endpoint refresh failed", "error", refreshErr)
			} else {
				c.endpointMu.Lock()
				if newEndpoint != c.endpoint {
					logger.Info("tls endpoint updated", "old", c.endpoint, "new", newEndpoint)
					c.endpoint = newEndpoint
					c.token = newToken
				}
				c.endpointMu.Unlock()
			}
		}

		interval := c.backoff.Next()
		logger.Info("retrying initial tls connection",
			"attempt", attempt,
			"max_retries", c.initialRetryMax,
			"interval", interval.Round(time.Millisecond))

		// Use timer instead of time.After to avoid goroutine leak
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
			logger.Info("initial tls connection succeeded after retries", "attempts", attempt)
			return nil
		}

		logger.Warn("initial tls connection retry failed", "attempt", attempt, "error", err)
	}

	return fmt.Errorf("max retries (%d) exceeded: %w", c.initialRetryMax, err)
}

// connect establishes a TLS connection using utls and performs handshake.
func (c *TLSClient) connect() error {
	c.endpointMu.RLock()
	endpoint := c.endpoint
	token := c.token
	ruleID := c.ruleID
	c.endpointMu.RUnlock()

	logger.Info("connecting to exit agent via tls", "endpoint", endpoint, "rule_id", ruleID)

	// Dial TCP connection with timeout
	dialCtx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()

	var d net.Dialer
	tcpConn, err := d.DialContext(dialCtx, "tcp", endpoint)
	if err != nil {
		return fmt.Errorf("dial tcp: %w", err)
	}

	// Wrap with utls
	// Use Chrome fingerprint for better compatibility and to avoid detection
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // Exit agent uses self-signed cert
	}
	tlsConn := tls.UClient(tcpConn, tlsConfig, tls.HelloChrome_Auto)

	// Perform TLS handshake
	if err := tlsConn.Handshake(); err != nil {
		tcpConn.Close()
		return fmt.Errorf("tls handshake: %w", err)
	}

	// Send tunnel handshake (length-prefixed JSON)
	handshake := &forward.TunnelHandshake{
		AgentToken: token,
		RuleID:     ruleID,
	}

	// Log token info for debugging (only prefix for security)
	tokenDbg := token
	if len(tokenDbg) > 15 {
		tokenDbg = tokenDbg[:15] + "..."
	}
	tokenParts := strings.SplitN(token, "_", 3)
	logger.Debug("sending tls tunnel handshake",
		"rule_id", ruleID,
		"token_prefix", tokenDbg,
		"token_len", len(token),
		"token_parts", len(tokenParts))

	handshakeData, err := json.Marshal(handshake)
	if err != nil {
		tlsConn.Close()
		return fmt.Errorf("marshal handshake: %w", err)
	}

	// Obfuscate handshake data (XOR + random padding)
	obfuscatedData := ObfuscateHandshake(handshakeData)

	if err := writeLengthPrefixedData(tlsConn, obfuscatedData); err != nil {
		tlsConn.Close()
		return fmt.Errorf("send handshake: %w", err)
	}

	// Wait for handshake result
	tlsConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	resultData, err := readLengthPrefixedData(tlsConn)
	if err != nil {
		tlsConn.Close()
		return fmt.Errorf("read handshake result: %w", err)
	}
	tlsConn.SetReadDeadline(time.Time{}) // Clear deadline

	// Deobfuscate result
	deobfuscatedResult, err := DeobfuscateHandshake(resultData)
	if err != nil {
		tlsConn.Close()
		return fmt.Errorf("deobfuscate handshake result: %w", err)
	}

	var result forward.TunnelHandshakeResult
	if err := json.Unmarshal(deobfuscatedResult, &result); err != nil {
		tlsConn.Close()
		return fmt.Errorf("unmarshal handshake result: %w", err)
	}
	if !result.Success {
		tlsConn.Close()
		return fmt.Errorf("handshake failed: %s", result.Error)
	}

	logger.Info("tls tunnel handshake successful", "entry_agent_id", result.EntryAgentID)

	// Hold write lock when updating connection
	c.writeMu.Lock()
	oldConn := c.conn
	c.conn = tlsConn
	c.writeMu.Unlock()

	// Set connected state to true after successful connection
	c.connected.Store(true)

	// Close old connection if exists (during reconnect)
	if oldConn != nil {
		oldConn.Close()
	}

	logger.Info("connected to exit agent via tls")
	return nil
}

// reconnect attempts to reconnect with exponential backoff.
func (c *TLSClient) reconnect() bool {
	for {
		interval := c.backoff.Next()
		attempt := c.backoff.Attempt()

		logger.Info("reconnecting tls with backoff",
			"attempt", attempt,
			"interval", interval.Round(time.Millisecond))

		// Use timer instead of time.After to avoid goroutine leak
		timer := time.NewTimer(interval)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}

		// Try to refresh endpoint after configured number of failed attempts
		if c.endpointRefresher != nil && c.refreshAfterAttempts > 0 &&
			attempt%c.refreshAfterAttempts == 0 {
			logger.Info("refreshing tls endpoint after failed reconnect attempts", "attempts", attempt)
			if newEndpoint, newToken, err := c.endpointRefresher(); err != nil {
				logger.Error("tls endpoint refresh failed", "error", err)
			} else {
				c.endpointMu.Lock()
				if newEndpoint != c.endpoint {
					logger.Info("tls endpoint updated", "old", c.endpoint, "new", newEndpoint)
					c.endpoint = newEndpoint
					c.token = newToken
				}
				c.endpointMu.Unlock()
			}
		}

		if err := c.connect(); err != nil {
			logger.Error("tls reconnect failed", "error", err, "attempt", attempt)
			continue
		}

		// Reset backoff on successful reconnection
		c.backoff.Reset()
		logger.Info("tls reconnected successfully after attempts", "attempts", attempt)
		return true
	}
}
