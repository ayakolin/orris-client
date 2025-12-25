package tunnel

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/orris-inc/orris-client/internal/logger"
)

// SendMessage sends a message through the TLS tunnel.
func (c *TLSClient) SendMessage(msg *Message) error {
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	if _, err := c.conn.Write(data); err != nil {
		return fmt.Errorf("write message: %w", err)
	}

	return nil
}

// readLoop reads messages from the TLS tunnel connection.
func (c *TLSClient) readLoop() {
	defer c.wg.Done()

	var reader *bufio.Reader

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// Use writeMu to safely read conn reference. writeMu protects both
		// conn writes (in SendMessage) and conn replacement (in connect/Stop).
		// This ensures we get a consistent view of the connection state.
		c.writeMu.Lock()
		conn := c.conn
		c.writeMu.Unlock()

		if conn == nil {
			if !c.reconnect() {
				return
			}
			// Create new reader after reconnect
			c.writeMu.Lock()
			conn = c.conn
			c.writeMu.Unlock()
			if conn != nil {
				reader = bufio.NewReader(conn)
			}
			continue
		}

		// Create reader for first connection or after reconnect
		if reader == nil {
			reader = bufio.NewReader(conn)
		}

		msg, err := DecodeMessage(reader)
		if err != nil {
			if err != io.EOF && c.ctx.Err() == nil {
				logger.Error("tls tunnel read error", "error", err)
			}

			// Clear connection state immediately to prevent SendMessage from using broken conn
			c.connected.Store(false)

			// Clear connection immediately to prevent SendMessage from using broken conn
			c.writeMu.Lock()
			if c.conn != nil {
				c.conn.Close()
				c.conn = nil
			}
			c.writeMu.Unlock()

			// Reset reader so it will be recreated after reconnect
			reader = nil

			if !c.reconnect() {
				return
			}

			// Create new reader after reconnect
			c.writeMu.Lock()
			conn = c.conn
			c.writeMu.Unlock()
			if conn != nil {
				reader = bufio.NewReader(conn)
			}
			continue
		}

		c.handleMessage(msg)
	}
}

// handleMessage dispatches a received message to the appropriate handler.
func (c *TLSClient) handleMessage(msg *Message) {
	c.handlerMu.RLock()
	h := c.handler
	c.handlerMu.RUnlock()

	if h == nil {
		return
	}

	switch msg.Type {
	case MsgData:
		h.HandleData(msg.ConnID, msg.Payload)
	case MsgClose:
		h.HandleClose(msg.ConnID)
	case MsgPong:
		logger.Debug("received tls pong")
	default:
		logger.Warn("unknown tls message type", "type", msg.Type)
	}
}

// heartbeatLoop sends periodic ping messages to keep the TLS tunnel connection alive.
func (c *TLSClient) heartbeatLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.SendMessage(NewPingMessage()); err != nil {
				logger.Error("send tls ping failed", "error", err)
			}
		}
	}
}
