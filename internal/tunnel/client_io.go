package tunnel

import (
	"bytes"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
	"github.com/orris-inc/orris-client/internal/logger"
)

// SendMessage sends a message through the tunnel.
func (c *Client) SendMessage(msg *Message) error {
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		return fmt.Errorf("write message: %w", err)
	}

	return nil
}

// readLoop reads messages from the tunnel connection.
func (c *Client) readLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		_, data, err := c.conn.ReadMessage()
		if err != nil {
			logger.Error("tunnel read error", "error", err)

			// Clear connection state immediately to prevent SendMessage from using broken conn
			c.connected.Store(false)

			// Clear connection immediately to prevent SendMessage from using broken conn
			c.writeMu.Lock()
			if c.conn != nil {
				c.conn.Close()
				c.conn = nil
			}
			c.writeMu.Unlock()

			if !c.reconnect() {
				return
			}
			continue
		}

		msg, err := DecodeMessage(bytes.NewReader(data))
		if err != nil {
			logger.Error("decode message error", "error", err)
			continue
		}

		c.handleMessage(msg)
	}
}

// handleMessage dispatches a received message to the appropriate handler.
func (c *Client) handleMessage(msg *Message) {
	switch msg.Type {
	case MsgData:
		if c.handler != nil {
			c.handler.HandleData(msg.ConnID, msg.Payload)
		}
	case MsgClose:
		if c.handler != nil {
			c.handler.HandleClose(msg.ConnID)
		}
	case MsgPong:
		// Notify Ping() caller if waiting
		c.pongMu.Lock()
		if c.pongCh != nil {
			select {
			case c.pongCh <- struct{}{}:
			default:
			}
		}
		c.pongMu.Unlock()
	default:
		logger.Warn("unknown message type", "type", msg.Type)
	}
}

// heartbeatLoop sends periodic ping messages to keep the tunnel connection alive.
func (c *Client) heartbeatLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.SendMessage(NewPingMessage()); err != nil {
				logger.Error("send ping failed", "error", err)
			}
		}
	}
}
