package tunnel

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// wsConn wraps a websocket.Conn to implement net.Conn interface.
// This adapter is used for SMUX over WebSocket (ws_smux).
type wsConn struct {
	conn    *websocket.Conn
	reader  io.Reader
	readMu  sync.Mutex // protects reader
	writeMu sync.Mutex // protects concurrent writes (WebSocket doesn't support concurrent writes)
}

// NewWSConn creates a new net.Conn wrapper for a WebSocket connection.
func NewWSConn(conn *websocket.Conn) net.Conn {
	return &wsConn{conn: conn}
}

// Read reads data from the WebSocket connection.
// WebSocket messages are read frame by frame and buffered.
func (c *wsConn) Read(b []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	// If there's leftover data from previous message, read from it first
	if c.reader != nil {
		n, err := c.reader.Read(b)
		if err == io.EOF {
			c.reader = nil
			if n > 0 {
				return n, nil
			}
			// Fall through to get next message
		} else {
			return n, err
		}
	}

	// Get next message
	_, reader, err := c.conn.NextReader()
	if err != nil {
		return 0, err
	}
	c.reader = reader

	return c.reader.Read(b)
}

// Write writes data to the WebSocket connection as a binary message.
// Protected by mutex since WebSocket doesn't support concurrent writes.
func (c *wsConn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	err := c.conn.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

// Close closes the WebSocket connection.
func (c *wsConn) Close() error {
	return c.conn.Close()
}

// LocalAddr returns the local network address.
func (c *wsConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *wsConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines.
func (c *wsConn) SetDeadline(t time.Time) error {
	if err := c.conn.SetReadDeadline(t); err != nil {
		return err
	}
	return c.conn.SetWriteDeadline(t)
}

// SetReadDeadline sets the read deadline.
func (c *wsConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline.
func (c *wsConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
