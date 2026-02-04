package tunnel

import (
	"context"
	"net"

	"github.com/orris-inc/orris-client/internal/forward"
)

// TunnelClient is the common interface for tunnel clients (WS and TLS).
type TunnelClient interface {
	// Start starts the tunnel client with auto-reconnect.
	Start(ctx context.Context) error

	// Stop stops the tunnel client.
	Stop() error

	// SendMessage sends a message through the tunnel.
	SendMessage(msg *Message) error

	// SetHandler sets the data handler.
	SetHandler(h DataHandler)

	// IsConnected returns true if the tunnel is connected.
	IsConnected() bool
}

// TunnelServer is the common interface for tunnel servers (WS and TLS).
type TunnelServer interface {
	// Start starts the tunnel server.
	Start(ctx context.Context) error

	// Stop stops the tunnel server.
	Stop() error

	// Port returns the actual listening port.
	Port() uint16

	// AddHandler adds a message handler for a rule.
	AddHandler(ruleID string, handler MessageHandler)

	// RemoveHandler removes a message handler.
	RemoveHandler(ruleID string)

	// UpdateRules updates the rules for handshake verification.
	UpdateRules(rules []forward.Rule)

	// SetSmuxHandler sets the SMUX stream handler for a rule.
	// Used when client connects with EnableSmux=true in handshake.
	SetSmuxHandler(ruleID string, handler SmuxStreamHandler)

	// RemoveSmuxHandler removes the SMUX stream handler for a rule.
	RemoveSmuxHandler(ruleID string)
}

// SmuxTunnelClient is the interface for SMUX-based tunnel clients.
// It extends the ability to open multiplexed streams over a single connection.
type SmuxTunnelClient interface {
	// Start starts the SMUX client and establishes the underlying connection.
	Start(ctx context.Context) error

	// Stop stops the SMUX client and closes all streams.
	Stop() error

	// OpenStream opens a new multiplexed stream.
	// Each stream can be used for bidirectional data transfer.
	OpenStream() (net.Conn, error)

	// IsConnected returns true if the underlying connection is established.
	IsConnected() bool
}

// SmuxTunnelServer is the interface for SMUX-based tunnel servers.
// It accepts multiplexed streams from SMUX clients.
type SmuxTunnelServer interface {
	// Start starts the SMUX server.
	Start(ctx context.Context) error

	// Stop stops the SMUX server and closes all sessions.
	Stop() error

	// Port returns the actual listening port.
	Port() uint16

	// SetStreamHandler sets the handler for incoming streams.
	SetStreamHandler(ruleID string, handler SmuxStreamHandler)

	// RemoveStreamHandler removes the stream handler.
	RemoveStreamHandler(ruleID string)

	// UpdateRules updates the rules for handshake verification.
	UpdateRules(rules []forward.Rule)
}

// SmuxStreamHandler handles incoming SMUX streams.
type SmuxStreamHandler interface {
	// HandleStream handles an incoming SMUX stream.
	// The stream should be used for bidirectional data transfer.
	// The handler is responsible for closing the stream when done.
	HandleStream(stream net.Conn)

	// OnSessionClose is called when the SMUX session is closed.
	OnSessionClose()
}
