package tunnel

import (
	"context"

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
}
