package forwarder

import (
	"context"
	"fmt"
	"sync"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
	"github.com/orris-inc/orris-client/internal/tunnel"
)

// RelayForwarder handles relay forwarding (WS tunnel inbound -> WS tunnel outbound).
// It bridges data between two tunnel connections in a chain.
type RelayForwarder struct {
	rule    *forward.Rule
	traffic *TrafficCounter

	inbound  tunnel.Sender  // sender to previous hop (set by tunnel.Server)
	outbound *tunnel.Client // client to next hop

	closedMu sync.RWMutex
	closed   map[uint64]bool // track closed connections to avoid loops

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRelayForwarder creates a new relay forwarder.
func NewRelayForwarder(rule *forward.Rule, outbound *tunnel.Client) *RelayForwarder {
	return &RelayForwarder{
		rule:     rule,
		outbound: outbound,
		traffic:  &TrafficCounter{},
		closed:   make(map[uint64]bool),
	}
}

// SetSender sets the inbound tunnel sender (called by tunnel.Server).
func (f *RelayForwarder) SetSender(s tunnel.Sender) {
	f.inbound = s
}

// Start starts the relay forwarder.
func (f *RelayForwarder) Start(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	// Set outbound handler wrapper for responses from next hop
	f.outbound.SetHandler(&relayOutboundHandler{relay: f})

	logger.Info("relay forwarder started",
		"rule_id", f.rule.ID,
		"next_hop", fmt.Sprintf("%s:%d", f.rule.NextHopAddress, f.rule.NextHopWsPort))
	return nil
}

// Stop stops the relay forwarder.
func (f *RelayForwarder) Stop() error {
	if f.cancel != nil {
		f.cancel()
	}
	f.wg.Wait()
	logger.Info("relay forwarder stopped", "rule_id", f.rule.ID)
	return nil
}

// Traffic returns the traffic counter.
func (f *RelayForwarder) Traffic() *TrafficCounter {
	return f.traffic
}

// RuleID returns the rule ID.
func (f *RelayForwarder) RuleID() string {
	return f.rule.ID
}

// HandleConnect handles connect message from inbound (previous hop -> relay -> next hop).
// Implements tunnel.MessageHandler for tunnel.Server.
func (f *RelayForwarder) HandleConnect(connID uint64) {
	logger.Debug("relay forward connect", "rule_id", f.rule.ID, "conn_id", connID)

	if f.outbound == nil {
		logger.Error("relay outbound not connected", "rule_id", f.rule.ID, "conn_id", connID)
		if f.inbound != nil {
			f.inbound.SendMessage(tunnel.NewCloseMessage(connID))
		}
		return
	}

	if err := f.outbound.SendMessage(tunnel.NewConnectMessage(connID)); err != nil {
		logger.Error("relay forward connect failed", "rule_id", f.rule.ID, "conn_id", connID, "error", err)
		if f.inbound != nil {
			f.inbound.SendMessage(tunnel.NewCloseMessage(connID))
		}
	}
}

// HandleConnectWithPayload handles connect message with payload (for UDP).
// Forwards the message with payload intact to the next hop.
func (f *RelayForwarder) HandleConnectWithPayload(connID uint64, payload []byte) {
	logger.Debug("relay forward connect with payload", "rule_id", f.rule.ID, "conn_id", connID, "payload_len", len(payload))

	if f.outbound == nil {
		logger.Error("relay outbound not connected", "rule_id", f.rule.ID, "conn_id", connID)
		if f.inbound != nil {
			f.inbound.SendMessage(tunnel.NewCloseMessage(connID))
		}
		return
	}

	// Forward connect message with payload intact (preserves UDP flag in connID)
	msg := &tunnel.Message{
		Type:    tunnel.MsgConnect,
		ConnID:  connID,
		Payload: payload,
	}
	if err := f.outbound.SendMessage(msg); err != nil {
		logger.Error("relay forward connect with payload failed", "rule_id", f.rule.ID, "conn_id", connID, "error", err)
		if f.inbound != nil {
			f.inbound.SendMessage(tunnel.NewCloseMessage(connID))
		}
	}
}

// HandleData handles data message from inbound (previous hop -> relay -> next hop).
// Implements tunnel.MessageHandler for tunnel.Server.
func (f *RelayForwarder) HandleData(connID uint64, data []byte) {
	if f.outbound == nil {
		return
	}

	f.traffic.AddUpload(int64(len(data)))

	if err := f.outbound.SendMessage(tunnel.NewDataMessage(connID, data)); err != nil {
		logger.Debug("relay forward data to next hop failed", "rule_id", f.rule.ID, "conn_id", connID, "error", err)
		f.closeConn(connID)
	}
}

// HandleClose handles close message from inbound (previous hop -> relay -> next hop).
// Implements tunnel.MessageHandler for tunnel.Server.
func (f *RelayForwarder) HandleClose(connID uint64) {
	f.closedMu.Lock()
	defer f.closedMu.Unlock()

	if f.closed[connID] {
		return
	}
	f.closed[connID] = true

	logger.Debug("relay forward close to next hop", "rule_id", f.rule.ID, "conn_id", connID)

	if f.outbound != nil {
		f.outbound.SendMessage(tunnel.NewCloseMessage(connID))
	}
}

// handleOutboundData handles data from outbound (next hop -> relay -> previous hop).
func (f *RelayForwarder) handleOutboundData(connID uint64, data []byte) {
	if f.inbound == nil {
		return
	}

	f.traffic.AddDownload(int64(len(data)))

	if err := f.inbound.SendMessage(tunnel.NewDataMessage(connID, data)); err != nil {
		logger.Debug("relay forward data to previous hop failed", "rule_id", f.rule.ID, "conn_id", connID, "error", err)
		f.closeConn(connID)
	}
}

// handleOutboundClose handles close from outbound (next hop -> relay -> previous hop).
func (f *RelayForwarder) handleOutboundClose(connID uint64) {
	f.closedMu.Lock()
	defer f.closedMu.Unlock()

	if f.closed[connID] {
		return
	}
	f.closed[connID] = true

	logger.Debug("relay forward close to previous hop", "rule_id", f.rule.ID, "conn_id", connID)

	if f.inbound != nil {
		f.inbound.SendMessage(tunnel.NewCloseMessage(connID))
	}
}

func (f *RelayForwarder) closeConn(connID uint64) {
	f.closedMu.Lock()
	defer f.closedMu.Unlock()

	if f.closed[connID] {
		return
	}
	f.closed[connID] = true

	if f.inbound != nil {
		f.inbound.SendMessage(tunnel.NewCloseMessage(connID))
	}
	if f.outbound != nil {
		f.outbound.SendMessage(tunnel.NewCloseMessage(connID))
	}
}

// relayOutboundHandler wraps RelayForwarder for outbound DataHandler interface.
type relayOutboundHandler struct {
	relay *RelayForwarder
}

// HandleData implements tunnel.DataHandler for outbound responses.
func (h *relayOutboundHandler) HandleData(connID uint64, data []byte) {
	h.relay.handleOutboundData(connID, data)
}

// HandleClose implements tunnel.DataHandler for outbound responses.
func (h *relayOutboundHandler) HandleClose(connID uint64) {
	h.relay.handleOutboundClose(connID)
}
