package forwarder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
	"github.com/orris-inc/orris-client/internal/tunnel"
)

// udpBoundaryClient tracks UDP client state on boundary relay side.
type udpBoundaryClient struct {
	clientAddr     string       // Entry-side client address (for logging)
	upstream       *net.UDPConn // Connection to next hop
	lastActiveNano atomic.Int64 // Unix nanoseconds, safe for concurrent access
}

// BoundaryRelayForwarder handles boundary relay forwarding (tunnel inbound -> direct outbound).
// This is used in hybrid chain mode where the relay node receives data via tunnel
// from the previous hop and forwards to the next hop using direct TCP/UDP connection.
type BoundaryRelayForwarder struct {
	rule    *forward.Rule
	traffic *TrafficCounter

	tunnel tunnel.Sender
	connMu sync.RWMutex
	conns  map[uint64]*connState // connID -> TCP next hop connection state (async write)

	// Circuit breaker to prevent connection storms when next hop is unreachable
	cb *circuitBreaker

	// UDP connections
	udpConnsMu sync.RWMutex
	udpConns   map[uint64]*udpBoundaryClient // connID -> UDP state

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBoundaryRelayForwarder creates a new boundary relay forwarder.
func NewBoundaryRelayForwarder(rule *forward.Rule) *BoundaryRelayForwarder {
	return &BoundaryRelayForwarder{
		rule:     rule,
		traffic:  &TrafficCounter{},
		cb:       newCircuitBreaker(),
		conns:    make(map[uint64]*connState),
		udpConns: make(map[uint64]*udpBoundaryClient),
	}
}

// SetSender sets the tunnel sender (called by tunnel.Server).
func (f *BoundaryRelayForwarder) SetSender(s tunnel.Sender) {
	f.tunnel = s
}

// Start starts the boundary relay forwarder.
func (f *BoundaryRelayForwarder) Start(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	// Validate next hop address
	if f.rule.NextHopAddress == "" {
		return fmt.Errorf("next hop address is empty")
	}
	if f.rule.NextHopPort == 0 {
		return fmt.Errorf("next hop port is empty")
	}

	// Start UDP cleanup loop
	f.wg.Add(1)
	go f.udpCleanupLoop()

	logger.Info("boundary relay forwarder started",
		"rule_id", f.rule.ID,
		"next_hop", fmt.Sprintf("%s:%d", f.rule.NextHopAddress, f.rule.NextHopPort),
		"hop_mode", f.rule.HopMode,
		"outbound_mode", f.rule.OutboundMode)
	return nil
}

// Stop stops the boundary relay forwarder.
func (f *BoundaryRelayForwarder) Stop() error {
	if f.cancel != nil {
		f.cancel()
	}

	// Close TCP connections
	f.connMu.Lock()
	for _, cs := range f.conns {
		cs.Close()
	}
	f.conns = make(map[uint64]*connState)
	f.connMu.Unlock()

	// Close UDP connections
	f.udpConnsMu.Lock()
	for _, client := range f.udpConns {
		if client.upstream != nil {
			client.upstream.Close()
		}
	}
	f.udpConns = make(map[uint64]*udpBoundaryClient)
	f.udpConnsMu.Unlock()

	f.wg.Wait()
	logger.Info("boundary relay forwarder stopped", "rule_id", f.rule.ID)
	return nil
}

// Traffic returns the traffic counter.
func (f *BoundaryRelayForwarder) Traffic() *TrafficCounter {
	return f.traffic
}

// RuleID returns the rule ID.
func (f *BoundaryRelayForwarder) RuleID() string {
	return f.rule.ID
}

// HandleConnect handles TCP connect message from tunnel.
func (f *BoundaryRelayForwarder) HandleConnect(connID uint64) {
	nextHopAddr := net.JoinHostPort(f.rule.NextHopAddress, fmt.Sprintf("%d", f.rule.NextHopPort))

	// Check circuit breaker before attempting dial
	if !f.cb.Allow() {
		logger.Debug("boundary relay connection rejected by circuit breaker",
			"conn_id", connID,
			"next_hop", nextHopAddr,
			"state", f.cb.State())
		if f.tunnel != nil {
			f.tunnel.SendMessage(tunnel.NewCloseMessage(connID))
		}
		return
	}

	dialer := &net.Dialer{Timeout: 500 * time.Millisecond}
	if f.rule.BindIP != "" {
		dialer.LocalAddr = &net.TCPAddr{IP: net.ParseIP(f.rule.BindIP)}
	}

	nextHopConn, err := dialer.Dial("tcp", nextHopAddr)
	if err != nil {
		f.cb.RecordFailure()
		logger.Error("boundary relay tcp dial next hop failed",
			"conn_id", connID,
			"next_hop", nextHopAddr,
			"bind_ip", f.rule.BindIP,
			"cb_state", f.cb.State(),
			"error", err)
		if f.tunnel != nil {
			f.tunnel.SendMessage(tunnel.NewCloseMessage(connID))
		}
		return
	}

	// Dial succeeded, record success
	f.cb.RecordSuccess()

	// Create connState with async write queue for upload direction
	cs := newConnState(nextHopConn, f.traffic.AddUpload)

	f.connMu.Lock()
	f.conns[connID] = cs
	f.connMu.Unlock()

	logger.Debug("boundary relay tcp next hop connection established", "conn_id", connID, "next_hop", nextHopAddr)

	f.wg.Add(1)
	go f.readFromNextHop(connID, nextHopConn)
}

// HandleConnectWithPayload handles UDP connect message with client address payload.
func (f *BoundaryRelayForwarder) HandleConnectWithPayload(connID uint64, payload []byte) {
	// Check if this is a UDP connection
	if !tunnel.IsUDPConnID(connID) {
		// TCP connect with payload - ignore payload, treat as regular connect
		f.HandleConnect(connID)
		return
	}

	actualID := tunnel.GetConnIDValue(connID)
	clientAddr := string(payload)

	nextHopAddr := net.JoinHostPort(f.rule.NextHopAddress, fmt.Sprintf("%d", f.rule.NextHopPort))

	// Check circuit breaker before attempting dial
	if !f.cb.Allow() {
		logger.Debug("boundary relay udp connection rejected by circuit breaker",
			"conn_id", actualID,
			"next_hop", nextHopAddr,
			"state", f.cb.State())
		f.sendUDPClose(actualID)
		return
	}

	upstreamAddr, err := net.ResolveUDPAddr("udp", nextHopAddr)
	if err != nil {
		logger.Error("boundary relay resolve udp next hop failed", "conn_id", actualID, "error", err)
		f.sendUDPClose(actualID)
		return
	}

	var localAddr *net.UDPAddr
	if f.rule.BindIP != "" {
		localAddr = &net.UDPAddr{IP: net.ParseIP(f.rule.BindIP)}
	}

	upstream, err := net.DialUDP("udp", localAddr, upstreamAddr)
	if err != nil {
		f.cb.RecordFailure()
		logger.Error("boundary relay udp dial next hop failed",
			"conn_id", actualID,
			"next_hop", nextHopAddr,
			"bind_ip", f.rule.BindIP,
			"cb_state", f.cb.State(),
			"error", err)
		f.sendUDPClose(actualID)
		return
	}

	// Dial succeeded, record success
	f.cb.RecordSuccess()

	client := &udpBoundaryClient{
		clientAddr: clientAddr,
		upstream:   upstream,
	}
	client.lastActiveNano.Store(time.Now().UnixNano())

	f.udpConnsMu.Lock()
	f.udpConns[actualID] = client
	f.udpConnsMu.Unlock()

	logger.Debug("boundary relay udp connection established", "conn_id", actualID, "client", clientAddr, "next_hop", nextHopAddr)

	f.wg.Add(1)
	go f.udpUpstreamReadLoop(actualID, client)
}

// HandleData handles data message from tunnel (previous hop -> boundary relay -> next hop).
// Uses async write queue to prevent blocking the tunnel read loop.
func (f *BoundaryRelayForwarder) HandleData(connID uint64, data []byte) {
	// Check if this is a UDP connection
	if tunnel.IsUDPConnID(connID) {
		f.handleUDPData(tunnel.GetConnIDValue(connID), data)
		return
	}

	// TCP connection - use async write
	f.connMu.RLock()
	cs, ok := f.conns[connID]
	f.connMu.RUnlock()

	if !ok || cs.IsClosed() {
		return
	}

	if err := cs.Write(data); err != nil {
		if errors.Is(err, ErrQueueFull) {
			// Queue full - next hop is accepting too slow, close connection to prevent resource leak
			logger.Error("boundary relay write queue full, closing connection", "conn_id", connID, "len", len(data))
			f.closeTCPConn(connID)
		} else {
			logger.Debug("boundary relay async write to tcp next hop failed", "conn_id", connID, "error", err)
			f.closeTCPConn(connID)
		}
	}
	// Note: traffic is counted in connState.writeLoop after actual write
}

// handleUDPData handles UDP data from tunnel and forwards to next hop.
func (f *BoundaryRelayForwarder) handleUDPData(connID uint64, data []byte) {
	f.udpConnsMu.RLock()
	client, ok := f.udpConns[connID]
	f.udpConnsMu.RUnlock()

	if !ok {
		logger.Debug("boundary relay udp unknown connID", "conn_id", connID)
		return
	}

	client.lastActiveNano.Store(time.Now().UnixNano())

	n, err := client.upstream.Write(data)
	if err != nil {
		logger.Debug("boundary relay write to udp next hop failed", "conn_id", connID, "error", err)
		f.closeUDPConn(connID)
		return
	}
	f.traffic.AddUpload(int64(n))
}

// HandleClose handles close message from tunnel.
func (f *BoundaryRelayForwarder) HandleClose(connID uint64) {
	if tunnel.IsUDPConnID(connID) {
		f.closeUDPConn(tunnel.GetConnIDValue(connID))
		return
	}
	f.closeTCPConn(connID)
}

func (f *BoundaryRelayForwarder) readFromNextHop(connID uint64, nextHopConn net.Conn) {
	defer f.wg.Done()
	defer f.closeTCPConn(connID)

	bufp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufp)
	buf := *bufp

	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

		n, err := nextHopConn.Read(buf)
		if err != nil {
			if err != io.EOF && !isClosedError(err) {
				logger.Debug("boundary relay read from tcp next hop failed", "conn_id", connID, "error", err)
			}
			return
		}

		f.traffic.AddDownload(int64(n))

		if f.tunnel != nil {
			if err := f.tunnel.SendMessage(tunnel.NewDataMessage(connID, buf[:n])); err != nil {
				logger.Error("boundary relay send tcp data message failed", "conn_id", connID, "error", err)
				return
			}
		}
	}
}

// udpUpstreamReadLoop reads responses from UDP next hop and sends back through tunnel.
func (f *BoundaryRelayForwarder) udpUpstreamReadLoop(connID uint64, client *udpBoundaryClient) {
	defer f.wg.Done()
	defer f.closeUDPConn(connID)

	buf := make([]byte, udpMaxPacketSize)

	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

		// Set read deadline to allow periodic check
		client.upstream.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := client.upstream.Read(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if err != io.EOF && !isClosedError(err) && f.ctx.Err() == nil {
				logger.Debug("boundary relay udp upstream read error", "conn_id", connID, "error", err)
			}
			return
		}

		client.lastActiveNano.Store(time.Now().UnixNano())
		f.traffic.AddDownload(int64(n))

		// Send response back through tunnel
		if f.tunnel != nil {
			if err := f.tunnel.SendMessage(tunnel.NewUDPDataMessage(connID, buf[:n])); err != nil {
				logger.Error("boundary relay send udp data message failed", "conn_id", connID, "error", err)
				return
			}
		}
	}
}

func (f *BoundaryRelayForwarder) closeTCPConn(connID uint64) {
	f.connMu.Lock()
	cs, ok := f.conns[connID]
	if ok {
		delete(f.conns, connID)
	}
	f.connMu.Unlock()

	if ok {
		cs.Close()
		if f.tunnel != nil {
			f.tunnel.SendMessage(tunnel.NewCloseMessage(connID))
		}
	}
}

// closeUDPConn closes a UDP connection.
func (f *BoundaryRelayForwarder) closeUDPConn(connID uint64) {
	f.udpConnsMu.Lock()
	client, ok := f.udpConns[connID]
	if ok {
		delete(f.udpConns, connID)
	}
	f.udpConnsMu.Unlock()

	if ok && client.upstream != nil {
		client.upstream.Close()
	}
}

// sendUDPClose sends a UDP close message through tunnel.
func (f *BoundaryRelayForwarder) sendUDPClose(connID uint64) {
	if f.tunnel != nil {
		f.tunnel.SendMessage(tunnel.NewUDPCloseMessage(connID))
	}
}

// udpCleanupLoop periodically cleans up idle UDP connections.
func (f *BoundaryRelayForwarder) udpCleanupLoop() {
	defer f.wg.Done()

	ticker := time.NewTicker(udpCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			f.cleanupIdleUDPClients()
		}
	}
}

// cleanupIdleUDPClients removes idle UDP clients.
func (f *BoundaryRelayForwarder) cleanupIdleUDPClients() {
	now := time.Now()

	f.udpConnsMu.Lock()
	defer f.udpConnsMu.Unlock()

	for connID, client := range f.udpConns {
		lastActiveNano := client.lastActiveNano.Load()
		lastActive := time.Unix(0, lastActiveNano)
		if now.Sub(lastActive) > udpIdleTimeout {
			logger.Debug("boundary relay removing idle udp client", "conn_id", connID, "client", client.clientAddr)
			if client.upstream != nil {
				client.upstream.Close()
			}
			delete(f.udpConns, connID)
		}
	}
}

// ListenPort returns 0 as BoundaryRelayForwarder does not have a listening port.
func (f *BoundaryRelayForwarder) ListenPort() uint16 {
	return 0
}

// Connections returns the current number of active connections (TCP + UDP).
func (f *BoundaryRelayForwarder) Connections() int {
	f.connMu.RLock()
	tcpConns := len(f.conns)
	f.connMu.RUnlock()

	f.udpConnsMu.RLock()
	udpConns := len(f.udpConns)
	f.udpConnsMu.RUnlock()

	return tcpConns + udpConns
}
