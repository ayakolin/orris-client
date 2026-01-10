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

// udpExitClient tracks UDP client state on exit side.
type udpExitClient struct {
	clientAddr     string       // Entry-side client address (for logging)
	upstream       *net.UDPConn // Connection to target
	lastActiveNano atomic.Int64 // Unix nanoseconds, safe for concurrent access
}

// ExitForwarder handles exit forwarding (WS tunnel -> target).
type ExitForwarder struct {
	rule    *forward.Rule
	traffic *TrafficCounter

	tunnel tunnel.Sender
	connMu sync.RWMutex
	conns  map[uint64]*connState // connID -> TCP target connection state (async write)

	// Circuit breaker to prevent connection storms when target is unreachable
	cb *circuitBreaker

	// UDP connections
	udpConnsMu sync.RWMutex
	udpConns   map[uint64]*udpExitClient // connID -> UDP state

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewExitForwarder creates a new exit forwarder.
func NewExitForwarder(rule *forward.Rule) *ExitForwarder {
	return &ExitForwarder{
		rule:     rule,
		traffic:  &TrafficCounter{},
		cb:       newCircuitBreaker(),
		conns:    make(map[uint64]*connState),
		udpConns: make(map[uint64]*udpExitClient),
	}
}

// SetSender sets the tunnel sender.
func (f *ExitForwarder) SetSender(s tunnel.Sender) {
	f.tunnel = s
}

// Start starts the exit forwarder.
func (f *ExitForwarder) Start(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	// Validate target address
	if f.rule.TargetAddress == "" {
		return fmt.Errorf("target address is empty")
	}

	// Start UDP cleanup loop
	f.wg.Add(1)
	go f.udpCleanupLoop()

	logger.Info("exit forwarder started",
		"rule_id", f.rule.ID,
		"target", fmt.Sprintf("%s:%d", f.rule.TargetAddress, f.rule.TargetPort))
	return nil
}

// Stop stops the exit forwarder.
func (f *ExitForwarder) Stop() error {
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
	f.udpConns = make(map[uint64]*udpExitClient)
	f.udpConnsMu.Unlock()

	f.wg.Wait()
	logger.Info("exit forwarder stopped", "rule_id", f.rule.ID)
	return nil
}

// Traffic returns the traffic counter.
func (f *ExitForwarder) Traffic() *TrafficCounter {
	return f.traffic
}

// RuleID returns the rule ID.
func (f *ExitForwarder) RuleID() string {
	return f.rule.ID
}

// OnTunnelDisconnect is called when the tunnel connection is closed.
// It closes all active connections to stop readFromTarget and udpUpstreamReadLoop goroutines.
func (f *ExitForwarder) OnTunnelDisconnect() {
	// Close all TCP connections to unblock Read() in readFromTarget
	f.connMu.Lock()
	for _, cs := range f.conns {
		cs.Close()
	}
	f.connMu.Unlock()

	// Close all UDP connections to unblock Read() in udpUpstreamReadLoop
	f.udpConnsMu.Lock()
	for _, client := range f.udpConns {
		if client.upstream != nil {
			client.upstream.Close()
		}
	}
	f.udpConnsMu.Unlock()

	logger.Debug("exit forwarder tunnel disconnected, closed all connections", "rule_id", f.rule.ID)
}

// HandleConnect handles TCP connect message from tunnel.
func (f *ExitForwarder) HandleConnect(connID uint64) {
	targetAddr := net.JoinHostPort(f.rule.TargetAddress, fmt.Sprintf("%d", f.rule.TargetPort))

	// Check circuit breaker before attempting dial
	if !f.cb.Allow() {
		logger.Debug("exit connection rejected by circuit breaker",
			"conn_id", connID,
			"target", targetAddr,
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

	targetConn, err := dialer.Dial("tcp", targetAddr)
	if err != nil {
		f.cb.RecordFailure()
		logger.Error("exit tcp dial target failed",
			"conn_id", connID,
			"target", targetAddr,
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
	cs := newConnState(targetConn, f.traffic.AddUpload)

	f.connMu.Lock()
	f.conns[connID] = cs
	f.connMu.Unlock()

	logger.Debug("exit tcp target connection established", "conn_id", connID, "target", targetAddr)

	f.wg.Add(1)
	go f.readFromTarget(connID, targetConn)
}

// HandleConnectWithPayload handles UDP connect message with client address payload.
func (f *ExitForwarder) HandleConnectWithPayload(connID uint64, payload []byte) {
	// Check if this is a UDP connection
	if !tunnel.IsUDPConnID(connID) {
		// TCP connect with payload - ignore payload, treat as regular connect
		f.HandleConnect(connID)
		return
	}

	actualID := tunnel.GetConnIDValue(connID)
	clientAddr := string(payload)

	targetAddr := net.JoinHostPort(f.rule.TargetAddress, fmt.Sprintf("%d", f.rule.TargetPort))

	// Check circuit breaker before attempting dial
	if !f.cb.Allow() {
		logger.Debug("exit udp connection rejected by circuit breaker",
			"conn_id", actualID,
			"target", targetAddr,
			"state", f.cb.State())
		f.sendUDPClose(actualID)
		return
	}

	upstreamAddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		logger.Error("exit resolve udp target failed", "conn_id", actualID, "error", err)
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
		logger.Error("exit udp dial target failed",
			"conn_id", actualID,
			"target", targetAddr,
			"bind_ip", f.rule.BindIP,
			"cb_state", f.cb.State(),
			"error", err)
		f.sendUDPClose(actualID)
		return
	}

	// Dial succeeded, record success
	f.cb.RecordSuccess()

	client := &udpExitClient{
		clientAddr: clientAddr,
		upstream:   upstream,
	}
	client.lastActiveNano.Store(time.Now().UnixNano())

	f.udpConnsMu.Lock()
	f.udpConns[actualID] = client
	f.udpConnsMu.Unlock()

	logger.Debug("exit udp connection established", "conn_id", actualID, "client", clientAddr, "target", targetAddr)

	f.wg.Add(1)
	go f.udpUpstreamReadLoop(actualID, client)
}

// HandleData handles data message from tunnel (entry -> exit -> target).
// Uses async write queue to prevent blocking the tunnel read loop.
func (f *ExitForwarder) HandleData(connID uint64, data []byte) {
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
			// Queue full - target is accepting too slow, close connection to prevent resource leak
			logger.Error("exit write queue full, closing connection", "conn_id", connID, "len", len(data))
			f.closeTCPConn(connID)
		} else {
			logger.Debug("exit async write to tcp target failed", "conn_id", connID, "error", err)
			f.closeTCPConn(connID)
		}
	}
	// Note: traffic is counted in connState.writeLoop after actual write
}

// handleUDPData handles UDP data from tunnel and forwards to target.
func (f *ExitForwarder) handleUDPData(connID uint64, data []byte) {
	f.udpConnsMu.RLock()
	client, ok := f.udpConns[connID]
	f.udpConnsMu.RUnlock()

	if !ok {
		logger.Debug("exit udp unknown connID", "conn_id", connID)
		return
	}

	client.lastActiveNano.Store(time.Now().UnixNano())

	n, err := client.upstream.Write(data)
	if err != nil {
		logger.Debug("exit write to udp target failed", "conn_id", connID, "error", err)
		f.closeUDPConn(connID)
		return
	}
	f.traffic.AddUpload(int64(n))
}

// HandleClose handles close message from tunnel.
func (f *ExitForwarder) HandleClose(connID uint64) {
	if tunnel.IsUDPConnID(connID) {
		f.closeUDPConn(tunnel.GetConnIDValue(connID))
		return
	}
	f.closeTCPConn(connID)
}

func (f *ExitForwarder) readFromTarget(connID uint64, targetConn net.Conn) {
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

		n, err := targetConn.Read(buf)
		if err != nil {
			if err != io.EOF && !isClosedError(err) {
				logger.Debug("exit read from tcp target failed", "conn_id", connID, "error", err)
			}
			return
		}

		f.traffic.AddDownload(int64(n))

		if f.tunnel != nil {
			if err := f.tunnel.SendMessage(tunnel.NewDataMessage(connID, buf[:n])); err != nil {
				logger.Error("exit send tcp data message failed", "conn_id", connID, "error", err)
				return
			}
		}
	}
}

// udpUpstreamReadLoop reads responses from UDP target and sends back through tunnel.
func (f *ExitForwarder) udpUpstreamReadLoop(connID uint64, client *udpExitClient) {
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
				logger.Debug("exit udp upstream read error", "conn_id", connID, "error", err)
			}
			return
		}

		client.lastActiveNano.Store(time.Now().UnixNano())
		f.traffic.AddDownload(int64(n))

		// Send response back through tunnel
		if f.tunnel != nil {
			if err := f.tunnel.SendMessage(tunnel.NewUDPDataMessage(connID, buf[:n])); err != nil {
				logger.Error("exit send udp data message failed", "conn_id", connID, "error", err)
				return
			}
		}
	}
}

func (f *ExitForwarder) closeTCPConn(connID uint64) {
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
func (f *ExitForwarder) closeUDPConn(connID uint64) {
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
func (f *ExitForwarder) sendUDPClose(connID uint64) {
	if f.tunnel != nil {
		f.tunnel.SendMessage(tunnel.NewUDPCloseMessage(connID))
	}
}

// udpCleanupLoop periodically cleans up idle UDP connections.
func (f *ExitForwarder) udpCleanupLoop() {
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
func (f *ExitForwarder) cleanupIdleUDPClients() {
	now := time.Now()

	f.udpConnsMu.Lock()
	defer f.udpConnsMu.Unlock()

	for connID, client := range f.udpConns {
		lastActiveNano := client.lastActiveNano.Load()
		lastActive := time.Unix(0, lastActiveNano)
		if now.Sub(lastActive) > udpIdleTimeout {
			logger.Debug("exit removing idle udp client", "conn_id", connID, "client", client.clientAddr)
			if client.upstream != nil {
				client.upstream.Close()
			}
			delete(f.udpConns, connID)
		}
	}
}

// ListenPort returns 0 as ExitForwarder does not have a listening port.
func (f *ExitForwarder) ListenPort() uint16 {
	return 0
}

// Connections returns the current number of active connections (TCP + UDP).
func (f *ExitForwarder) Connections() int {
	f.connMu.RLock()
	tcpConns := len(f.conns)
	f.connMu.RUnlock()

	f.udpConnsMu.RLock()
	udpConns := len(f.udpConns)
	f.udpConnsMu.RUnlock()

	return tcpConns + udpConns
}
