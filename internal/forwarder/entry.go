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

// udpClientEntry tracks UDP client state for cleanup.
type udpClientEntry struct {
	connID         uint64
	lastActiveNano atomic.Int64 // Unix nanoseconds, safe for concurrent access
}

// EntryForwarder handles entry forwarding (local port -> WS tunnel -> exit agent).
type EntryForwarder struct {
	rule        *forward.Rule
	tcpListener net.Listener
	udpConn     *net.UDPConn
	traffic     *TrafficCounter

	tunnel tunnel.Sender
	connMu sync.RWMutex
	conns  map[uint64]*connState // connID -> TCP client connection state (async write)

	// Circuit breaker to prevent connection storms when tunnel is unavailable
	cb *circuitBreaker

	// UDP client tracking: clientAddr -> client state mapping
	udpClientsMu sync.RWMutex
	udpClients   map[string]*udpClientEntry // clientAddr -> client state
	udpConnIDs   map[uint64]*net.UDPAddr    // connID -> clientAddr (for response routing)

	nextConnID atomic.Uint64
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewEntryForwarder creates a new entry forwarder.
func NewEntryForwarder(rule *forward.Rule, t tunnel.Sender) *EntryForwarder {
	return &EntryForwarder{
		rule:       rule,
		tunnel:     t,
		traffic:    &TrafficCounter{},
		cb:         newCircuitBreaker(),
		conns:      make(map[uint64]*connState),
		udpClients: make(map[string]*udpClientEntry),
		udpConnIDs: make(map[uint64]*net.UDPAddr),
	}
}

// Start starts the entry forwarder.
func (f *EntryForwarder) Start(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	protocol := f.rule.Protocol
	if protocol == "" {
		protocol = "tcp"
	}

	switch protocol {
	case "tcp":
		if err := f.startTCP(); err != nil {
			return err
		}
	case "udp":
		if err := f.startUDP(); err != nil {
			return err
		}
	case "both":
		if err := f.startTCP(); err != nil {
			return err
		}
		if err := f.startUDP(); err != nil {
			f.tcpListener.Close()
			return err
		}
	default:
		return fmt.Errorf("unsupported protocol: %s", protocol)
	}

	logger.Info("entry forwarder started",
		"rule_id", f.rule.ID,
		"listen_port", f.rule.ListenPort,
		"exit_agent_id", f.rule.ExitAgentID,
		"protocol", protocol)

	return nil
}

// startTCP starts the TCP listener.
func (f *EntryForwarder) startTCP() error {
	addr := fmt.Sprintf(":%d", f.rule.ListenPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp listen on %s: %w", addr, err)
	}
	f.tcpListener = listener

	f.wg.Add(1)
	go f.tcpAcceptLoop()

	return nil
}

// startUDP starts the UDP listener.
func (f *EntryForwarder) startUDP() error {
	addr := fmt.Sprintf(":%d", f.rule.ListenPort)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("resolve udp addr %s: %w", addr, err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("udp listen on %s: %w", addr, err)
	}
	f.udpConn = conn

	f.wg.Add(2)
	go f.udpReadLoop()
	go f.udpCleanupLoop()

	return nil
}

// Stop stops the entry forwarder.
func (f *EntryForwarder) Stop() error {
	if f.cancel != nil {
		f.cancel()
	}
	if f.tcpListener != nil {
		f.tcpListener.Close()
	}
	if f.udpConn != nil {
		f.udpConn.Close()
	}

	// Close all TCP connections
	f.connMu.Lock()
	for _, cs := range f.conns {
		cs.Close()
	}
	f.conns = make(map[uint64]*connState)
	f.connMu.Unlock()

	// Clear UDP client mappings
	f.udpClientsMu.Lock()
	f.udpClients = make(map[string]*udpClientEntry)
	f.udpConnIDs = make(map[uint64]*net.UDPAddr)
	f.udpClientsMu.Unlock()

	f.wg.Wait()
	logger.Info("entry forwarder stopped", "rule_id", f.rule.ID)
	return nil
}

// Traffic returns the traffic counter.
func (f *EntryForwarder) Traffic() *TrafficCounter {
	return f.traffic
}

// RuleID returns the rule ID.
func (f *EntryForwarder) RuleID() string {
	return f.rule.ID
}

// IsTunnelConnected returns true if the tunnel is connected.
func (f *EntryForwarder) IsTunnelConnected() bool {
	if f.tunnel == nil {
		return false
	}
	// Check if the tunnel implements ConnectionChecker
	if checker, ok := f.tunnel.(interface{ IsConnected() bool }); ok {
		return checker.IsConnected()
	}
	// If no way to check, assume connected
	return true
}

// PingTunnel pings the tunnel and returns the round-trip latency.
// Returns an error if the tunnel doesn't support ping or is not connected.
func (f *EntryForwarder) PingTunnel(ctx context.Context) (time.Duration, error) {
	if f.tunnel == nil {
		return 0, fmt.Errorf("tunnel not initialized")
	}
	if pinger, ok := f.tunnel.(tunnel.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return 0, fmt.Errorf("tunnel does not support ping")
}

// HandleConnect is not used by EntryForwarder (it initiates connections, not receives).
func (f *EntryForwarder) HandleConnect(connID uint64) {
	// Not used - EntryForwarder is the initiator
}

// HandleConnectWithPayload is not used by EntryForwarder.
func (f *EntryForwarder) HandleConnectWithPayload(connID uint64, payload []byte) {
	// Not used - EntryForwarder is the initiator
}

// HandleData handles data received from tunnel (exit -> entry -> client).
// Uses async write queue to prevent blocking the tunnel read loop.
func (f *EntryForwarder) HandleData(connID uint64, data []byte) {
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
			// Queue full - client is reading too slow, close connection to prevent resource leak
			logger.Error("entry write queue full, closing connection", "conn_id", connID, "len", len(data))
			f.closeTCPConn(connID)
		} else {
			logger.Debug("entry async write to tcp client failed", "conn_id", connID, "error", err)
			f.closeTCPConn(connID)
		}
	}
	// Note: traffic is counted in connState.writeLoop after actual write
}

// handleUDPData handles UDP data from tunnel and sends to client.
func (f *EntryForwarder) handleUDPData(connID uint64, data []byte) {
	f.udpClientsMu.RLock()
	clientAddr, ok := f.udpConnIDs[connID]
	f.udpClientsMu.RUnlock()

	if !ok {
		logger.Debug("entry udp unknown connID", "conn_id", connID)
		return
	}

	// Update last active time for the client
	f.udpClientsMu.RLock()
	for _, client := range f.udpClients {
		if client.connID == connID {
			client.lastActiveNano.Store(time.Now().UnixNano())
			break
		}
	}
	f.udpClientsMu.RUnlock()

	n, err := f.udpConn.WriteToUDP(data, clientAddr)
	if err != nil {
		logger.Debug("entry write to udp client failed", "conn_id", connID, "error", err)
		return
	}
	f.traffic.AddDownload(int64(n))
}

// HandleClose handles close message from tunnel.
func (f *EntryForwarder) HandleClose(connID uint64) {
	if tunnel.IsUDPConnID(connID) {
		f.closeUDPConn(tunnel.GetConnIDValue(connID))
		return
	}
	f.closeTCPConn(connID)
}

func (f *EntryForwarder) tcpAcceptLoop() {
	runAcceptLoop(acceptLoopConfig{
		ctx:      f.ctx,
		listener: f.tcpListener,
		cb:       f.cb,
		wg:       &f.wg,
		logName:  "entry",
		handler:  f.handleTCPConn,
	})
}

func (f *EntryForwarder) handleTCPConn(clientConn net.Conn) {
	defer f.wg.Done()
	defer clientConn.Close()

	// Check circuit breaker before processing
	if !f.cb.Allow() {
		logger.Debug("entry connection rejected by circuit breaker",
			"rule_id", f.rule.ID,
			"state", f.cb.State())
		return
	}

	connID := f.nextConnID.Add(1)

	// Create connState with async write queue for download direction
	cs := newConnState(clientConn, f.traffic.AddDownload)

	f.connMu.Lock()
	f.conns[connID] = cs
	f.connMu.Unlock()

	defer f.closeTCPConn(connID)

	logger.Debug("entry new tcp connection", "conn_id", connID, "client", clientConn.RemoteAddr())

	if err := f.tunnel.SendMessage(tunnel.NewConnectMessage(connID)); err != nil {
		f.cb.RecordFailure()
		logger.Error("entry send connect message failed", "conn_id", connID, "error", err)
		return
	}
	f.cb.RecordSuccess()

	bufp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufp)
	buf := *bufp

	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

		n, err := clientConn.Read(buf)
		if err != nil {
			if err != io.EOF && !isClosedError(err) {
				logger.Debug("entry read from client failed", "conn_id", connID, "error", err)
			}
			return
		}

		f.traffic.AddUpload(int64(n))

		if err := f.tunnel.SendMessage(tunnel.NewDataMessage(connID, buf[:n])); err != nil {
			f.cb.RecordFailure()
			logger.Error("entry send data message failed", "conn_id", connID, "error", err)
			return
		}
	}
}

func (f *EntryForwarder) closeTCPConn(connID uint64) {
	f.connMu.Lock()
	cs, ok := f.conns[connID]
	if ok {
		delete(f.conns, connID)
	}
	f.connMu.Unlock()

	if ok {
		cs.Close()
		f.tunnel.SendMessage(tunnel.NewCloseMessage(connID))
	}
}

// udpReadLoop reads UDP packets from clients and forwards them through tunnel.
func (f *EntryForwarder) udpReadLoop() {
	defer f.wg.Done()

	buf := make([]byte, udpMaxPacketSize)

	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

		n, clientAddr, err := f.udpConn.ReadFromUDP(buf)
		if err != nil {
			if !isClosedError(err) && f.ctx.Err() == nil {
				logger.Error("entry udp read error", "error", err)
			}
			continue
		}

		f.traffic.AddUpload(int64(n))

		// Get or create connID for this UDP client
		connID := f.getOrCreateUDPConnID(clientAddr)

		// Send data through tunnel (using UDP-flagged connID)
		if err := f.tunnel.SendMessage(tunnel.NewUDPDataMessage(connID, buf[:n])); err != nil {
			logger.Error("entry send udp data failed", "conn_id", connID, "error", err)
		}
	}
}

// getOrCreateUDPConnID gets or creates a connID for a UDP client.
// Uses double-check locking to prevent race conditions.
func (f *EntryForwarder) getOrCreateUDPConnID(clientAddr *net.UDPAddr) uint64 {
	key := clientAddr.String()

	// Fast path: check with read lock
	f.udpClientsMu.RLock()
	client, exists := f.udpClients[key]
	f.udpClientsMu.RUnlock()

	if exists {
		// Update last active time
		client.lastActiveNano.Store(time.Now().UnixNano())
		return client.connID
	}

	// Slow path: create with write lock
	f.udpClientsMu.Lock()

	// Double-check: another goroutine may have created it
	if client, exists = f.udpClients[key]; exists {
		f.udpClientsMu.Unlock()
		client.lastActiveNano.Store(time.Now().UnixNano())
		return client.connID
	}

	// Create new connID while holding the lock
	connID := f.nextConnID.Add(1)
	client = &udpClientEntry{connID: connID}
	client.lastActiveNano.Store(time.Now().UnixNano())
	f.udpClients[key] = client
	f.udpConnIDs[connID] = clientAddr
	f.udpClientsMu.Unlock()

	// Send connect message with client address (for Exit to know where to respond)
	if err := f.tunnel.SendMessage(tunnel.NewUDPConnectMessage(connID, key)); err != nil {
		logger.Error("entry send udp connect failed", "conn_id", connID, "error", err)
	}

	logger.Debug("entry udp client registered", "conn_id", connID, "client", key)
	return connID
}

// closeUDPConn removes a UDP client mapping.
func (f *EntryForwarder) closeUDPConn(connID uint64) {
	f.udpClientsMu.Lock()
	defer f.udpClientsMu.Unlock()

	if clientAddr, ok := f.udpConnIDs[connID]; ok {
		key := clientAddr.String()
		delete(f.udpClients, key)
		delete(f.udpConnIDs, connID)
		logger.Debug("entry udp client removed", "conn_id", connID, "client", key)
	}
}

// udpCleanupLoop periodically cleans up idle UDP clients.
func (f *EntryForwarder) udpCleanupLoop() {
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
func (f *EntryForwarder) cleanupIdleUDPClients() {
	now := time.Now()
	var toRemove []string

	f.udpClientsMu.RLock()
	for key, client := range f.udpClients {
		lastActiveNano := client.lastActiveNano.Load()
		lastActive := time.Unix(0, lastActiveNano)
		if now.Sub(lastActive) > udpIdleTimeout {
			toRemove = append(toRemove, key)
		}
	}
	f.udpClientsMu.RUnlock()

	if len(toRemove) == 0 {
		return
	}

	f.udpClientsMu.Lock()
	for _, key := range toRemove {
		if client, ok := f.udpClients[key]; ok {
			// Double-check: client may have been active since we released the lock
			lastActiveNano := client.lastActiveNano.Load()
			lastActive := time.Unix(0, lastActiveNano)
			if now.Sub(lastActive) > udpIdleTimeout {
				connID := client.connID
				delete(f.udpClients, key)
				delete(f.udpConnIDs, connID)
				logger.Debug("entry removing idle udp client", "conn_id", connID, "client", key)

				// Send close message to tunnel (outside of lock would be better, but acceptable)
				f.tunnel.SendMessage(tunnel.NewCloseMessage(tunnel.MakeUDPConnID(connID)))
			}
		}
	}
	f.udpClientsMu.Unlock()
}

// ListenPort returns the actual listening port from the listener or UDP connection.
func (f *EntryForwarder) ListenPort() uint16 {
	if f.tcpListener != nil {
		if addr, ok := f.tcpListener.Addr().(*net.TCPAddr); ok {
			return uint16(addr.Port)
		}
	}
	if f.udpConn != nil {
		if addr, ok := f.udpConn.LocalAddr().(*net.UDPAddr); ok {
			return uint16(addr.Port)
		}
	}
	return 0
}

// Connections returns the current number of active connections (TCP + UDP).
func (f *EntryForwarder) Connections() int {
	f.connMu.RLock()
	tcpConns := len(f.conns)
	f.connMu.RUnlock()

	f.udpClientsMu.RLock()
	udpConns := len(f.udpConnIDs)
	f.udpClientsMu.RUnlock()

	return tcpConns + udpConns
}
