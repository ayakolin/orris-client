package forwarder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
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
	tunnelIdx      int           // index of tunnel used by this UDP client
	lastActiveNano atomic.Int64  // Unix nanoseconds, safe for concurrent access
}

// tunnelEntry represents a tunnel with its weight for load balancing.
type tunnelEntry struct {
	tunnel tunnel.Sender
	weight uint16
}

// EntryForwarder handles entry forwarding (local port -> WS tunnel -> exit agent).
type EntryForwarder struct {
	rule        *forward.Rule
	tcpListener net.Listener
	udpConn     *net.UDPConn
	traffic     *TrafficCounter

	// Single tunnel mode (backward compatible)
	tunnel tunnel.Sender

	// Multi-tunnel mode for load balancing
	tunnels             []tunnelEntry           // multiple tunnels with weights
	totalWeight         uint32                  // sum of all weights for random selection
	loadBalanceStrategy forward.LoadBalanceStrategy // load balance strategy: failover (default), weighted

	// Health checker for multi-tunnel mode (nil if health check disabled)
	healthChecker *HealthChecker

	connMu     sync.RWMutex
	conns      map[uint64]*connState // connID -> TCP client connection state (async write)
	connTunnel map[uint64]int        // connID -> tunnel index (for multi-tunnel mode)

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

// NewEntryForwarder creates a new entry forwarder with a single tunnel.
func NewEntryForwarder(rule *forward.Rule, t tunnel.Sender) *EntryForwarder {
	return &EntryForwarder{
		rule:       rule,
		tunnel:     t,
		traffic:    &TrafficCounter{},
		cb:         newCircuitBreaker(),
		conns:      make(map[uint64]*connState),
		connTunnel: make(map[uint64]int),
		udpClients: make(map[string]*udpClientEntry),
		udpConnIDs: make(map[uint64]*net.UDPAddr),
	}
}

// NewEntryForwarderWithTunnels creates a new entry forwarder with multiple tunnels for load balancing.
// If healthConfig is provided and there are multiple tunnels, a HealthChecker will be created.
// exitAgentIDs should match the tunnel indices for health reporting.
// Panics if tunnels and weights have different lengths.
func NewEntryForwarderWithTunnels(rule *forward.Rule, tunnels []tunnel.TunnelClient, weights []uint16, healthConfig *forward.HealthCheckConfig, exitAgentIDs []string) *EntryForwarder {
	if len(tunnels) != len(weights) {
		panic(fmt.Sprintf("tunnels and weights length mismatch: %d vs %d", len(tunnels), len(weights)))
	}

	entries := make([]tunnelEntry, len(tunnels))
	var totalWeight uint32
	for i, t := range tunnels {
		entries[i] = tunnelEntry{tunnel: t, weight: weights[i]}
		totalWeight += uint32(weights[i])
	}

	ef := &EntryForwarder{
		rule:                rule,
		tunnels:             entries,
		totalWeight:         totalWeight,
		loadBalanceStrategy: rule.LoadBalanceStrategy,
		traffic:             &TrafficCounter{},
		cb:                  newCircuitBreaker(),
		conns:               make(map[uint64]*connState),
		connTunnel:          make(map[uint64]int),
		udpClients:          make(map[string]*udpClientEntry),
		udpConnIDs:          make(map[uint64]*net.UDPAddr),
	}

	// Enable health checker for multi-tunnel mode (only if more than 1 tunnel)
	if len(tunnels) > 1 {
		ef.healthChecker = NewHealthChecker(rule.ID, len(tunnels), healthConfig, exitAgentIDs)
		logger.Info("health checker enabled",
			"rule_id", rule.ID,
			"tunnel_count", len(tunnels),
			"load_balance_strategy", string(ef.loadBalanceStrategy))
	}

	return ef
}

// SetHealthChangeCallback sets the callback for health status changes.
// This should be called after creating the forwarder to enable health reporting.
func (f *EntryForwarder) SetHealthChangeCallback(callback HealthChangeCallback) {
	if f.healthChecker != nil {
		f.healthChecker.SetOnHealthChange(callback)
	}
}

// isMultiTunnel returns true if using multiple tunnels for load balancing.
func (f *EntryForwarder) isMultiTunnel() bool {
	return len(f.tunnels) > 0
}

// selectTunnel selects a tunnel based on the load balance strategy.
// If health checker is enabled, only healthy tunnels are considered.
// Returns the tunnel and its index.
func (f *EntryForwarder) selectTunnel() (tunnel.Sender, int) {
	if !f.isMultiTunnel() {
		return f.tunnel, 0
	}

	if len(f.tunnels) == 1 {
		return f.tunnels[0].tunnel, 0
	}

	// If health checker is enabled, select from healthy tunnels only
	if f.healthChecker != nil {
		healthyIndices := f.healthChecker.GetHealthyIndices()
		if len(healthyIndices) == 0 {
			// All unhealthy: fallback to any tunnel (best effort)
			logger.Warn("all tunnels unhealthy, selecting fallback",
				"rule_id", f.rule.ID)
			return f.selectAnyTunnel()
		}
		return f.selectFromIndices(healthyIndices)
	}

	// No health checker, use original logic
	return f.selectAnyTunnel()
}

// selectAnyTunnel selects a tunnel from all available tunnels based on strategy.
func (f *EntryForwarder) selectAnyTunnel() (tunnel.Sender, int) {
	if f.loadBalanceStrategy.IsFailover() {
		return f.selectFailover(nil)
	}
	return f.selectWeighted(nil)
}

// selectFromIndices selects a tunnel from the given indices based on strategy.
func (f *EntryForwarder) selectFromIndices(indices []int) (tunnel.Sender, int) {
	if len(indices) == 1 {
		idx := indices[0]
		return f.tunnels[idx].tunnel, idx
	}

	if f.loadBalanceStrategy.IsFailover() {
		return f.selectFailover(indices)
	}
	return f.selectWeighted(indices)
}

// selectFailover selects tunnel using failover strategy (priority-based).
// Tunnels are selected by weight (highest first), weight=0 serves as backup.
// If indices is nil, all tunnels are considered.
func (f *EntryForwarder) selectFailover(indices []int) (tunnel.Sender, int) {
	// Defensive check
	if len(f.tunnels) == 0 {
		return nil, -1
	}

	var bestIdx = -1
	var bestWeight uint16 = 0
	var backupIdx = -1 // backup tunnel index (weight=0)

	if indices == nil {
		// Consider all tunnels
		for i, entry := range f.tunnels {
			if entry.weight == 0 {
				if backupIdx == -1 {
					backupIdx = i
				}
				continue
			}
			if bestIdx == -1 || entry.weight > bestWeight {
				bestIdx = i
				bestWeight = entry.weight
			}
		}
	} else {
		// Consider only specified indices
		for _, i := range indices {
			if i < 0 || i >= len(f.tunnels) {
				continue // skip invalid index
			}
			entry := f.tunnels[i]
			if entry.weight == 0 {
				if backupIdx == -1 {
					backupIdx = i
				}
				continue
			}
			if bestIdx == -1 || entry.weight > bestWeight {
				bestIdx = i
				bestWeight = entry.weight
			}
		}
	}

	// Use best tunnel if found, otherwise use backup
	if bestIdx != -1 {
		return f.tunnels[bestIdx].tunnel, bestIdx
	}
	if backupIdx != -1 {
		return f.tunnels[backupIdx].tunnel, backupIdx
	}

	// Fallback: first available tunnel
	if indices == nil {
		return f.tunnels[0].tunnel, 0
	}
	if len(indices) > 0 && indices[0] >= 0 && indices[0] < len(f.tunnels) {
		return f.tunnels[indices[0]].tunnel, indices[0]
	}
	return f.tunnels[0].tunnel, 0
}

// selectWeighted selects tunnel using weighted random distribution.
// weight=0 tunnels are excluded unless all weights are 0.
// If indices is nil, all tunnels are considered.
func (f *EntryForwarder) selectWeighted(indices []int) (tunnel.Sender, int) {
	// Defensive check
	if len(f.tunnels) == 0 {
		return nil, -1
	}

	var totalWeight uint32
	var candidates []int

	if indices == nil {
		// Consider all tunnels
		candidates = make([]int, 0, len(f.tunnels))
		for i, entry := range f.tunnels {
			if entry.weight > 0 {
				candidates = append(candidates, i)
				totalWeight += uint32(entry.weight)
			}
		}
		// If all weights are 0, consider all tunnels with uniform distribution
		if len(candidates) == 0 {
			idx := rand.Intn(len(f.tunnels))
			return f.tunnels[idx].tunnel, idx
		}
	} else {
		// Consider only specified indices
		candidates = make([]int, 0, len(indices))
		for _, i := range indices {
			if i < 0 || i >= len(f.tunnels) {
				continue // skip invalid index
			}
			if f.tunnels[i].weight > 0 {
				candidates = append(candidates, i)
				totalWeight += uint32(f.tunnels[i].weight)
			}
		}
		// If all weights are 0, consider all specified indices with uniform distribution
		if len(candidates) == 0 {
			// Filter valid indices first
			validIndices := make([]int, 0, len(indices))
			for _, i := range indices {
				if i >= 0 && i < len(f.tunnels) {
					validIndices = append(validIndices, i)
				}
			}
			if len(validIndices) == 0 {
				return f.tunnels[0].tunnel, 0
			}
			idx := validIndices[rand.Intn(len(validIndices))]
			return f.tunnels[idx].tunnel, idx
		}
	}

	// Single candidate
	if len(candidates) == 1 {
		return f.tunnels[candidates[0]].tunnel, candidates[0]
	}

	// Weighted random selection
	r := rand.Uint32() % totalWeight
	var cumulative uint32
	for _, i := range candidates {
		cumulative += uint32(f.tunnels[i].weight)
		if r < cumulative {
			return f.tunnels[i].tunnel, i
		}
	}

	// Fallback
	idx := candidates[len(candidates)-1]
	return f.tunnels[idx].tunnel, idx
}

// getTunnelByIndex returns the tunnel at the given index.
func (f *EntryForwarder) getTunnelByIndex(idx int) tunnel.Sender {
	if !f.isMultiTunnel() {
		return f.tunnel
	}
	if idx >= 0 && idx < len(f.tunnels) {
		return f.tunnels[idx].tunnel
	}
	return f.tunnels[0].tunnel
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

	// Log exit agent info (prefer ExitAgents for load balancing, fallback to ExitAgentID)
	exitAgentInfo := f.rule.ExitAgentID
	if len(f.rule.ExitAgents) > 0 {
		exitAgentInfo = fmt.Sprintf("%d agents (load balancing)", len(f.rule.ExitAgents))
	}

	logger.Info("entry forwarder started",
		"rule_id", f.rule.ID,
		"listen_port", f.rule.ListenPort,
		"exit_agents", exitAgentInfo,
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

// IsTunnelConnected returns true if at least one tunnel is connected.
func (f *EntryForwarder) IsTunnelConnected() bool {
	if f.isMultiTunnel() {
		// Check if any tunnel is connected
		for _, entry := range f.tunnels {
			if checker, ok := entry.tunnel.(interface{ IsConnected() bool }); ok {
				if checker.IsConnected() {
					return true
				}
			} else {
				// If no way to check, assume connected
				return true
			}
		}
		return false
	}

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

// PingTunnel pings the first available tunnel and returns the round-trip latency.
// Returns an error if the tunnel doesn't support ping or is not connected.
func (f *EntryForwarder) PingTunnel(ctx context.Context) (time.Duration, error) {
	if f.isMultiTunnel() {
		// Ping the first tunnel that supports it
		for _, entry := range f.tunnels {
			if pinger, ok := entry.tunnel.(tunnel.Pinger); ok {
				return pinger.Ping(ctx)
			}
		}
		return 0, fmt.Errorf("no tunnel supports ping")
	}

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

	// Select tunnel for this connection (load balancing)
	selectedTunnel, tunnelIdx := f.selectTunnel()

	// Create connState with async write queue for download direction
	cs := newConnState(clientConn, f.traffic.AddDownload)

	f.connMu.Lock()
	f.conns[connID] = cs
	f.connTunnel[connID] = tunnelIdx
	f.connMu.Unlock()

	defer f.closeTCPConn(connID)

	logger.Debug("entry new tcp connection", "conn_id", connID, "client", clientConn.RemoteAddr(), "tunnel_idx", tunnelIdx)

	if err := selectedTunnel.SendMessage(tunnel.NewConnectMessage(connID)); err != nil {
		f.cb.RecordFailure()
		// Record health check failure with error message
		if f.healthChecker != nil {
			f.healthChecker.RecordFailureWithError(tunnelIdx, err.Error())
		}
		logger.Error("entry send connect message failed", "conn_id", connID, "error", err)
		return
	}
	f.cb.RecordSuccess()
	// Record health check success
	if f.healthChecker != nil {
		f.healthChecker.RecordSuccess(tunnelIdx)
	}

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

		if err := selectedTunnel.SendMessage(tunnel.NewDataMessage(connID, buf[:n])); err != nil {
			f.cb.RecordFailure()
			// Record health check failure with error message
			if f.healthChecker != nil {
				f.healthChecker.RecordFailureWithError(tunnelIdx, err.Error())
			}
			logger.Error("entry send data message failed", "conn_id", connID, "error", err)
			return
		}
	}
}

func (f *EntryForwarder) closeTCPConn(connID uint64) {
	f.connMu.Lock()
	cs, ok := f.conns[connID]
	tunnelIdx := f.connTunnel[connID]
	if ok {
		delete(f.conns, connID)
		delete(f.connTunnel, connID)
	}
	f.connMu.Unlock()

	if ok {
		cs.Close()
		// Send close message through the same tunnel used by this connection
		t := f.getTunnelByIndex(tunnelIdx)
		t.SendMessage(tunnel.NewCloseMessage(connID))
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

		// Get or create connID for this UDP client (with load balancing)
		connID, tunnelIdx := f.getOrCreateUDPConnID(clientAddr)

		// Send data through the assigned tunnel (using UDP-flagged connID)
		t := f.getTunnelByIndex(tunnelIdx)
		if err := t.SendMessage(tunnel.NewUDPDataMessage(connID, buf[:n])); err != nil {
			logger.Error("entry send udp data failed", "conn_id", connID, "error", err)
		}
	}
}

// getOrCreateUDPConnID gets or creates a connID for a UDP client.
// Uses double-check locking to prevent race conditions.
// Returns connID and tunnel index.
func (f *EntryForwarder) getOrCreateUDPConnID(clientAddr *net.UDPAddr) (uint64, int) {
	key := clientAddr.String()

	// Fast path: check with read lock
	f.udpClientsMu.RLock()
	client, exists := f.udpClients[key]
	f.udpClientsMu.RUnlock()

	if exists {
		// Update last active time
		client.lastActiveNano.Store(time.Now().UnixNano())
		return client.connID, client.tunnelIdx
	}

	// Slow path: create with write lock
	f.udpClientsMu.Lock()

	// Double-check: another goroutine may have created it
	if client, exists = f.udpClients[key]; exists {
		f.udpClientsMu.Unlock()
		client.lastActiveNano.Store(time.Now().UnixNano())
		return client.connID, client.tunnelIdx
	}

	// Select tunnel for this UDP client (load balancing)
	selectedTunnel, tunnelIdx := f.selectTunnel()

	// Create new connID while holding the lock
	connID := f.nextConnID.Add(1)
	client = &udpClientEntry{connID: connID, tunnelIdx: tunnelIdx}
	client.lastActiveNano.Store(time.Now().UnixNano())
	f.udpClients[key] = client
	f.udpConnIDs[connID] = clientAddr
	f.udpClientsMu.Unlock()

	// Send connect message with client address (for Exit to know where to respond)
	if err := selectedTunnel.SendMessage(tunnel.NewUDPConnectMessage(connID, key)); err != nil {
		logger.Error("entry send udp connect failed", "conn_id", connID, "error", err)
	}

	logger.Debug("entry udp client registered", "conn_id", connID, "client", key, "tunnel_idx", tunnelIdx)
	return connID, tunnelIdx
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
				tunnelIdx := client.tunnelIdx
				delete(f.udpClients, key)
				delete(f.udpConnIDs, connID)
				logger.Debug("entry removing idle udp client", "conn_id", connID, "client", key)

				// Send close message through the same tunnel used by this UDP client
				t := f.getTunnelByIndex(tunnelIdx)
				t.SendMessage(tunnel.NewCloseMessage(tunnel.MakeUDPConnID(connID)))
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
