package forwarder

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"

	"github.com/orris-inc/orris-client/internal/logger"
)

// DirectChainForwarder handles direct chain forwarding without WS tunnel.
// It supports TCP and UDP protocols with direct TCP/UDP connections between hops.
type DirectChainForwarder struct {
	rule    *forward.Rule
	traffic *TrafficCounter

	tcpListener net.Listener
	udpConn     *net.UDPConn

	// Circuit breaker to prevent connection storms when downstream is unreachable
	cb *circuitBreaker

	// UDP client tracking for response routing
	udpClientsMu sync.RWMutex
	udpClients   map[string]*udpClient // client addr -> upstream conn

	// Active TCP connections tracking
	activeConns atomic.Int32
	tcpConnsMu  sync.Mutex
	tcpConns    map[net.Conn]struct{} // active TCP connections for graceful shutdown

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDirectChainForwarder creates a new direct chain forwarder.
func NewDirectChainForwarder(rule *forward.Rule) *DirectChainForwarder {
	return &DirectChainForwarder{
		rule:       rule,
		traffic:    &TrafficCounter{},
		cb:         newCircuitBreaker(),
		udpClients: make(map[string]*udpClient),
		tcpConns:   make(map[net.Conn]struct{}),
	}
}

// Start starts the direct chain forwarder.
func (f *DirectChainForwarder) Start(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	// Log rule details for debugging
	logger.Debug("direct chain rule details",
		"rule_id", f.rule.ID,
		"role", f.rule.Role,
		"is_last_in_chain", f.rule.IsLastInChain,
		"next_hop_address", f.rule.NextHopAddress,
		"next_hop_port", f.rule.NextHopPort,
		"target_address", f.rule.TargetAddress,
		"target_port", f.rule.TargetPort)

	// Determine next hop address
	var nextHop string
	if f.rule.IsLastInChain {
		// Exit node: connect to final target
		if f.rule.TargetAddress == "" {
			return fmt.Errorf("target address is empty for exit node")
		}
		nextHop = net.JoinHostPort(f.rule.TargetAddress, fmt.Sprintf("%d", f.rule.TargetPort))
	} else {
		// Entry or Relay: connect to next hop
		if f.rule.NextHopAddress == "" {
			return fmt.Errorf("next hop address is empty for non-exit node")
		}
		nextHop = net.JoinHostPort(f.rule.NextHopAddress, fmt.Sprintf("%d", f.rule.NextHopPort))
	}

	// Prevent self-connection loop that would cause FD exhaustion
	if f.rule.ListenPort == f.rule.NextHopPort && !f.rule.IsLastInChain {
		if isLocalAddress(f.rule.NextHopAddress) {
			return fmt.Errorf("next hop would connect to self (listen=%d, next_hop=%s:%d)",
				f.rule.ListenPort, f.rule.NextHopAddress, f.rule.NextHopPort)
		}
	}

	protocol := f.rule.Protocol
	if protocol == "" {
		protocol = "tcp"
	}

	switch protocol {
	case "tcp":
		if err := f.startTCP(nextHop); err != nil {
			return err
		}
	case "udp":
		if err := f.startUDP(nextHop); err != nil {
			return err
		}
	case "both":
		if err := f.startTCP(nextHop); err != nil {
			return err
		}
		if err := f.startUDP(nextHop); err != nil {
			f.tcpListener.Close()
			return err
		}
	default:
		return fmt.Errorf("unsupported protocol: %s", protocol)
	}

	logger.Info("direct chain forwarder started",
		"rule_id", f.rule.ID,
		"listen_port", f.rule.ListenPort,
		"next_hop", nextHop,
		"protocol", protocol,
		"role", f.rule.Role)

	return nil
}

// startTCP starts the TCP listener and accept loop.
func (f *DirectChainForwarder) startTCP(nextHop string) error {
	addr := fmt.Sprintf(":%d", f.rule.ListenPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp listen on %s: %w", addr, err)
	}
	f.tcpListener = listener

	f.wg.Add(1)
	go f.tcpAcceptLoop(nextHop)

	return nil
}

// startUDP starts the UDP listener.
func (f *DirectChainForwarder) startUDP(nextHop string) error {
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
	go f.udpReadLoop(nextHop)
	go f.udpCleanupLoop()

	return nil
}

// Stop stops the direct chain forwarder.
func (f *DirectChainForwarder) Stop() error {
	if f.cancel != nil {
		f.cancel()
	}

	if f.tcpListener != nil {
		f.tcpListener.Close()
	}

	if f.udpConn != nil {
		f.udpConn.Close()
	}

	// Close all active TCP connections to unblock io.Copy
	f.tcpConnsMu.Lock()
	for conn := range f.tcpConns {
		conn.Close()
	}
	f.tcpConns = make(map[net.Conn]struct{})
	f.tcpConnsMu.Unlock()

	// Close all UDP upstream connections
	f.udpClientsMu.Lock()
	for _, client := range f.udpClients {
		if client.upstream != nil {
			client.upstream.Close()
		}
	}
	f.udpClients = make(map[string]*udpClient)
	f.udpClientsMu.Unlock()

	f.wg.Wait()
	logger.Info("direct chain forwarder stopped", "rule_id", f.rule.ID)
	return nil
}

// Traffic returns the traffic counter.
func (f *DirectChainForwarder) Traffic() *TrafficCounter {
	return f.traffic
}

// RuleID returns the rule ID.
func (f *DirectChainForwarder) RuleID() string {
	return f.rule.ID
}

// tcpAcceptLoop accepts TCP connections and handles them.
func (f *DirectChainForwarder) tcpAcceptLoop(nextHop string) {
	runAcceptLoop(acceptLoopConfig{
		ctx:      f.ctx,
		listener: f.tcpListener,
		cb:       f.cb,
		wg:       &f.wg,
		logName:  "direct chain",
		handler: func(conn net.Conn) {
			f.handleTCPConn(conn, nextHop)
		},
	})
}

// handleTCPConn handles a single TCP connection.
func (f *DirectChainForwarder) handleTCPConn(clientConn net.Conn, nextHop string) {
	defer f.wg.Done()
	defer clientConn.Close()

	// Track this connection for graceful shutdown
	f.tcpConnsMu.Lock()
	f.tcpConns[clientConn] = struct{}{}
	f.tcpConnsMu.Unlock()
	defer func() {
		f.tcpConnsMu.Lock()
		delete(f.tcpConns, clientConn)
		f.tcpConnsMu.Unlock()
	}()

	f.activeConns.Add(1)
	defer f.activeConns.Add(-1)

	// Check circuit breaker before attempting dial
	if !f.cb.Allow() {
		logger.Debug("direct chain connection rejected by circuit breaker",
			"rule_id", f.rule.ID,
			"next_hop", nextHop,
			"state", f.cb.State())
		return
	}

	// Connect to next hop with optional bind IP
	dialer := &net.Dialer{Timeout: 500 * time.Millisecond}
	if f.rule.BindIP != "" {
		dialer.LocalAddr = &net.TCPAddr{IP: net.ParseIP(f.rule.BindIP)}
	}

	upstreamConn, err := dialer.Dial("tcp", nextHop)
	if err != nil {
		f.cb.RecordFailure()
		logger.Error("direct chain tcp dial failed",
			"rule_id", f.rule.ID,
			"next_hop", nextHop,
			"bind_ip", f.rule.BindIP,
			"cb_state", f.cb.State(),
			"error", err)
		return
	}

	// Dial succeeded, record success
	f.cb.RecordSuccess()
	defer upstreamConn.Close()

	// Track upstream connection for graceful shutdown
	f.tcpConnsMu.Lock()
	f.tcpConns[upstreamConn] = struct{}{}
	f.tcpConnsMu.Unlock()
	defer func() {
		f.tcpConnsMu.Lock()
		delete(f.tcpConns, upstreamConn)
		f.tcpConnsMu.Unlock()
	}()

	logger.Debug("direct chain tcp connection established",
		"rule_id", f.rule.ID,
		"client", clientConn.RemoteAddr(),
		"next_hop", nextHop)

	var wg sync.WaitGroup
	wg.Add(2)

	// Client -> Upstream (upload)
	// Use copyWithTraffic for zero-copy via splice(2) on Linux
	go func() {
		defer wg.Done()
		copyWithTraffic(f.ctx, upstreamConn, clientConn, f.traffic.AddUpload)
		if tc, ok := upstreamConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	// Upstream -> Client (download)
	go func() {
		defer wg.Done()
		copyWithTraffic(f.ctx, clientConn, upstreamConn, f.traffic.AddDownload)
		if tc, ok := clientConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	wg.Wait()
}

// udpReadLoop reads UDP packets and forwards them.
func (f *DirectChainForwarder) udpReadLoop(nextHop string) {
	defer f.wg.Done()

	buf := make([]byte, 65535) // max UDP packet size

	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

		n, clientAddr, err := f.udpConn.ReadFromUDP(buf)
		if err != nil {
			if !isClosedError(err) && f.ctx.Err() == nil {
				logger.Error("direct chain udp read error", "error", err)
			}
			continue
		}

		f.traffic.AddUpload(int64(n))

		// Get or create upstream connection for this client
		client := f.getOrCreateUDPClient(clientAddr, nextHop)
		if client == nil {
			continue
		}

		// Forward packet to upstream
		_, err = client.upstream.Write(buf[:n])
		if err != nil {
			logger.Debug("direct chain udp write to upstream failed",
				"client", clientAddr,
				"error", err)
			f.removeUDPClient(clientAddr.String())
		}
	}
}

// getOrCreateUDPClient gets or creates a UDP client connection.
// Uses double-check locking to prevent race conditions.
func (f *DirectChainForwarder) getOrCreateUDPClient(clientAddr *net.UDPAddr, nextHop string) *udpClient {
	key := clientAddr.String()

	// Fast path: check with read lock
	f.udpClientsMu.RLock()
	client, exists := f.udpClients[key]
	f.udpClientsMu.RUnlock()

	if exists {
		client.lastActiveNano.Store(time.Now().UnixNano())
		return client
	}

	// Check circuit breaker before creating new connection
	if !f.cb.Allow() {
		logger.Debug("direct chain udp connection rejected by circuit breaker",
			"next_hop", nextHop,
			"state", f.cb.State())
		return nil
	}

	// Slow path: create with write lock
	f.udpClientsMu.Lock()

	// Double-check: another goroutine may have created it
	if client, exists = f.udpClients[key]; exists {
		f.udpClientsMu.Unlock()
		client.lastActiveNano.Store(time.Now().UnixNano())
		return client
	}

	// Create new upstream connection with optional bind IP
	upstreamAddr, err := net.ResolveUDPAddr("udp", nextHop)
	if err != nil {
		f.udpClientsMu.Unlock()
		logger.Error("direct chain resolve upstream addr failed",
			"next_hop", nextHop,
			"error", err)
		return nil
	}

	var localAddr *net.UDPAddr
	if f.rule.BindIP != "" {
		localAddr = &net.UDPAddr{IP: net.ParseIP(f.rule.BindIP)}
	}

	upstream, err := net.DialUDP("udp", localAddr, upstreamAddr)
	if err != nil {
		f.udpClientsMu.Unlock()
		f.cb.RecordFailure()
		logger.Error("direct chain udp dial upstream failed",
			"next_hop", nextHop,
			"bind_ip", f.rule.BindIP,
			"cb_state", f.cb.State(),
			"error", err)
		return nil
	}

	// Dial succeeded, record success
	f.cb.RecordSuccess()

	client = &udpClient{
		clientAddr: clientAddr,
		upstream:   upstream,
	}
	client.lastActiveNano.Store(time.Now().UnixNano())

	f.udpClients[key] = client
	f.udpClientsMu.Unlock()

	// Start goroutine to read responses from upstream
	f.wg.Add(1)
	go f.udpUpstreamReadLoop(client)

	logger.Debug("direct chain udp client created",
		"rule_id", f.rule.ID,
		"client", clientAddr,
		"next_hop", nextHop)

	return client
}

// udpUpstreamReadLoop reads responses from upstream and sends to client.
func (f *DirectChainForwarder) udpUpstreamReadLoop(client *udpClient) {
	defer f.wg.Done()

	buf := make([]byte, 65535)

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
				logger.Debug("direct chain udp upstream read error",
					"client", client.clientAddr,
					"error", err)
			}
			f.removeUDPClient(client.clientAddr.String())
			return
		}

		client.lastActiveNano.Store(time.Now().UnixNano())
		f.traffic.AddDownload(int64(n))

		// Send response back to client
		_, err = f.udpConn.WriteToUDP(buf[:n], client.clientAddr)
		if err != nil {
			logger.Debug("direct chain udp write to client failed",
				"client", client.clientAddr,
				"error", err)
		}
	}
}

// udpCleanupLoop periodically cleans up idle UDP clients.
func (f *DirectChainForwarder) udpCleanupLoop() {
	defer f.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	idleTimeout := 2 * time.Minute

	for {
		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			f.cleanupIdleUDPClients(idleTimeout)
		}
	}
}

// cleanupIdleUDPClients removes idle UDP clients.
func (f *DirectChainForwarder) cleanupIdleUDPClients(timeout time.Duration) {
	now := time.Now()

	f.udpClientsMu.Lock()
	defer f.udpClientsMu.Unlock()

	for key, client := range f.udpClients {
		lastActiveNano := client.lastActiveNano.Load()
		lastActive := time.Unix(0, lastActiveNano)
		if now.Sub(lastActive) > timeout {
			logger.Debug("direct chain removing idle udp client",
				"client", client.clientAddr)
			client.upstream.Close()
			delete(f.udpClients, key)
		}
	}
}

// removeUDPClient removes a UDP client by key.
func (f *DirectChainForwarder) removeUDPClient(key string) {
	f.udpClientsMu.Lock()
	defer f.udpClientsMu.Unlock()

	if client, exists := f.udpClients[key]; exists {
		client.upstream.Close()
		delete(f.udpClients, key)
	}
}

// ListenPort returns the actual listening port from the listener or UDP connection.
func (f *DirectChainForwarder) ListenPort() uint16 {
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

// Connections returns the current number of active TCP connections.
// For UDP, returns the number of tracked client connections.
func (f *DirectChainForwarder) Connections() int {
	tcpConns := int(f.activeConns.Load())

	f.udpClientsMu.RLock()
	udpConns := len(f.udpClients)
	f.udpClientsMu.RUnlock()

	return tcpConns + udpConns
}
