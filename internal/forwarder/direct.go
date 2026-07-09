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

// DirectForwarder handles direct forwarding (local port -> target).
type DirectForwarder struct {
	rule        *forward.Rule
	tcpListener net.Listener
	udpConn     *net.UDPConn
	traffic     *TrafficCounter

	// Circuit breaker to prevent connection storms when target is unreachable
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

// NewDirectForwarder creates a new direct forwarder.
func NewDirectForwarder(rule *forward.Rule) *DirectForwarder {
	return &DirectForwarder{
		rule:       rule,
		traffic:    &TrafficCounter{},
		cb:         newCircuitBreaker(),
		udpClients: make(map[string]*udpClient),
		tcpConns:   make(map[net.Conn]struct{}),
	}
}

// Start starts the direct forwarder.
func (f *DirectForwarder) Start(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	// Validate target address
	if f.rule.TargetAddress == "" {
		return fmt.Errorf("target address is empty")
	}

	// Prevent self-connection loop that would cause FD exhaustion
	if f.rule.ListenPort == f.rule.TargetPort && isLocalAddress(f.rule.TargetAddress) {
		return fmt.Errorf("target would connect to self (listen=%d, target=%s:%d)",
			f.rule.ListenPort, f.rule.TargetAddress, f.rule.TargetPort)
	}

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

	logger.Info("direct forwarder started",
		"rule_id", f.rule.ID,
		"listen_ip", f.rule.ListenIP,
		"listen_port", f.rule.ListenPort,
		"target", fmt.Sprintf("%s:%d", f.rule.TargetAddress, f.rule.TargetPort),
		"protocol", protocol)

	return nil
}

// startTCP starts the TCP listener.
func (f *DirectForwarder) startTCP() error {
	addr := listenAddr(f.rule.ListenIP, f.rule.ListenPort)
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
func (f *DirectForwarder) startUDP() error {
	addr := listenAddr(f.rule.ListenIP, f.rule.ListenPort)
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

// Stop stops the direct forwarder.
func (f *DirectForwarder) Stop() error {
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
	logger.Info("direct forwarder stopped", "rule_id", f.rule.ID)
	return nil
}

// Traffic returns the traffic counter.
func (f *DirectForwarder) Traffic() *TrafficCounter {
	return f.traffic
}

// RuleID returns the rule ID.
func (f *DirectForwarder) RuleID() string {
	return f.rule.ID
}

// ListenIP returns the actual listening IP, or empty for wildcard listeners.
func (f *DirectForwarder) ListenIP() string {
	if ip := tcpListenIP(f.tcpListener, f.rule.ListenIP); ip != "" {
		return ip
	}
	return udpListenIP(f.udpConn, f.rule.ListenIP)
}

func (f *DirectForwarder) tcpAcceptLoop() {
	runAcceptLoop(acceptLoopConfig{
		ctx:      f.ctx,
		listener: f.tcpListener,
		cb:       f.cb,
		wg:       &f.wg,
		logName:  "direct",
		handler:  f.handleTCPConn,
	})
}

func (f *DirectForwarder) handleTCPConn(clientConn net.Conn) {
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
		logger.Debug("direct connection rejected by circuit breaker",
			"rule_id", f.rule.ID,
			"state", f.cb.State())
		return
	}

	targetAddr := net.JoinHostPort(f.rule.TargetAddress, fmt.Sprintf("%d", f.rule.TargetPort))

	dialer := &net.Dialer{Timeout: targetDialTimeout}
	if f.rule.BindIP != "" {
		dialer.LocalAddr = &net.TCPAddr{IP: net.ParseIP(f.rule.BindIP)}
	}

	targetConn, err := dialer.Dial("tcp", targetAddr)
	if err != nil {
		f.cb.RecordFailure()
		logger.Error("direct tcp dial target failed",
			"target", targetAddr,
			"bind_ip", f.rule.BindIP,
			"cb_state", f.cb.State(),
			"error", err)
		return
	}

	// Dial succeeded, record success
	f.cb.RecordSuccess()
	defer targetConn.Close()

	// Track target connection for graceful shutdown
	f.tcpConnsMu.Lock()
	f.tcpConns[targetConn] = struct{}{}
	f.tcpConnsMu.Unlock()
	defer func() {
		f.tcpConnsMu.Lock()
		delete(f.tcpConns, targetConn)
		f.tcpConnsMu.Unlock()
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// Client -> Target (upload)
	// Use copyWithTraffic for zero-copy via splice(2) on Linux
	go func() {
		defer wg.Done()
		copyWithTraffic(f.ctx, targetConn, clientConn, f.traffic.AddUpload)
		if tc, ok := targetConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	// Target -> Client (download)
	go func() {
		defer wg.Done()
		copyWithTraffic(f.ctx, clientConn, targetConn, f.traffic.AddDownload)
		if tc, ok := clientConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	wg.Wait()
}

// udpReadLoop reads UDP packets and forwards them to the target.
func (f *DirectForwarder) udpReadLoop() {
	defer f.wg.Done()

	targetAddr := net.JoinHostPort(f.rule.TargetAddress, fmt.Sprintf("%d", f.rule.TargetPort))
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
				logger.Error("direct udp read error", "error", err)
			}
			continue
		}

		f.traffic.AddUpload(int64(n))

		// Get or create upstream connection for this client
		client := f.getOrCreateUDPClient(clientAddr, targetAddr)
		if client == nil {
			continue
		}

		// Forward packet to upstream
		_, err = client.upstream.Write(buf[:n])
		if err != nil {
			logger.Debug("direct udp write to target failed", "client", clientAddr, "error", err)
			f.removeUDPClient(clientAddr.String())
		}
	}
}

// getOrCreateUDPClient gets or creates a UDP client connection.
// Uses double-check locking to prevent race conditions.
func (f *DirectForwarder) getOrCreateUDPClient(clientAddr *net.UDPAddr, targetAddr string) *udpClient {
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
		logger.Debug("direct udp connection rejected by circuit breaker",
			"target", targetAddr,
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
	upstreamAddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		f.udpClientsMu.Unlock()
		logger.Error("direct resolve upstream addr failed", "target", targetAddr, "error", err)
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
		logger.Error("direct udp dial target failed",
			"target", targetAddr,
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

	logger.Debug("direct udp client created", "rule_id", f.rule.ID, "client", clientAddr, "target", targetAddr)

	return client
}

// udpUpstreamReadLoop reads responses from upstream and sends to client.
func (f *DirectForwarder) udpUpstreamReadLoop(client *udpClient) {
	defer f.wg.Done()

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
				logger.Debug("direct udp upstream read error", "client", client.clientAddr, "error", err)
			}
			f.removeUDPClient(client.clientAddr.String())
			return
		}

		client.lastActiveNano.Store(time.Now().UnixNano())
		f.traffic.AddDownload(int64(n))

		// Send response back to client
		_, err = f.udpConn.WriteToUDP(buf[:n], client.clientAddr)
		if err != nil {
			logger.Debug("direct udp write to client failed", "client", client.clientAddr, "error", err)
		}
	}
}

// udpCleanupLoop periodically cleans up idle UDP clients.
func (f *DirectForwarder) udpCleanupLoop() {
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
func (f *DirectForwarder) cleanupIdleUDPClients() {
	now := time.Now()

	f.udpClientsMu.Lock()
	defer f.udpClientsMu.Unlock()

	for key, client := range f.udpClients {
		lastActiveNano := client.lastActiveNano.Load()
		lastActive := time.Unix(0, lastActiveNano)
		if now.Sub(lastActive) > udpIdleTimeout {
			logger.Debug("direct removing idle udp client", "client", client.clientAddr)
			client.upstream.Close()
			delete(f.udpClients, key)
		}
	}
}

// removeUDPClient removes a UDP client by key.
func (f *DirectForwarder) removeUDPClient(key string) {
	f.udpClientsMu.Lock()
	defer f.udpClientsMu.Unlock()

	if client, exists := f.udpClients[key]; exists {
		client.upstream.Close()
		delete(f.udpClients, key)
	}
}

// ListenPort returns the actual listening port from the listener or UDP connection.
func (f *DirectForwarder) ListenPort() uint16 {
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
func (f *DirectForwarder) Connections() int {
	tcpConns := int(f.activeConns.Load())

	f.udpClientsMu.RLock()
	udpConns := len(f.udpClients)
	f.udpClientsMu.RUnlock()

	return tcpConns + udpConns
}
