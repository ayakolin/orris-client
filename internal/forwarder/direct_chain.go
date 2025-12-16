package forwarder

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
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

	// UDP client tracking for response routing
	udpClientsMu sync.RWMutex
	udpClients   map[string]*udpClient // client addr -> upstream conn

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDirectChainForwarder creates a new direct chain forwarder.
func NewDirectChainForwarder(rule *forward.Rule) *DirectChainForwarder {
	return &DirectChainForwarder{
		rule:       rule,
		traffic:    &TrafficCounter{},
		udpClients: make(map[string]*udpClient),
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
		nextHop = net.JoinHostPort(f.rule.TargetAddress, fmt.Sprintf("%d", f.rule.TargetPort))
	} else {
		// Entry or Relay: connect to next hop
		nextHop = net.JoinHostPort(f.rule.NextHopAddress, fmt.Sprintf("%d", f.rule.NextHopPort))
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
	defer f.wg.Done()

	for {
		conn, err := f.tcpListener.Accept()
		if err != nil {
			select {
			case <-f.ctx.Done():
				return
			default:
				if !isClosedError(err) {
					logger.Error("direct chain tcp accept error", "error", err)
				}
				continue
			}
		}

		f.wg.Add(1)
		go f.handleTCPConn(conn, nextHop)
	}
}

// handleTCPConn handles a single TCP connection.
func (f *DirectChainForwarder) handleTCPConn(clientConn net.Conn, nextHop string) {
	defer f.wg.Done()
	defer clientConn.Close()

	// Connect to next hop with optional bind IP
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	if f.rule.BindIP != "" {
		dialer.LocalAddr = &net.TCPAddr{IP: net.ParseIP(f.rule.BindIP)}
	}

	upstreamConn, err := dialer.Dial("tcp", nextHop)
	if err != nil {
		logger.Error("direct chain tcp dial failed",
			"rule_id", f.rule.ID,
			"next_hop", nextHop,
			"bind_ip", f.rule.BindIP,
			"error", err)
		return
	}
	defer upstreamConn.Close()

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
func (f *DirectChainForwarder) getOrCreateUDPClient(clientAddr *net.UDPAddr, nextHop string) *udpClient {
	key := clientAddr.String()

	f.udpClientsMu.RLock()
	client, exists := f.udpClients[key]
	f.udpClientsMu.RUnlock()

	if exists {
		client.lastActiveNano.Store(time.Now().UnixNano())
		return client
	}

	// Create new upstream connection with optional bind IP
	upstreamAddr, err := net.ResolveUDPAddr("udp", nextHop)
	if err != nil {
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
		logger.Error("direct chain udp dial upstream failed",
			"next_hop", nextHop,
			"bind_ip", f.rule.BindIP,
			"error", err)
		return nil
	}

	client = &udpClient{
		clientAddr: clientAddr,
		upstream:   upstream,
	}
	client.lastActiveNano.Store(time.Now().UnixNano())

	f.udpClientsMu.Lock()
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
