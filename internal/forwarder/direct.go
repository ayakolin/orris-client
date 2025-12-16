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

// DirectForwarder handles direct forwarding (local port -> target).
type DirectForwarder struct {
	rule        *forward.Rule
	tcpListener net.Listener
	udpConn     *net.UDPConn
	traffic     *TrafficCounter

	// UDP client tracking for response routing
	udpClientsMu sync.RWMutex
	udpClients   map[string]*udpClient // client addr -> upstream conn

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDirectForwarder creates a new direct forwarder.
func NewDirectForwarder(rule *forward.Rule) *DirectForwarder {
	return &DirectForwarder{
		rule:       rule,
		traffic:    &TrafficCounter{},
		udpClients: make(map[string]*udpClient),
	}
}

// Start starts the direct forwarder.
func (f *DirectForwarder) Start(ctx context.Context) error {
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

	logger.Info("direct forwarder started",
		"rule_id", f.rule.ID,
		"listen_port", f.rule.ListenPort,
		"target", fmt.Sprintf("%s:%d", f.rule.TargetAddress, f.rule.TargetPort),
		"protocol", protocol)

	return nil
}

// startTCP starts the TCP listener.
func (f *DirectForwarder) startTCP() error {
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
func (f *DirectForwarder) startUDP() error {
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

func (f *DirectForwarder) tcpAcceptLoop() {
	defer f.wg.Done()

	for {
		conn, err := f.tcpListener.Accept()
		if err != nil {
			select {
			case <-f.ctx.Done():
				return
			default:
				if !isClosedError(err) {
					logger.Error("direct tcp accept error", "error", err)
				}
				continue
			}
		}

		f.wg.Add(1)
		go f.handleTCPConn(conn)
	}
}

func (f *DirectForwarder) handleTCPConn(clientConn net.Conn) {
	defer f.wg.Done()
	defer clientConn.Close()

	targetAddr := net.JoinHostPort(f.rule.TargetAddress, fmt.Sprintf("%d", f.rule.TargetPort))

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	if f.rule.BindIP != "" {
		dialer.LocalAddr = &net.TCPAddr{IP: net.ParseIP(f.rule.BindIP)}
	}

	targetConn, err := dialer.Dial("tcp", targetAddr)
	if err != nil {
		logger.Error("direct tcp dial target failed", "target", targetAddr, "bind_ip", f.rule.BindIP, "error", err)
		return
	}
	defer targetConn.Close()

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
func (f *DirectForwarder) getOrCreateUDPClient(clientAddr *net.UDPAddr, targetAddr string) *udpClient {
	key := clientAddr.String()

	f.udpClientsMu.RLock()
	client, exists := f.udpClients[key]
	f.udpClientsMu.RUnlock()

	if exists {
		client.lastActiveNano.Store(time.Now().UnixNano())
		return client
	}

	// Create new upstream connection with optional bind IP
	upstreamAddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		logger.Error("direct resolve upstream addr failed", "target", targetAddr, "error", err)
		return nil
	}

	var localAddr *net.UDPAddr
	if f.rule.BindIP != "" {
		localAddr = &net.UDPAddr{IP: net.ParseIP(f.rule.BindIP)}
	}

	upstream, err := net.DialUDP("udp", localAddr, upstreamAddr)
	if err != nil {
		logger.Error("direct udp dial target failed", "target", targetAddr, "bind_ip", f.rule.BindIP, "error", err)
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
