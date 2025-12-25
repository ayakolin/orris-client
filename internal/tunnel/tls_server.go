package tunnel

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
)

// TLSServer is a TLS tunnel server for Exit agents.
// It accepts connections from Entry agents and forwards data to targets.
type TLSServer struct {
	port   uint16
	client *forward.Client // API client for handshake verification

	rulesMu sync.RWMutex
	rules   []forward.Rule

	listener net.Listener

	handlerMu sync.RWMutex
	handlers  map[string]MessageHandler // ruleID -> handler

	connMu      sync.RWMutex
	conns       map[net.Conn]struct{}
	connLock    map[net.Conn]*sync.Mutex    // per-connection write lock
	connHandler map[net.Conn]MessageHandler // per-connection handler (by ruleID)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewTLSServer creates a new TLS tunnel server.
func NewTLSServer(port uint16, client *forward.Client, rules []forward.Rule) *TLSServer {
	return &TLSServer{
		port:        port,
		client:      client,
		rules:       rules,
		handlers:    make(map[string]MessageHandler),
		conns:       make(map[net.Conn]struct{}),
		connLock:    make(map[net.Conn]*sync.Mutex),
		connHandler: make(map[net.Conn]MessageHandler),
	}
}

// UpdateRules updates the rules for handshake verification.
func (s *TLSServer) UpdateRules(rules []forward.Rule) {
	s.rulesMu.Lock()
	s.rules = rules
	s.rulesMu.Unlock()
}

// AddHandler adds a message handler for a rule.
func (s *TLSServer) AddHandler(ruleID string, handler MessageHandler) {
	s.handlerMu.Lock()
	s.handlers[ruleID] = handler
	s.handlerMu.Unlock()
}

// RemoveHandler removes a message handler.
func (s *TLSServer) RemoveHandler(ruleID string) {
	s.handlerMu.Lock()
	delete(s.handlers, ruleID)
	s.handlerMu.Unlock()
}

// Start starts the TLS tunnel server.
// If port is 0, a random available port will be used.
func (s *TLSServer) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Generate self-signed certificate for TLS
	cert, err := generateSelfSignedCert()
	if err != nil {
		return fmt.Errorf("generate cert: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Create TLS listener
	listener, err := tls.Listen("tcp", fmt.Sprintf(":%d", s.port), tlsConfig)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.listener = listener

	// Update port with the actual port (important when port was 0)
	s.port = uint16(listener.Addr().(*net.TCPAddr).Port)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		logger.Info("tls tunnel server started", "port", s.port)
		s.acceptLoop()
	}()

	return nil
}

// Port returns the actual listening port.
func (s *TLSServer) Port() uint16 {
	return s.port
}

// Stop stops the TLS tunnel server.
func (s *TLSServer) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}

	s.connMu.Lock()
	for conn, mu := range s.connLock {
		// Hold write lock before closing to prevent concurrent write
		mu.Lock()
		conn.Close()
		mu.Unlock()
	}
	s.conns = make(map[net.Conn]struct{})
	s.connLock = make(map[net.Conn]*sync.Mutex)
	s.connHandler = make(map[net.Conn]MessageHandler)
	s.connMu.Unlock()

	if s.listener != nil {
		s.listener.Close()
	}

	s.wg.Wait()
	logger.Info("tls tunnel server stopped")
	return nil
}

func (s *TLSServer) acceptLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			logger.Error("tls accept error", "error", err)
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(conn)
		}()
	}
}

func (s *TLSServer) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()

	// Create per-connection write lock
	writeMu := &sync.Mutex{}

	// Perform handshake
	ruleID, err := s.performHandshake(conn, writeMu)
	if err != nil {
		logger.Error("tls tunnel handshake failed", "remote", remoteAddr, "error", err)
		conn.Close()
		return
	}

	// Create sender
	sender := &tlsConnSender{conn: conn, mu: writeMu}

	// Get handler for this rule, with retry for startup race condition
	// Handler may not be registered yet if forwarder is still connecting to next hop
	var handler MessageHandler
	for i := 0; i < 10; i++ {
		s.handlerMu.RLock()
		h, ok := s.handlers[ruleID]
		s.handlerMu.RUnlock()

		if ok {
			handler = h
			break
		}

		if i == 9 {
			logger.Error("no handler for rule after retries", "rule_id", ruleID)
			conn.Close()
			return
		}

		logger.Debug("handler not ready, waiting", "rule_id", ruleID, "attempt", i+1)

		// Use select with timer to respect context cancellation
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			conn.Close()
			return
		case <-timer.C:
		}
	}

	// Set sender for the handler
	if sh, ok := handler.(interface{ SetSender(Sender) }); ok {
		sh.SetSender(sender)
	}

	// Check if server is shutting down before registering connection.
	// This prevents race condition where Stop() clears maps but we add after.
	s.connMu.Lock()
	if s.ctx.Err() != nil {
		s.connMu.Unlock()
		conn.Close()
		return
	}
	s.conns[conn] = struct{}{}
	s.connLock[conn] = writeMu
	s.connHandler[conn] = handler
	s.connMu.Unlock()

	logger.Info("entry agent connected via tls", "remote", remoteAddr, "rule_id", ruleID)

	defer func() {
		s.connMu.Lock()
		delete(s.conns, conn)
		delete(s.connLock, conn)
		delete(s.connHandler, conn)
		s.connMu.Unlock()
		conn.Close()
		logger.Info("entry agent disconnected from tls", "remote", remoteAddr, "rule_id", ruleID)
	}()

	s.readLoop(conn, sender, handler)
}

func (s *TLSServer) performHandshake(conn net.Conn, writeMu *sync.Mutex) (string, error) {
	// Set read deadline for handshake
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	// Read handshake message (length-prefixed JSON)
	reader := bufio.NewReader(conn)
	handshakeData, err := readLengthPrefixedData(reader)
	if err != nil {
		return "", fmt.Errorf("read handshake: %w", err)
	}

	var handshake forward.TunnelHandshake
	if err := json.Unmarshal(handshakeData, &handshake); err != nil {
		return "", fmt.Errorf("unmarshal handshake: %w", err)
	}

	// Log received token info for debugging
	tokenDbg := handshake.AgentToken
	if len(tokenDbg) > 15 {
		tokenDbg = tokenDbg[:15] + "..."
	}
	logger.Debug("received tls tunnel handshake", "rule_id", handshake.RuleID, "token_prefix", tokenDbg)

	// Verify handshake via server API with timeout
	logger.Debug("verifying tls handshake via server API", "rule_id", handshake.RuleID)

	verifyCtx, verifyCancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer verifyCancel()
	result, err := s.client.VerifyTunnelHandshakeViaServer(verifyCtx, &handshake)
	if err != nil {
		// Send failure result
		failResult := &forward.TunnelHandshakeResult{
			Success: false,
			Error:   fmt.Sprintf("server verification failed: %v", err),
		}
		resultData, _ := json.Marshal(failResult)
		writeMu.Lock()
		writeLengthPrefixedData(conn, resultData)
		writeMu.Unlock()
		return "", fmt.Errorf("verify handshake via server: %w", err)
	}

	// Send success result
	resultData, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("marshal result: %w", err)
	}
	writeMu.Lock()
	err = writeLengthPrefixedData(conn, resultData)
	writeMu.Unlock()
	if err != nil {
		return "", fmt.Errorf("send result: %w", err)
	}

	logger.Info("tls tunnel handshake verified", "rule_id", handshake.RuleID, "entry_agent_id", result.EntryAgentID)
	return handshake.RuleID, nil
}

func (s *TLSServer) readLoop(conn net.Conn, sender *tlsConnSender, handler MessageHandler) {
	reader := bufio.NewReader(conn)
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		msg, err := DecodeMessage(reader)
		if err != nil {
			if err != io.EOF && s.ctx.Err() == nil {
				logger.Error("tls tunnel read error", "error", err)
			}
			return
		}

		s.handleMessage(sender, handler, msg)
	}
}

func (s *TLSServer) handleMessage(sender *tlsConnSender, handler MessageHandler, msg *Message) {
	switch msg.Type {
	case MsgConnect:
		// UDP connections have payload (client address), TCP connections don't
		if len(msg.Payload) > 0 {
			handler.HandleConnectWithPayload(msg.ConnID, msg.Payload)
		} else {
			handler.HandleConnect(msg.ConnID)
		}
	case MsgData:
		handler.HandleData(msg.ConnID, msg.Payload)
	case MsgClose:
		handler.HandleClose(msg.ConnID)
	case MsgPing:
		// Use the same sender to share the write lock
		sender.SendMessage(NewPongMessage())
	default:
		logger.Warn("unknown message type", "type", msg.Type)
	}
}

// tlsConnSender implements Sender for a TLS connection.
// All tlsConnSender instances for the same connection share the same mutex.
type tlsConnSender struct {
	conn net.Conn
	mu   *sync.Mutex // shared per-connection write lock
}

func (s *tlsConnSender) SendMessage(msg *Message) error {
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.conn.Write(data); err != nil {
		return fmt.Errorf("write message: %w", err)
	}

	return nil
}
