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

	"github.com/xtaci/smux"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
)

// TLSServer is a TLS tunnel server for Exit agents.
// It accepts connections from Entry agents and forwards data to targets.
// Supports both message-based protocol (tls) and SMUX multiplexing (tls_smux).
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
	connRuleID  map[net.Conn]string         // per-connection ruleID

	// SMUX support for tls_smux tunnel type
	smuxHandlerMu sync.RWMutex
	smuxHandlers  map[string]SmuxStreamHandler // ruleID -> SMUX stream handler

	smuxSessionMu sync.RWMutex
	smuxSessions  map[*smux.Session]string // session -> ruleID

	smuxConfig *SmuxConfig

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewTLSServer creates a new TLS tunnel server.
func NewTLSServer(port uint16, client *forward.Client, rules []forward.Rule) *TLSServer {
	return &TLSServer{
		port:         port,
		client:       client,
		rules:        rules,
		handlers:     make(map[string]MessageHandler),
		conns:        make(map[net.Conn]struct{}),
		connLock:     make(map[net.Conn]*sync.Mutex),
		connHandler:  make(map[net.Conn]MessageHandler),
		connRuleID:   make(map[net.Conn]string),
		smuxHandlers: make(map[string]SmuxStreamHandler),
		smuxSessions: make(map[*smux.Session]string),
		smuxConfig:   DefaultSmuxConfig(),
	}
}

// UpdateRules updates the rules for handshake verification.
func (s *TLSServer) UpdateRules(rules []forward.Rule) {
	s.rulesMu.Lock()
	s.rules = rules
	s.rulesMu.Unlock()
}

// AddHandler adds a message handler for a rule.
// If replacing an existing handler, it updates all connections using the old handler.
func (s *TLSServer) AddHandler(ruleID string, handler MessageHandler) {
	s.handlerMu.Lock()
	oldHandler := s.handlers[ruleID]
	s.handlers[ruleID] = handler
	s.handlerMu.Unlock()

	// If replacing an existing handler, update all connections using it
	if oldHandler != nil {
		// Notify old handler to disconnect (close all its connections)
		oldHandler.OnTunnelDisconnect()

		// Update connHandler for all connections with this ruleID
		s.connMu.Lock()
		for conn, rid := range s.connRuleID {
			if rid == ruleID {
				s.connHandler[conn] = handler
				// Update sender for new handler
				if sh, ok := handler.(interface{ SetSender(Sender) }); ok {
					mu := s.connLock[conn]
					sh.SetSender(&tlsConnSender{conn: conn, mu: mu})
				}
			}
		}
		s.connMu.Unlock()

		logger.Info("updated handler for existing connections", "rule_id", ruleID)
	}
}

// RemoveHandler removes a message handler.
func (s *TLSServer) RemoveHandler(ruleID string) {
	s.handlerMu.Lock()
	delete(s.handlers, ruleID)
	s.handlerMu.Unlock()
}

// SetSmuxHandler sets the SMUX stream handler for a rule.
func (s *TLSServer) SetSmuxHandler(ruleID string, handler SmuxStreamHandler) {
	s.smuxHandlerMu.Lock()
	oldHandler := s.smuxHandlers[ruleID]
	s.smuxHandlers[ruleID] = handler
	s.smuxHandlerMu.Unlock()

	// Notify old handler if exists
	if oldHandler != nil {
		oldHandler.OnSessionClose()
		logger.Info("updated smux stream handler for rule", "rule_id", ruleID)
	}
}

// RemoveSmuxHandler removes the SMUX stream handler for a rule.
func (s *TLSServer) RemoveSmuxHandler(ruleID string) {
	s.smuxHandlerMu.Lock()
	delete(s.smuxHandlers, ruleID)
	s.smuxHandlerMu.Unlock()
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

	// Close all message-based connections
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
	s.connRuleID = make(map[net.Conn]string)
	s.connMu.Unlock()

	// Close all SMUX sessions
	s.smuxSessionMu.Lock()
	for session := range s.smuxSessions {
		session.Close()
	}
	s.smuxSessions = make(map[*smux.Session]string)
	s.smuxSessionMu.Unlock()

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
	ruleID, isProbe, enableSmux, err := s.performHandshake(conn, writeMu)
	if err != nil {
		logger.Error("tls tunnel handshake failed", "remote", remoteAddr, "error", err)
		conn.Close()
		return
	}

	// Create sender
	sender := &tlsConnSender{conn: conn, mu: writeMu}

	// Handle probe connections separately - only respond to ping/pong, don't affect forwarder
	if isProbe {
		logger.Info("probe connection established via tls", "remote", remoteAddr, "rule_id", ruleID)
		defer func() {
			conn.Close()
			logger.Info("probe connection closed via tls", "remote", remoteAddr, "rule_id", ruleID)
		}()
		s.probeReadLoop(conn, sender)
		return
	}

	// Handle SMUX connections
	if enableSmux {
		s.handleSmuxConnection(conn, ruleID, remoteAddr)
		return
	}

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
	s.connRuleID[conn] = ruleID
	s.connMu.Unlock()

	// Double-check: if handler was replaced while we were setting up, use the new one.
	// This handles the race condition where AddHandler is called between getting
	// the handler from handlers map and registering the connection.
	s.handlerMu.RLock()
	currentHandler := s.handlers[ruleID]
	s.handlerMu.RUnlock()

	if currentHandler != nil && currentHandler != handler {
		s.connMu.Lock()
		s.connHandler[conn] = currentHandler
		s.connMu.Unlock()
		handler = currentHandler
		logger.Info("handler was updated during connection setup, using new handler", "rule_id", ruleID)
	}

	// Set sender for the handler (after ensuring we have the latest handler)
	if sh, ok := handler.(interface{ SetSender(Sender) }); ok {
		sh.SetSender(sender)
	}

	logger.Info("entry agent connected via tls", "remote", remoteAddr, "rule_id", ruleID)

	defer func() {
		// Notify handler that tunnel is disconnected before closing connection.
		// This allows handler to close all active target connections and stop goroutines.
		handler.OnTunnelDisconnect()

		s.connMu.Lock()
		delete(s.conns, conn)
		delete(s.connLock, conn)
		delete(s.connHandler, conn)
		delete(s.connRuleID, conn)
		s.connMu.Unlock()
		conn.Close()
		logger.Info("entry agent disconnected from tls", "remote", remoteAddr, "rule_id", ruleID)
	}()

	s.readLoop(conn, sender)
}

func (s *TLSServer) performHandshake(conn net.Conn, writeMu *sync.Mutex) (ruleID string, isProbe bool, enableSmux bool, err error) {
	// Set read deadline for handshake
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	// Read handshake message (length-prefixed, obfuscated)
	// Note: We read directly from conn to avoid bufio.Reader pre-reading data
	// that would be lost when SMUX session takes over the connection.
	obfuscatedData, err := readLengthPrefixedData(conn)
	if err != nil {
		return "", false, false, fmt.Errorf("read handshake: %w", err)
	}

	// Deobfuscate handshake data
	handshakeData, err := DeobfuscateHandshake(obfuscatedData)
	if err != nil {
		return "", false, false, fmt.Errorf("deobfuscate handshake: %w", err)
	}

	var handshake forward.TunnelHandshake
	if err := json.Unmarshal(handshakeData, &handshake); err != nil {
		return "", false, false, fmt.Errorf("unmarshal handshake: %w", err)
	}

	// Log received token info for debugging
	tokenDbg := handshake.AgentToken
	if len(tokenDbg) > 15 {
		tokenDbg = tokenDbg[:15] + "..."
	}
	logger.Debug("received tls tunnel handshake", "rule_id", handshake.RuleID, "token_prefix", tokenDbg, "is_probe", handshake.IsProbe, "enable_smux", handshake.EnableSmux)

	// Verify handshake via server API with timeout
	logger.Debug("verifying tls handshake via server API", "rule_id", handshake.RuleID)

	verifyCtx, verifyCancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer verifyCancel()
	result, err := s.client.VerifyTunnelHandshakeViaServer(verifyCtx, &handshake)
	if err != nil {
		// Send failure result (obfuscated)
		failResult := &forward.TunnelHandshakeResult{
			Success: false,
			Error:   fmt.Sprintf("server verification failed: %v", err),
		}
		resultData, _ := json.Marshal(failResult)
		obfuscatedResult := ObfuscateHandshake(resultData)
		writeMu.Lock()
		writeLengthPrefixedData(conn, obfuscatedResult)
		writeMu.Unlock()
		return "", false, false, fmt.Errorf("verify handshake via server: %w", err)
	}

	// Send success result (obfuscated)
	resultData, err := json.Marshal(result)
	if err != nil {
		return "", false, false, fmt.Errorf("marshal result: %w", err)
	}
	obfuscatedResult := ObfuscateHandshake(resultData)
	writeMu.Lock()
	err = writeLengthPrefixedData(conn, obfuscatedResult)
	writeMu.Unlock()
	if err != nil {
		return "", false, false, fmt.Errorf("send result: %w", err)
	}

	logger.Info("tls tunnel handshake verified", "rule_id", handshake.RuleID, "entry_agent_id", result.EntryAgentID, "is_probe", handshake.IsProbe, "enable_smux", handshake.EnableSmux)
	return handshake.RuleID, handshake.IsProbe, handshake.EnableSmux, nil
}

func (s *TLSServer) readLoop(conn net.Conn, sender *tlsConnSender) {
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

		// Get current handler from map to support hot-swapping during config sync
		s.connMu.RLock()
		handler := s.connHandler[conn]
		s.connMu.RUnlock()

		if handler == nil {
			logger.Warn("no handler for connection, skipping message")
			continue
		}

		s.handleMessage(sender, handler, msg)
	}
}

// probeReadLoop handles probe connections - only responds to ping/pong messages.
// Probe connections are used for tunnel health checks and should not affect the forwarder.
func (s *TLSServer) probeReadLoop(conn net.Conn, sender *tlsConnSender) {
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
				logger.Debug("probe connection read error", "error", err)
			}
			return
		}

		// Only handle ping messages for probe connections
		if msg.Type == MsgPing {
			sender.SendMessage(NewPongMessage())
		}
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

// handleSmuxConnection handles a TLS connection with SMUX multiplexing.
func (s *TLSServer) handleSmuxConnection(conn net.Conn, ruleID, remoteAddr string) {
	// Create SMUX session as server
	session, err := smux.Server(conn, s.smuxConfig.ToSmuxConfig())
	if err != nil {
		logger.Error("create smux session failed", "remote", remoteAddr, "error", err)
		conn.Close()
		return
	}

	// Register session
	s.smuxSessionMu.Lock()
	s.smuxSessions[session] = ruleID
	s.smuxSessionMu.Unlock()

	logger.Info("smux session established via tls", "remote", remoteAddr, "rule_id", ruleID)

	defer func() {
		// Unregister and close session
		s.smuxSessionMu.Lock()
		delete(s.smuxSessions, session)
		s.smuxSessionMu.Unlock()

		session.Close()

		// Notify handler
		s.smuxHandlerMu.RLock()
		handler := s.smuxHandlers[ruleID]
		s.smuxHandlerMu.RUnlock()
		if handler != nil {
			handler.OnSessionClose()
		}

		logger.Info("smux session closed via tls", "remote", remoteAddr, "rule_id", ruleID)
	}()

	s.acceptSmuxStreams(session, ruleID, remoteAddr)
}

// acceptSmuxStreams accepts and handles streams from a SMUX session.
func (s *TLSServer) acceptSmuxStreams(session *smux.Session, ruleID, remoteAddr string) {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		stream, err := session.AcceptStream()
		if err != nil {
			// Normal closure conditions - don't log as error
			if s.ctx.Err() != nil || session.IsClosed() {
				return
			}
			// EOF is normal when client closes the session
			if err == io.EOF || err.Error() == "EOF" {
				logger.Debug("smux session closed by client", "remote", remoteAddr)
				return
			}
			logger.Error("smux accept stream error", "remote", remoteAddr, "error", err)
			return
		}

		// Get handler for this rule
		s.smuxHandlerMu.RLock()
		handler := s.smuxHandlers[ruleID]
		s.smuxHandlerMu.RUnlock()

		if handler == nil {
			logger.Warn("no smux handler for stream", "rule_id", ruleID)
			stream.Close()
			continue
		}

		// Handle stream in a goroutine
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			handler.HandleStream(stream)
		}()
	}
}
