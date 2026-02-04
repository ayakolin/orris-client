package tunnel

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xtaci/smux"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
)

// MessageHandler handles messages from tunnel clients.
type MessageHandler interface {
	HandleConnect(connID uint64)
	HandleConnectWithPayload(connID uint64, payload []byte) // For UDP connections with client address
	HandleData(connID uint64, data []byte)
	HandleClose(connID uint64)
	// OnTunnelDisconnect is called when the tunnel connection is closed.
	// Implementations should close all active connections to stop goroutines.
	OnTunnelDisconnect()
}

// Sender sends messages through the tunnel.
type Sender interface {
	SendMessage(msg *Message) error
}

// Server is a WebSocket tunnel server for Exit agents.
// It accepts connections from Entry agents and forwards data to targets.
// Supports both message-based protocol (ws) and SMUX multiplexing (ws_smux).
type Server struct {
	port   uint16
	client *forward.Client // API client for handshake verification

	rulesMu sync.RWMutex
	rules   []forward.Rule

	listener net.Listener
	server   *http.Server
	upgrader websocket.Upgrader

	handlerMu sync.RWMutex
	handlers  map[string]MessageHandler // ruleID -> handler

	connMu      sync.RWMutex
	conns       map[*websocket.Conn]struct{}
	connLock    map[*websocket.Conn]*sync.Mutex    // per-connection write lock
	connHandler map[*websocket.Conn]MessageHandler // per-connection handler (by ruleID)
	connRuleID  map[*websocket.Conn]string         // per-connection ruleID

	// SMUX support for ws_smux tunnel type
	smuxHandlerMu sync.RWMutex
	smuxHandlers  map[string]SmuxStreamHandler // ruleID -> SMUX stream handler

	smuxSessionMu sync.RWMutex
	smuxSessions  map[*smux.Session]string // session -> ruleID

	smuxConfig *SmuxConfig

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewServer creates a new tunnel server.
func NewServer(port uint16, client *forward.Client, rules []forward.Rule) *Server {
	return &Server{
		port:         port,
		client:       client,
		rules:        rules,
		handlers:     make(map[string]MessageHandler),
		conns:        make(map[*websocket.Conn]struct{}),
		connLock:     make(map[*websocket.Conn]*sync.Mutex),
		connHandler:  make(map[*websocket.Conn]MessageHandler),
		connRuleID:   make(map[*websocket.Conn]string),
		smuxHandlers: make(map[string]SmuxStreamHandler),
		smuxSessions: make(map[*smux.Session]string),
		smuxConfig:   DefaultSmuxConfig(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  64 * 1024,
			WriteBufferSize: 64 * 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

// UpdateRules updates the rules for handshake verification.
func (s *Server) UpdateRules(rules []forward.Rule) {
	s.rulesMu.Lock()
	s.rules = rules
	s.rulesMu.Unlock()
}

// AddHandler adds a message handler for a rule.
// If replacing an existing handler, it updates all connections using the old handler.
func (s *Server) AddHandler(ruleID string, handler MessageHandler) {
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
					sh.SetSender(&connSender{conn: conn, mu: mu})
				}
			}
		}
		s.connMu.Unlock()

		logger.Info("updated handler for existing connections", "rule_id", ruleID)
	}
}

// RemoveHandler removes a message handler.
func (s *Server) RemoveHandler(ruleID string) {
	s.handlerMu.Lock()
	delete(s.handlers, ruleID)
	s.handlerMu.Unlock()
}

// SetSmuxHandler sets the SMUX stream handler for a rule.
func (s *Server) SetSmuxHandler(ruleID string, handler SmuxStreamHandler) {
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
func (s *Server) RemoveSmuxHandler(ruleID string) {
	s.smuxHandlerMu.Lock()
	delete(s.smuxHandlers, ruleID)
	s.smuxHandlerMu.Unlock()
}

// Start starts the tunnel server with TLS.
// If port is 0, a random available port will be used.
func (s *Server) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Generate self-signed certificate for TLS
	cert, err := generateSelfSignedCert()
	if err != nil {
		return fmt.Errorf("generate cert: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"http/1.1"}, // Disable HTTP/2, WebSocket requires HTTP/1.1
	}

	// Create TCP listener first to get the actual port (supports port 0 for random)
	tcpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	// Wrap with TLS
	s.listener = tls.NewListener(tcpListener, tlsConfig)

	// Update port with the actual port (important when port was 0)
	s.port = uint16(tcpListener.Addr().(*net.TCPAddr).Port)

	mux := http.NewServeMux()
	mux.HandleFunc("/tunnel", s.handleTunnel)

	s.server = &http.Server{
		Handler: mux,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		logger.Info("wss tunnel server started", "port", s.port)
		if err := s.server.Serve(s.listener); err != http.ErrServerClosed {
			logger.Error("wss tunnel server error", "error", err)
		}
	}()

	return nil
}

// Port returns the actual listening port.
func (s *Server) Port() uint16 {
	return s.port
}

// Stop stops the tunnel server.
func (s *Server) Stop() error {
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
	s.conns = make(map[*websocket.Conn]struct{})
	s.connLock = make(map[*websocket.Conn]*sync.Mutex)
	s.connHandler = make(map[*websocket.Conn]MessageHandler)
	s.connRuleID = make(map[*websocket.Conn]string)
	s.connMu.Unlock()

	// Close all SMUX sessions
	s.smuxSessionMu.Lock()
	for session := range s.smuxSessions {
		session.Close()
	}
	s.smuxSessions = make(map[*smux.Session]string)
	s.smuxSessionMu.Unlock()

	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.server.Shutdown(ctx)
	}

	s.wg.Wait()
	logger.Info("tunnel server stopped")
	return nil
}

func (s *Server) handleTunnel(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("tunnel upgrade failed", "error", err)
		return
	}

	// Create per-connection write lock
	writeMu := &sync.Mutex{}

	// Perform handshake
	ruleID, isProbe, enableSmux, err := s.performHandshake(conn, writeMu)
	if err != nil {
		logger.Error("tunnel handshake failed", "remote", r.RemoteAddr, "error", err)
		conn.Close()
		return
	}

	// Create sender
	sender := &connSender{conn: conn, mu: writeMu}

	// Handle probe connections separately - only respond to ping/pong, don't affect forwarder
	if isProbe {
		logger.Info("probe connection established", "remote", r.RemoteAddr, "rule_id", ruleID)
		defer func() {
			conn.Close()
			logger.Info("probe connection closed", "remote", r.RemoteAddr, "rule_id", ruleID)
		}()
		s.probeReadLoop(conn, sender)
		return
	}

	// Handle SMUX connections
	if enableSmux {
		s.handleSmuxConnection(conn, ruleID, r.RemoteAddr)
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
		time.Sleep(500 * time.Millisecond)
	}

	// Register connection first, then check for handler updates
	s.connMu.Lock()
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

	logger.Info("entry agent connected", "remote", r.RemoteAddr, "rule_id", ruleID)

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
		logger.Info("entry agent disconnected", "remote", r.RemoteAddr, "rule_id", ruleID)
	}()

	s.readLoop(conn, sender)
}

func (s *Server) performHandshake(conn *websocket.Conn, writeMu *sync.Mutex) (ruleID string, isProbe bool, enableSmux bool, err error) {
	// Set read deadline for handshake
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	// Read handshake message
	_, data, err := conn.ReadMessage()
	if err != nil {
		return "", false, false, fmt.Errorf("read handshake: %w", err)
	}

	var handshake forward.TunnelHandshake
	if err := json.Unmarshal(data, &handshake); err != nil {
		return "", false, false, fmt.Errorf("unmarshal handshake: %w", err)
	}

	// Log received token info for debugging
	tokenDbg := handshake.AgentToken
	if len(tokenDbg) > 15 {
		tokenDbg = tokenDbg[:15] + "..."
	}
	logger.Debug("received tunnel handshake", "rule_id", handshake.RuleID, "token_prefix", tokenDbg, "is_probe", handshake.IsProbe, "enable_smux", handshake.EnableSmux)

	// Verify handshake via server API with timeout
	logger.Debug("verifying handshake via server API", "rule_id", handshake.RuleID)

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
		conn.WriteMessage(websocket.TextMessage, resultData)
		writeMu.Unlock()
		return "", false, false, fmt.Errorf("verify handshake via server: %w", err)
	}

	// Send success result
	resultData, err := json.Marshal(result)
	if err != nil {
		return "", false, false, fmt.Errorf("marshal result: %w", err)
	}
	writeMu.Lock()
	err = conn.WriteMessage(websocket.TextMessage, resultData)
	writeMu.Unlock()
	if err != nil {
		return "", false, false, fmt.Errorf("send result: %w", err)
	}

	logger.Info("tunnel handshake verified", "rule_id", handshake.RuleID, "entry_agent_id", result.EntryAgentID, "is_probe", handshake.IsProbe, "enable_smux", handshake.EnableSmux)
	return handshake.RuleID, handshake.IsProbe, handshake.EnableSmux, nil
}

func (s *Server) readLoop(conn *websocket.Conn, sender *connSender) {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				logger.Error("tunnel read error", "error", err)
			}
			return
		}

		msg, err := DecodeMessage(bytes.NewReader(data))
		if err != nil {
			logger.Error("decode message error", "error", err)
			continue
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
func (s *Server) probeReadLoop(conn *websocket.Conn, sender *connSender) {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				logger.Debug("probe connection read error", "error", err)
			}
			return
		}

		msg, err := DecodeMessage(bytes.NewReader(data))
		if err != nil {
			logger.Debug("probe decode message error", "error", err)
			continue
		}

		// Only handle ping messages for probe connections
		if msg.Type == MsgPing {
			sender.SendMessage(NewPongMessage())
		}
	}
}

func (s *Server) handleMessage(sender *connSender, handler MessageHandler, msg *Message) {
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

// connSender implements Sender for a WebSocket connection.
// All connSender instances for the same connection share the same mutex.
type connSender struct {
	conn *websocket.Conn
	mu   *sync.Mutex // shared per-connection write lock
}

func (s *connSender) SendMessage(msg *Message) error {
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		return fmt.Errorf("write message: %w", err)
	}

	return nil
}

// handleSmuxConnection handles a WebSocket connection with SMUX multiplexing.
func (s *Server) handleSmuxConnection(conn *websocket.Conn, ruleID, remoteAddr string) {
	// Wrap WebSocket as net.Conn for SMUX
	netConn := NewWSConn(conn)

	// Create SMUX session as server
	session, err := smux.Server(netConn, s.smuxConfig.ToSmuxConfig())
	if err != nil {
		logger.Error("create smux session failed", "remote", remoteAddr, "error", err)
		conn.Close()
		return
	}

	// Register session
	s.smuxSessionMu.Lock()
	s.smuxSessions[session] = ruleID
	s.smuxSessionMu.Unlock()

	logger.Info("smux session established via ws", "remote", remoteAddr, "rule_id", ruleID)

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

		logger.Info("smux session closed via ws", "remote", remoteAddr, "rule_id", ruleID)
	}()

	s.acceptSmuxStreams(session, ruleID, remoteAddr)
}

// acceptSmuxStreams accepts and handles streams from a SMUX session.
func (s *Server) acceptSmuxStreams(session *smux.Session, ruleID, remoteAddr string) {
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
