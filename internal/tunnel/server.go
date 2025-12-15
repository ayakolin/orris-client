package tunnel

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/orris-inc/orris-client/internal/forward"

	"github.com/orris-inc/orris-client/internal/logger"
)

// MessageHandler handles messages from tunnel clients.
type MessageHandler interface {
	HandleConnect(connID uint64)
	HandleConnectWithPayload(connID uint64, payload []byte) // For UDP connections with client address
	HandleData(connID uint64, data []byte)
	HandleClose(connID uint64)
}

// Sender sends messages through the tunnel.
type Sender interface {
	SendMessage(msg *Message) error
}

// Server is a WebSocket tunnel server for Exit agents.
// It accepts connections from Entry agents and forwards data to targets.
type Server struct {
	port   uint16
	client *forward.Client // API client for handshake verification
	rules  []forward.Rule

	listener net.Listener
	server   *http.Server
	upgrader websocket.Upgrader

	handlerMu sync.RWMutex
	handlers  map[string]MessageHandler // ruleID -> handler

	connMu      sync.RWMutex
	conns       map[*websocket.Conn]struct{}
	connLock    map[*websocket.Conn]*sync.Mutex    // per-connection write lock
	connHandler map[*websocket.Conn]MessageHandler // per-connection handler (by ruleID)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewServer creates a new tunnel server.
func NewServer(port uint16, client *forward.Client, rules []forward.Rule) *Server {
	return &Server{
		port:        port,
		client:      client,
		rules:       rules,
		handlers:    make(map[string]MessageHandler),
		conns:       make(map[*websocket.Conn]struct{}),
		connLock:    make(map[*websocket.Conn]*sync.Mutex),
		connHandler: make(map[*websocket.Conn]MessageHandler),
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
	s.connMu.Lock()
	s.rules = rules
	s.connMu.Unlock()
}

// AddHandler adds a message handler for a rule.
func (s *Server) AddHandler(ruleID string, handler MessageHandler) {
	s.handlerMu.Lock()
	s.handlers[ruleID] = handler
	s.handlerMu.Unlock()
}

// RemoveHandler removes a message handler.
func (s *Server) RemoveHandler(ruleID string) {
	s.handlerMu.Lock()
	delete(s.handlers, ruleID)
	s.handlerMu.Unlock()
}

// Start starts the tunnel server.
// If port is 0, a random available port will be used.
func (s *Server) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Create listener first to get the actual port (supports port 0 for random)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.listener = listener

	// Update port with the actual port (important when port was 0)
	s.port = uint16(listener.Addr().(*net.TCPAddr).Port)

	mux := http.NewServeMux()
	mux.HandleFunc("/tunnel", s.handleTunnel)

	s.server = &http.Server{
		Handler: mux,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		logger.Info("tunnel server started", "port", s.port)
		if err := s.server.Serve(s.listener); err != http.ErrServerClosed {
			logger.Error("tunnel server error", "error", err)
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
	s.connMu.Unlock()

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
	ruleID, err := s.performHandshake(conn, writeMu)
	if err != nil {
		logger.Error("tunnel handshake failed", "remote", r.RemoteAddr, "error", err)
		conn.Close()
		return
	}

	// Create sender
	sender := &connSender{conn: conn, mu: writeMu}

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

	// Set sender for the handler
	if sh, ok := handler.(interface{ SetSender(Sender) }); ok {
		sh.SetSender(sender)
	}

	s.connMu.Lock()
	s.conns[conn] = struct{}{}
	s.connLock[conn] = writeMu
	s.connHandler[conn] = handler
	s.connMu.Unlock()

	logger.Info("entry agent connected", "remote", r.RemoteAddr, "rule_id", ruleID)

	defer func() {
		s.connMu.Lock()
		delete(s.conns, conn)
		delete(s.connLock, conn)
		delete(s.connHandler, conn)
		s.connMu.Unlock()
		conn.Close()
		logger.Info("entry agent disconnected", "remote", r.RemoteAddr, "rule_id", ruleID)
	}()

	s.readLoop(conn, sender, handler)
}

func (s *Server) performHandshake(conn *websocket.Conn, writeMu *sync.Mutex) (string, error) {
	// Set read deadline for handshake
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	// Read handshake message
	_, data, err := conn.ReadMessage()
	if err != nil {
		return "", fmt.Errorf("read handshake: %w", err)
	}

	var handshake forward.TunnelHandshake
	if err := json.Unmarshal(data, &handshake); err != nil {
		return "", fmt.Errorf("unmarshal handshake: %w", err)
	}

	// Log received token info for debugging
	tokenDbg := handshake.AgentToken
	if len(tokenDbg) > 15 {
		tokenDbg = tokenDbg[:15] + "..."
	}
	logger.Debug("received tunnel handshake", "rule_id", handshake.RuleID, "token_prefix", tokenDbg)

	// Verify handshake
	// Verify handshake via server API
	logger.Debug("verifying handshake via server API", "rule_id", handshake.RuleID)

	result, err := s.client.VerifyTunnelHandshakeViaServer(context.Background(), &handshake)
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
		return "", fmt.Errorf("verify handshake via server: %w", err)
	}

	// Send success result
	resultData, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("marshal result: %w", err)
	}
	writeMu.Lock()
	err = conn.WriteMessage(websocket.TextMessage, resultData)
	writeMu.Unlock()
	if err != nil {
		return "", fmt.Errorf("send result: %w", err)
	}

	logger.Info("tunnel handshake verified", "rule_id", handshake.RuleID, "entry_agent_id", result.EntryAgentID)
	return handshake.RuleID, nil
}

func (s *Server) readLoop(conn *websocket.Conn, sender *connSender, handler MessageHandler) {
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

		s.handleMessage(sender, handler, msg)
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
