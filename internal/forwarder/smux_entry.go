package forwarder

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
	"github.com/orris-inc/orris-client/internal/tunnel"
)

// SmuxEntryForwarder handles entry forwarding using SMUX multiplexing.
// Each user connection opens a dedicated SMUX stream for bidirectional data transfer.
type SmuxEntryForwarder struct {
	rule        *forward.Rule
	tcpListener net.Listener
	traffic     *TrafficCounter

	smuxClient tunnel.SmuxTunnelClient

	// Circuit breaker to prevent connection storms when tunnel is unavailable
	cb *circuitBreaker

	// Active connections tracking
	connMu     sync.RWMutex
	conns      map[uint64]net.Conn // connID -> user connection
	streams    map[uint64]net.Conn // connID -> smux stream
	nextConnID atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSmuxEntryForwarder creates a new SMUX entry forwarder.
func NewSmuxEntryForwarder(rule *forward.Rule, client tunnel.SmuxTunnelClient) *SmuxEntryForwarder {
	return &SmuxEntryForwarder{
		rule:       rule,
		smuxClient: client,
		traffic:    &TrafficCounter{},
		cb:         newCircuitBreaker(),
		conns:      make(map[uint64]net.Conn),
		streams:    make(map[uint64]net.Conn),
	}
}

// Start starts the SMUX entry forwarder.
func (f *SmuxEntryForwarder) Start(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	protocol := f.rule.Protocol
	if protocol == "" {
		protocol = "tcp"
	}

	// SMUX only supports TCP currently
	if protocol != "tcp" && protocol != "both" {
		return fmt.Errorf("smux forwarder only supports tcp protocol, got: %s", protocol)
	}

	if err := f.startTCP(); err != nil {
		return err
	}

	exitAgentInfo := f.rule.ExitAgentID
	if len(f.rule.ExitAgents) > 0 {
		exitAgentInfo = fmt.Sprintf("%d agents", len(f.rule.ExitAgents))
	}

	logger.Info("smux entry forwarder started",
		"rule_id", f.rule.ID,
		"listen_port", f.rule.ListenPort,
		"exit_agents", exitAgentInfo,
		"protocol", protocol)

	return nil
}

// startTCP starts the TCP listener.
func (f *SmuxEntryForwarder) startTCP() error {
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

// Stop stops the SMUX entry forwarder.
func (f *SmuxEntryForwarder) Stop() error {
	if f.cancel != nil {
		f.cancel()
	}
	if f.tcpListener != nil {
		f.tcpListener.Close()
	}

	// Close all connections and streams
	f.connMu.Lock()
	for _, conn := range f.conns {
		conn.Close()
	}
	for _, stream := range f.streams {
		stream.Close()
	}
	f.conns = make(map[uint64]net.Conn)
	f.streams = make(map[uint64]net.Conn)
	f.connMu.Unlock()

	f.wg.Wait()

	// Stop the SMUX client to release resources
	if f.smuxClient != nil {
		f.smuxClient.Stop()
	}

	logger.Info("smux entry forwarder stopped", "rule_id", f.rule.ID)
	return nil
}

// Traffic returns the traffic counter.
func (f *SmuxEntryForwarder) Traffic() *TrafficCounter {
	return f.traffic
}

// RuleID returns the rule ID.
func (f *SmuxEntryForwarder) RuleID() string {
	return f.rule.ID
}

// ListenPort returns the actual listening port.
func (f *SmuxEntryForwarder) ListenPort() uint16 {
	if f.tcpListener != nil {
		if addr, ok := f.tcpListener.Addr().(*net.TCPAddr); ok {
			return uint16(addr.Port)
		}
	}
	return 0
}

// Connections returns the current number of active connections.
func (f *SmuxEntryForwarder) Connections() int {
	f.connMu.RLock()
	defer f.connMu.RUnlock()
	return len(f.conns)
}

// IsTunnelConnected returns true if the SMUX client is connected.
func (f *SmuxEntryForwarder) IsTunnelConnected() bool {
	if f.smuxClient == nil {
		return false
	}
	return f.smuxClient.IsConnected()
}

func (f *SmuxEntryForwarder) tcpAcceptLoop() {
	runAcceptLoop(acceptLoopConfig{
		ctx:      f.ctx,
		listener: f.tcpListener,
		cb:       f.cb,
		wg:       &f.wg,
		logName:  "smux_entry",
		handler:  f.handleConnection,
	})
}

func (f *SmuxEntryForwarder) handleConnection(userConn net.Conn) {
	defer f.wg.Done()
	defer userConn.Close()

	// Check circuit breaker
	if !f.cb.Allow() {
		logger.Debug("smux entry connection rejected by circuit breaker",
			"rule_id", f.rule.ID,
			"state", f.cb.State())
		return
	}

	connID := f.nextConnID.Add(1)
	logger.Debug("smux entry new connection", "conn_id", connID, "client", userConn.RemoteAddr())

	// Open SMUX stream
	stream, err := f.smuxClient.OpenStream()
	if err != nil {
		f.cb.RecordFailure()
		logger.Error("smux entry open stream failed", "conn_id", connID, "error", err)
		return
	}
	f.cb.RecordSuccess()

	// Register connection and stream
	f.connMu.Lock()
	f.conns[connID] = userConn
	f.streams[connID] = stream
	f.connMu.Unlock()

	defer func() {
		// Cleanup
		f.connMu.Lock()
		delete(f.conns, connID)
		delete(f.streams, connID)
		f.connMu.Unlock()
		stream.Close()
	}()

	// Bidirectional copy with half-close support
	var wg sync.WaitGroup
	wg.Add(2)

	// User -> Stream (upload)
	go func() {
		defer wg.Done()
		n, _ := io.Copy(stream, userConn)
		f.traffic.AddUpload(n)
		// Signal end of data to remote by closing write side
		if cw, ok := stream.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		}
	}()

	// Stream -> User (download)
	go func() {
		defer wg.Done()
		n, _ := io.Copy(userConn, stream)
		f.traffic.AddDownload(n)
		// Signal end of data to user by closing write side
		if cw, ok := userConn.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		}
	}()

	wg.Wait()
	logger.Debug("smux entry connection closed", "conn_id", connID)
}
