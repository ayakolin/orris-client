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

// SmuxExitForwarder handles exit forwarding using SMUX streams.
// Each incoming SMUX stream is connected to the target for bidirectional data transfer.
type SmuxExitForwarder struct {
	rule    *forward.Rule
	traffic *TrafficCounter

	// Circuit breaker to prevent connection storms when target is unreachable
	cb *circuitBreaker

	// Active streams tracking
	streamMu     sync.RWMutex
	streams      map[uint64]net.Conn // streamID -> stream
	targets      map[uint64]net.Conn // streamID -> target connection
	nextStreamID atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSmuxExitForwarder creates a new SMUX exit forwarder.
func NewSmuxExitForwarder(rule *forward.Rule) *SmuxExitForwarder {
	return &SmuxExitForwarder{
		rule:    rule,
		traffic: &TrafficCounter{},
		cb:      newCircuitBreaker(),
		streams: make(map[uint64]net.Conn),
		targets: make(map[uint64]net.Conn),
	}
}

// Start starts the SMUX exit forwarder.
func (f *SmuxExitForwarder) Start(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	if f.rule.TargetAddress == "" {
		return fmt.Errorf("target address is empty")
	}

	logger.Info("smux exit forwarder started",
		"rule_id", f.rule.ID,
		"target", fmt.Sprintf("%s:%d", f.rule.TargetAddress, f.rule.TargetPort))
	return nil
}

// Stop stops the SMUX exit forwarder.
func (f *SmuxExitForwarder) Stop() error {
	if f.cancel != nil {
		f.cancel()
	}

	// Close all streams and targets
	f.streamMu.Lock()
	for _, stream := range f.streams {
		stream.Close()
	}
	for _, target := range f.targets {
		target.Close()
	}
	f.streams = make(map[uint64]net.Conn)
	f.targets = make(map[uint64]net.Conn)
	f.streamMu.Unlock()

	f.wg.Wait()
	logger.Info("smux exit forwarder stopped", "rule_id", f.rule.ID)
	return nil
}

// Traffic returns the traffic counter.
func (f *SmuxExitForwarder) Traffic() *TrafficCounter {
	return f.traffic
}

// RuleID returns the rule ID.
func (f *SmuxExitForwarder) RuleID() string {
	return f.rule.ID
}

// ListenIP returns empty because SmuxExitForwarder does not listen locally.
func (f *SmuxExitForwarder) ListenIP() string {
	return ""
}

// ListenPort returns 0 as SmuxExitForwarder does not have a listening port.
func (f *SmuxExitForwarder) ListenPort() uint16 {
	return 0
}

// Connections returns the current number of active streams.
func (f *SmuxExitForwarder) Connections() int {
	f.streamMu.RLock()
	defer f.streamMu.RUnlock()
	return len(f.streams)
}

// HandleStream handles an incoming SMUX stream.
// Implements tunnel.SmuxStreamHandler interface.
func (f *SmuxExitForwarder) HandleStream(stream net.Conn) {
	// Track this stream in WaitGroup for graceful shutdown
	f.wg.Add(1)
	defer f.wg.Done()

	streamID := f.nextStreamID.Add(1)
	targetAddr := net.JoinHostPort(f.rule.TargetAddress, fmt.Sprintf("%d", f.rule.TargetPort))

	// Check circuit breaker
	if !f.cb.Allow() {
		logger.Debug("smux exit stream rejected by circuit breaker",
			"stream_id", streamID,
			"target", targetAddr,
			"state", f.cb.State())
		stream.Close()
		return
	}

	// Connect to target with reasonable timeout for cross-region connections
	// Use DialContext to support cancellation when forwarder is stopped
	dialCtx, dialCancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer dialCancel()

	dialer := &net.Dialer{}
	if f.rule.BindIP != "" {
		dialer.LocalAddr = &net.TCPAddr{IP: net.ParseIP(f.rule.BindIP)}
	}

	targetConn, err := dialer.DialContext(dialCtx, "tcp", targetAddr)
	if err != nil {
		f.cb.RecordFailure()
		logger.Error("smux exit dial target failed",
			"stream_id", streamID,
			"target", targetAddr,
			"bind_ip", f.rule.BindIP,
			"cb_state", f.cb.State(),
			"error", err)
		stream.Close()
		return
	}
	f.cb.RecordSuccess()

	// Register stream and target
	f.streamMu.Lock()
	f.streams[streamID] = stream
	f.targets[streamID] = targetConn
	f.streamMu.Unlock()

	logger.Debug("smux exit stream connected to target", "stream_id", streamID, "target", targetAddr)

	defer func() {
		// Cleanup
		f.streamMu.Lock()
		delete(f.streams, streamID)
		delete(f.targets, streamID)
		f.streamMu.Unlock()
		stream.Close()
		targetConn.Close()
		logger.Debug("smux exit stream closed", "stream_id", streamID)
	}()

	// Bidirectional copy with half-close support
	var copyWg sync.WaitGroup
	copyWg.Add(2)

	// Stream -> Target (upload from entry's perspective)
	go func() {
		defer copyWg.Done()
		n, _ := io.Copy(targetConn, stream)
		f.traffic.AddUpload(n)
		// Signal end of data to target by closing write side
		if cw, ok := targetConn.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		}
	}()

	// Target -> Stream (download from entry's perspective)
	go func() {
		defer copyWg.Done()
		n, _ := io.Copy(stream, targetConn)
		f.traffic.AddDownload(n)
		// Signal end of data to stream by closing write side
		if cw, ok := stream.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		}
	}()

	copyWg.Wait()
}

// OnSessionClose is called when the SMUX session is closed.
// Implements tunnel.SmuxStreamHandler interface.
func (f *SmuxExitForwarder) OnSessionClose() {
	// Close all active streams and targets
	f.streamMu.Lock()
	for _, stream := range f.streams {
		stream.Close()
	}
	for _, target := range f.targets {
		target.Close()
	}
	f.streamMu.Unlock()

	logger.Debug("smux exit forwarder session closed, closed all connections", "rule_id", f.rule.ID)
}
