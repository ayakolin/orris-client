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
	"github.com/orris-inc/orris-client/internal/tunnel"
)

// SmuxRelayForwarder handles relay forwarding using SMUX streams.
// It accepts incoming SMUX streams from previous hop and forwards them to next hop via SMUX.
type SmuxRelayForwarder struct {
	rule    *forward.Rule
	traffic *TrafficCounter

	// Outbound SMUX client to next hop
	smuxClient tunnel.SmuxTunnelClient

	// Active streams tracking
	streamMu     sync.RWMutex
	inStreams    map[uint64]net.Conn // streamID -> incoming stream
	outStreams   map[uint64]net.Conn // streamID -> outgoing stream
	nextStreamID atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSmuxRelayForwarder creates a new SMUX relay forwarder.
func NewSmuxRelayForwarder(rule *forward.Rule, client tunnel.SmuxTunnelClient) *SmuxRelayForwarder {
	return &SmuxRelayForwarder{
		rule:       rule,
		smuxClient: client,
		traffic:    &TrafficCounter{},
		inStreams:  make(map[uint64]net.Conn),
		outStreams: make(map[uint64]net.Conn),
	}
}

// Start starts the SMUX relay forwarder.
func (f *SmuxRelayForwarder) Start(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	logger.Info("smux relay forwarder started",
		"rule_id", f.rule.ID,
		"next_hop", net.JoinHostPort(f.rule.NextHopAddress, fmt.Sprintf("%d", f.rule.NextHopPort)))
	return nil
}

// Stop stops the SMUX relay forwarder.
func (f *SmuxRelayForwarder) Stop() error {
	if f.cancel != nil {
		f.cancel()
	}

	// Close all streams
	f.streamMu.Lock()
	for _, stream := range f.inStreams {
		stream.Close()
	}
	for _, stream := range f.outStreams {
		stream.Close()
	}
	f.inStreams = make(map[uint64]net.Conn)
	f.outStreams = make(map[uint64]net.Conn)
	f.streamMu.Unlock()

	f.wg.Wait()

	// Stop the outbound SMUX client to release resources
	if f.smuxClient != nil {
		f.smuxClient.Stop()
	}

	logger.Info("smux relay forwarder stopped", "rule_id", f.rule.ID)
	return nil
}

// Traffic returns the traffic counter.
func (f *SmuxRelayForwarder) Traffic() *TrafficCounter {
	return f.traffic
}

// RuleID returns the rule ID.
func (f *SmuxRelayForwarder) RuleID() string {
	return f.rule.ID
}

// ListenPort returns 0 as SmuxRelayForwarder does not have a listening port.
func (f *SmuxRelayForwarder) ListenPort() uint16 {
	return 0
}

// Connections returns the current number of active streams.
func (f *SmuxRelayForwarder) Connections() int {
	f.streamMu.RLock()
	defer f.streamMu.RUnlock()
	return len(f.inStreams)
}

// HandleStream handles an incoming SMUX stream from previous hop.
// Implements tunnel.SmuxStreamHandler interface.
func (f *SmuxRelayForwarder) HandleStream(inStream net.Conn) {
	// Track this stream in WaitGroup for graceful shutdown
	f.wg.Add(1)
	defer f.wg.Done()

	streamID := f.nextStreamID.Add(1)

	// Open outbound stream to next hop
	outStream, err := f.smuxClient.OpenStream()
	if err != nil {
		logger.Error("smux relay open outbound stream failed", "stream_id", streamID, "error", err)
		inStream.Close()
		return
	}

	// Register streams
	f.streamMu.Lock()
	f.inStreams[streamID] = inStream
	f.outStreams[streamID] = outStream
	f.streamMu.Unlock()

	logger.Debug("smux relay stream connected", "stream_id", streamID)

	defer func() {
		// Cleanup
		f.streamMu.Lock()
		delete(f.inStreams, streamID)
		delete(f.outStreams, streamID)
		f.streamMu.Unlock()
		inStream.Close()
		outStream.Close()
		logger.Debug("smux relay stream closed", "stream_id", streamID)
	}()

	// Bidirectional copy with half-close support
	var copyWg sync.WaitGroup
	copyWg.Add(2)

	// Incoming -> Outgoing (upload direction)
	go func() {
		defer copyWg.Done()
		n, _ := io.Copy(outStream, inStream)
		f.traffic.AddUpload(n)
		// Signal end of data to outbound stream
		if cw, ok := outStream.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		}
	}()

	// Outgoing -> Incoming (download direction)
	go func() {
		defer copyWg.Done()
		n, _ := io.Copy(inStream, outStream)
		f.traffic.AddDownload(n)
		// Signal end of data to inbound stream
		if cw, ok := inStream.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		}
	}()

	copyWg.Wait()
}

// OnSessionClose is called when the SMUX session is closed.
// Implements tunnel.SmuxStreamHandler interface.
func (f *SmuxRelayForwarder) OnSessionClose() {
	// Close all active streams
	f.streamMu.Lock()
	for _, stream := range f.inStreams {
		stream.Close()
	}
	for _, stream := range f.outStreams {
		stream.Close()
	}
	f.streamMu.Unlock()

	logger.Debug("smux relay forwarder session closed, closed all streams", "rule_id", f.rule.ID)
}

// SmuxBoundaryRelayForwarder handles boundary relay forwarding using SMUX streams.
// It accepts incoming SMUX streams and forwards them to next hop via direct TCP connection.
type SmuxBoundaryRelayForwarder struct {
	rule    *forward.Rule
	traffic *TrafficCounter

	// Circuit breaker to prevent connection storms
	cb *circuitBreaker

	// Active streams tracking
	streamMu     sync.RWMutex
	streams      map[uint64]net.Conn // streamID -> incoming stream
	targets      map[uint64]net.Conn // streamID -> target connection
	nextStreamID atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSmuxBoundaryRelayForwarder creates a new SMUX boundary relay forwarder.
func NewSmuxBoundaryRelayForwarder(rule *forward.Rule) *SmuxBoundaryRelayForwarder {
	return &SmuxBoundaryRelayForwarder{
		rule:    rule,
		traffic: &TrafficCounter{},
		cb:      newCircuitBreaker(),
		streams: make(map[uint64]net.Conn),
		targets: make(map[uint64]net.Conn),
	}
}

// Start starts the SMUX boundary relay forwarder.
func (f *SmuxBoundaryRelayForwarder) Start(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	nextHop := net.JoinHostPort(f.rule.NextHopAddress, fmt.Sprintf("%d", f.rule.NextHopPort))
	logger.Info("smux boundary relay forwarder started",
		"rule_id", f.rule.ID,
		"next_hop", nextHop)
	return nil
}

// Stop stops the SMUX boundary relay forwarder.
func (f *SmuxBoundaryRelayForwarder) Stop() error {
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
	logger.Info("smux boundary relay forwarder stopped", "rule_id", f.rule.ID)
	return nil
}

// Traffic returns the traffic counter.
func (f *SmuxBoundaryRelayForwarder) Traffic() *TrafficCounter {
	return f.traffic
}

// RuleID returns the rule ID.
func (f *SmuxBoundaryRelayForwarder) RuleID() string {
	return f.rule.ID
}

// ListenPort returns 0 as SmuxBoundaryRelayForwarder does not have a listening port.
func (f *SmuxBoundaryRelayForwarder) ListenPort() uint16 {
	return 0
}

// Connections returns the current number of active streams.
func (f *SmuxBoundaryRelayForwarder) Connections() int {
	f.streamMu.RLock()
	defer f.streamMu.RUnlock()
	return len(f.streams)
}

// HandleStream handles an incoming SMUX stream and forwards to next hop via direct TCP.
// Implements tunnel.SmuxStreamHandler interface.
func (f *SmuxBoundaryRelayForwarder) HandleStream(stream net.Conn) {
	// Track this stream in WaitGroup for graceful shutdown
	f.wg.Add(1)
	defer f.wg.Done()

	streamID := f.nextStreamID.Add(1)
	nextHop := net.JoinHostPort(f.rule.NextHopAddress, fmt.Sprintf("%d", f.rule.NextHopPort))

	// Check circuit breaker
	if !f.cb.Allow() {
		logger.Debug("smux boundary relay stream rejected by circuit breaker",
			"stream_id", streamID,
			"next_hop", nextHop,
			"state", f.cb.State())
		stream.Close()
		return
	}

	// Connect to next hop via direct TCP with reasonable timeout
	// Use DialContext to support cancellation when forwarder is stopped
	dialCtx, dialCancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer dialCancel()

	dialer := &net.Dialer{}
	targetConn, err := dialer.DialContext(dialCtx, "tcp", nextHop)
	if err != nil {
		f.cb.RecordFailure()
		logger.Error("smux boundary relay dial next hop failed",
			"stream_id", streamID,
			"next_hop", nextHop,
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

	logger.Debug("smux boundary relay stream connected", "stream_id", streamID, "next_hop", nextHop)

	defer func() {
		// Cleanup
		f.streamMu.Lock()
		delete(f.streams, streamID)
		delete(f.targets, streamID)
		f.streamMu.Unlock()
		stream.Close()
		targetConn.Close()
		logger.Debug("smux boundary relay stream closed", "stream_id", streamID)
	}()

	// Bidirectional copy with half-close support
	var copyWg sync.WaitGroup
	copyWg.Add(2)

	// Stream -> Target (upload direction)
	go func() {
		defer copyWg.Done()
		n, _ := io.Copy(targetConn, stream)
		f.traffic.AddUpload(n)
		// Signal end of data to target
		if cw, ok := targetConn.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		}
	}()

	// Target -> Stream (download direction)
	go func() {
		defer copyWg.Done()
		n, _ := io.Copy(stream, targetConn)
		f.traffic.AddDownload(n)
		// Signal end of data to stream
		if cw, ok := stream.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		}
	}()

	copyWg.Wait()
}

// OnSessionClose is called when the SMUX session is closed.
// Implements tunnel.SmuxStreamHandler interface.
func (f *SmuxBoundaryRelayForwarder) OnSessionClose() {
	// Close all active streams and targets
	f.streamMu.Lock()
	for _, stream := range f.streams {
		stream.Close()
	}
	for _, target := range f.targets {
		target.Close()
	}
	f.streamMu.Unlock()

	logger.Debug("smux boundary relay forwarder session closed, closed all connections", "rule_id", f.rule.ID)
}
