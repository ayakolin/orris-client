package agent

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
)

// executeProbe executes a probe task and returns the result.
func (a *Agent) executeProbe(ctx context.Context, task *forward.ProbeTask) *forward.ProbeTaskResult {
	switch task.Type {
	case forward.ProbeTaskTypeTunnelPing:
		return a.executeTunnelPing(ctx, task)
	default:
		return a.executeBasicProbe(ctx, task)
	}
}

// executeBasicProbe executes target and tunnel probe tasks.
func (a *Agent) executeBasicProbe(ctx context.Context, task *forward.ProbeTask) *forward.ProbeTaskResult {
	result := &forward.ProbeTaskResult{
		TaskID: task.ID,
		Type:   task.Type,
		RuleID: task.RuleID,
	}

	timeout := time.Duration(task.Timeout) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	start := time.Now()

	switch task.Type {
	case forward.ProbeTaskTypeTarget:
		result.Success, result.Error = a.probeTarget(task.Target, task.Port, task.Protocol, timeout)
		result.LatencyMs = time.Since(start).Milliseconds()
	case forward.ProbeTaskTypeTunnel:
		var latency time.Duration
		result.Success, result.Error, latency = a.probeTunnelByRule(ctx, task.RuleID, timeout)
		result.LatencyMs = latency.Milliseconds()
	default:
		result.Success = false
		result.Error = fmt.Sprintf("unknown probe type: %s", task.Type)
		result.LatencyMs = time.Since(start).Milliseconds()
	}

	logger.Debug("probe executed",
		"task_id", task.ID,
		"type", task.Type,
		"success", result.Success,
		"latency_ms", result.LatencyMs)

	return result
}

// executeTunnelPing executes a tunnel ping probe task.
func (a *Agent) executeTunnelPing(ctx context.Context, task *forward.ProbeTask) *forward.ProbeTaskResult {
	logger.Info("executing tunnel ping",
		"task_id", task.ID,
		"target", task.Target,
		"port", task.Port,
		"tunnel_type", task.TunnelType,
		"rule_id", task.RuleID)

	result := forward.ExecuteTunnelPing(ctx, task)

	logger.Info("tunnel ping executed",
		"task_id", task.ID,
		"success", result.Success,
		"error", result.Error,
		"avg_latency_ms", result.AvgLatencyMs,
		"min_latency_ms", result.MinLatencyMs,
		"max_latency_ms", result.MaxLatencyMs,
		"packet_loss", result.PacketLoss,
		"pings_sent", result.PingsSent,
		"pings_recv", result.PingsRecv)

	return result
}

// probeTarget probes a target address for connectivity.
func (a *Agent) probeTarget(target string, port uint16, protocol string, timeout time.Duration) (bool, string) {
	addr := net.JoinHostPort(target, fmt.Sprintf("%d", port))

	conn, err := net.DialTimeout(protocol, addr, timeout)
	if err != nil {
		return false, err.Error()
	}
	conn.Close()

	return true, ""
}

// tunnelProber is implemented by forwarders that support tunnel ping.
type tunnelProber interface {
	IsTunnelConnected() bool
	PingTunnel(ctx context.Context) (time.Duration, error)
}

// probeTunnelByRule probes tunnel connectivity by rule ID and returns the measured latency.
func (a *Agent) probeTunnelByRule(ctx context.Context, ruleID string, timeout time.Duration) (bool, string, time.Duration) {
	// Find the forwarder for this rule
	a.forwardersMu.RLock()
	f, exists := a.forwarders[ruleID]
	a.forwardersMu.RUnlock()

	if !exists {
		return false, "rule not found", 0
	}

	// Check if forwarder supports tunnel probing
	prober, ok := f.(tunnelProber)
	if !ok {
		logger.Debug("probe tunnel: forwarder does not support ping", "rule_id", ruleID, "type", fmt.Sprintf("%T", f))
		// For forwarders without tunnel, just check if it exists
		return true, "", 0
	}

	if !prober.IsTunnelConnected() {
		return false, "tunnel disconnected", 0
	}

	// Measure actual tunnel latency via ping
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	latency, err := prober.PingTunnel(pingCtx)
	if err != nil {
		logger.Debug("probe tunnel: ping failed", "rule_id", ruleID, "error", err)
		return false, fmt.Sprintf("ping failed: %v", err), 0
	}

	logger.Debug("probe tunnel: ping success", "rule_id", ruleID, "latency", latency)
	return true, "", latency
}
