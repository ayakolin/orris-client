package agent

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/forwarder"
	"github.com/orris-inc/orris-client/internal/logger"
)

// executeProbe executes a probe task and returns the result.
func (a *Agent) executeProbe(ctx context.Context, task *forward.ProbeTask) *forward.ProbeTaskResult {
	switch task.Type {
	case forward.ProbeTaskTypeTunnelPing:
		return a.executeTunnelPing(ctx, task)
	default:
		return a.executeBasicProbe(task)
	}
}

// executeBasicProbe executes target and tunnel probe tasks.
func (a *Agent) executeBasicProbe(task *forward.ProbeTask) *forward.ProbeTaskResult {
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
	case forward.ProbeTaskTypeTunnel:
		result.Success, result.Error = a.probeTunnelByRule(task.RuleID)
	default:
		result.Success = false
		result.Error = fmt.Sprintf("unknown probe type: %s", task.Type)
	}

	result.LatencyMs = time.Since(start).Milliseconds()

	logger.Debug("probe executed",
		"task_id", task.ID,
		"type", task.Type,
		"success", result.Success,
		"latency_ms", result.LatencyMs)

	return result
}

// executeTunnelPing executes a tunnel ping probe task.
func (a *Agent) executeTunnelPing(ctx context.Context, task *forward.ProbeTask) *forward.ProbeTaskResult {
	result := forward.ExecuteTunnelPing(ctx, task)

	logger.Debug("tunnel ping executed",
		"task_id", task.ID,
		"type", task.Type,
		"success", result.Success,
		"avg_latency_ms", result.AvgLatencyMs,
		"packet_loss", result.PacketLoss)

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

// probeTunnelByRule probes tunnel connectivity by rule ID.
func (a *Agent) probeTunnelByRule(ruleID string) (bool, string) {
	// Find the forwarder for this rule
	a.forwardersMu.RLock()
	f, exists := a.forwarders[ruleID]
	a.forwardersMu.RUnlock()

	if !exists {
		return false, "rule not found"
	}

	// Check if it's an entry forwarder with a tunnel
	if ef, ok := f.(*forwarder.EntryForwarder); ok {
		if ef.IsTunnelConnected() {
			return true, ""
		}
		return false, "tunnel disconnected"
	}

	// For non-entry rules, just check if forwarder exists
	return true, ""
}
