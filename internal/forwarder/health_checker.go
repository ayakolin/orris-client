package forwarder

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
)

// maxErrorLength limits error message length to prevent excessive data transmission.
const maxErrorLength = 200

// sanitizeErrorMessage cleans error messages before sending to server.
// It removes potentially sensitive information like IP addresses and file paths,
// and truncates long messages.
func sanitizeErrorMessage(errMsg string) string {
	if errMsg == "" {
		return ""
	}

	// Truncate long messages
	if len(errMsg) > maxErrorLength {
		errMsg = errMsg[:maxErrorLength] + "..."
	}

	// Extract only the error type/category, not full details
	// Common patterns: "connect: connection refused", "dial tcp: i/o timeout"
	if idx := strings.LastIndex(errMsg, ": "); idx != -1 {
		suffix := errMsg[idx+2:]
		// Keep only common error descriptions
		switch {
		case strings.Contains(suffix, "connection refused"):
			return "connection refused"
		case strings.Contains(suffix, "connection reset"):
			return "connection reset"
		case strings.Contains(suffix, "timeout"):
			return "timeout"
		case strings.Contains(suffix, "no route"):
			return "no route to host"
		case strings.Contains(suffix, "network unreachable"):
			return "network unreachable"
		case strings.Contains(suffix, "connection closed"):
			return "connection closed"
		case strings.Contains(suffix, "EOF"):
			return "connection closed"
		}
	}

	// For unknown errors, return a generic message
	return "connection error"
}

// Default health check thresholds.
const (
	DefaultUnhealthyThreshold = 2 // 2 failures -> unhealthy
	DefaultHealthyThreshold   = 1 // 1 success -> healthy
)

// HealthChangeCallback is called when a tunnel's health status changes.
// It receives the health report that should be sent to the server.
type HealthChangeCallback func(report *forward.TunnelHealthReport)

// tunnelHealthState tracks health state for a single tunnel.
type tunnelHealthState struct {
	failureCount atomic.Uint32
	successCount atomic.Uint32
	healthy      atomic.Bool
	lastError    atomic.Value // stores string
}

// HealthChecker manages health state for multiple tunnels.
// It uses configurable thresholds to determine when a tunnel becomes unhealthy or healthy.
type HealthChecker struct {
	ruleID             string
	unhealthyThreshold uint32
	healthyThreshold   uint32
	states             []*tunnelHealthState
	exitAgentIDs       []string // exit agent ID for each tunnel (immutable after creation)

	callbackMu     sync.RWMutex         // protects onHealthChange
	onHealthChange HealthChangeCallback // callback when health changes
}

// NewHealthChecker creates a HealthChecker for the given number of tunnels.
// If config is nil, default thresholds are used.
// exitAgentIDs should match the tunnel indices for health reporting.
func NewHealthChecker(ruleID string, tunnelCount int, config *forward.HealthCheckConfig, exitAgentIDs []string) *HealthChecker {
	unhealthyThreshold := uint32(DefaultUnhealthyThreshold)
	healthyThreshold := uint32(DefaultHealthyThreshold)

	if config != nil {
		if config.UnhealthyThreshold > 0 {
			unhealthyThreshold = config.UnhealthyThreshold
		}
		if config.HealthyThreshold > 0 {
			healthyThreshold = config.HealthyThreshold
		}
	}

	states := make([]*tunnelHealthState, tunnelCount)
	for i := range states {
		states[i] = &tunnelHealthState{}
		states[i].healthy.Store(true) // start healthy
	}

	// Deep copy exitAgentIDs to prevent external modification
	var agentIDsCopy []string
	if len(exitAgentIDs) > 0 {
		agentIDsCopy = make([]string, len(exitAgentIDs))
		copy(agentIDsCopy, exitAgentIDs)
	}

	return &HealthChecker{
		ruleID:             ruleID,
		unhealthyThreshold: unhealthyThreshold,
		healthyThreshold:   healthyThreshold,
		states:             states,
		exitAgentIDs:       agentIDsCopy,
	}
}

// SetOnHealthChange sets the callback for health status changes.
// This method is thread-safe.
func (h *HealthChecker) SetOnHealthChange(callback HealthChangeCallback) {
	h.callbackMu.Lock()
	h.onHealthChange = callback
	h.callbackMu.Unlock()
}

// RecordFailure records a failure for the tunnel at index.
// Returns true if the tunnel became unhealthy.
func (h *HealthChecker) RecordFailure(idx int) bool {
	return h.RecordFailureWithError(idx, "")
}

// RecordFailureWithError records a failure with an error message.
// Returns true if the tunnel became unhealthy.
func (h *HealthChecker) RecordFailureWithError(idx int, errMsg string) bool {
	if idx < 0 || idx >= len(h.states) {
		return false
	}

	state := h.states[idx]
	state.successCount.Store(0) // reset success count
	if errMsg != "" {
		state.lastError.Store(errMsg)
	}
	failures := state.failureCount.Add(1)

	if failures >= h.unhealthyThreshold && state.healthy.CompareAndSwap(true, false) {
		logger.Warn("tunnel marked unhealthy",
			"rule_id", h.ruleID,
			"tunnel_idx", idx,
			"failure_count", failures)

		// Notify health change
		h.notifyHealthChange(idx, false, int(failures), errMsg, nil)
		return true
	}
	return false
}

// RecordSuccess records a success for the tunnel at index.
// Returns true if the tunnel became healthy.
func (h *HealthChecker) RecordSuccess(idx int) bool {
	return h.RecordSuccessWithLatency(idx, nil)
}

// RecordSuccessWithLatency records a success with optional latency measurement.
// Returns true if the tunnel became healthy.
func (h *HealthChecker) RecordSuccessWithLatency(idx int, latencyMs *int64) bool {
	if idx < 0 || idx >= len(h.states) {
		return false
	}

	state := h.states[idx]
	state.failureCount.Store(0) // reset failure count
	state.lastError.Store("")   // clear error
	successes := state.successCount.Add(1)

	if successes >= h.healthyThreshold && state.healthy.CompareAndSwap(false, true) {
		logger.Info("tunnel marked healthy",
			"rule_id", h.ruleID,
			"tunnel_idx", idx)

		// Notify health change
		h.notifyHealthChange(idx, true, 0, "", latencyMs)
		return true
	}
	return false
}

// IsHealthy returns the health status of tunnel at index.
func (h *HealthChecker) IsHealthy(idx int) bool {
	if idx < 0 || idx >= len(h.states) {
		return false
	}
	return h.states[idx].healthy.Load()
}

// GetHealthyIndices returns indices of all healthy tunnels.
func (h *HealthChecker) GetHealthyIndices() []int {
	indices := make([]int, 0, len(h.states))
	for i, state := range h.states {
		if state.healthy.Load() {
			indices = append(indices, i)
		}
	}
	return indices
}

// HealthyCount returns the number of healthy tunnels.
func (h *HealthChecker) HealthyCount() int {
	count := 0
	for _, state := range h.states {
		if state.healthy.Load() {
			count++
		}
	}
	return count
}

// TunnelCount returns the total number of tunnels being monitored.
func (h *HealthChecker) TunnelCount() int {
	return len(h.states)
}

// notifyHealthChange calls the callback with a health report if set.
// This method is thread-safe.
func (h *HealthChecker) notifyHealthChange(idx int, healthy bool, failCount int, errMsg string, latencyMs *int64) {
	h.callbackMu.RLock()
	callback := h.onHealthChange
	h.callbackMu.RUnlock()

	if callback == nil {
		return
	}

	// Get exit agent ID for this tunnel
	var exitAgentID string
	if idx >= 0 && idx < len(h.exitAgentIDs) {
		exitAgentID = h.exitAgentIDs[idx]
	}

	report := &forward.TunnelHealthReport{
		RuleID:      h.ruleID,
		ExitAgentID: exitAgentID,
		Healthy:     healthy,
		FailCount:   failCount,
		Error:       sanitizeErrorMessage(errMsg),
		LatencyMs:   latencyMs,
		CheckedAt:   time.Now().Unix(),
	}

	// Call callback in goroutine to avoid blocking
	go callback(report)
}
