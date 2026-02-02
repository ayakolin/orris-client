package forwarder

import (
	"testing"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"
)

func TestNewHealthChecker(t *testing.T) {
	tests := []struct {
		name        string
		tunnelCount int
		config      *forward.HealthCheckConfig
		wantUH      uint32 // unhealthy threshold
		wantH       uint32 // healthy threshold
	}{
		{
			name:        "nil config uses defaults",
			tunnelCount: 3,
			config:      nil,
			wantUH:      DefaultUnhealthyThreshold,
			wantH:       DefaultHealthyThreshold,
		},
		{
			name:        "custom config",
			tunnelCount: 2,
			config:      &forward.HealthCheckConfig{UnhealthyThreshold: 3, HealthyThreshold: 2},
			wantUH:      3,
			wantH:       2,
		},
		{
			name:        "zero values use defaults",
			tunnelCount: 2,
			config:      &forward.HealthCheckConfig{UnhealthyThreshold: 0, HealthyThreshold: 0},
			wantUH:      DefaultUnhealthyThreshold,
			wantH:       DefaultHealthyThreshold,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hc := NewHealthChecker("test_rule", tt.tunnelCount, tt.config, nil)
			if hc.unhealthyThreshold != tt.wantUH {
				t.Errorf("unhealthyThreshold = %d, want %d", hc.unhealthyThreshold, tt.wantUH)
			}
			if hc.healthyThreshold != tt.wantH {
				t.Errorf("healthyThreshold = %d, want %d", hc.healthyThreshold, tt.wantH)
			}
			if hc.TunnelCount() != tt.tunnelCount {
				t.Errorf("TunnelCount() = %d, want %d", hc.TunnelCount(), tt.tunnelCount)
			}
			// All tunnels should start healthy
			if hc.HealthyCount() != tt.tunnelCount {
				t.Errorf("HealthyCount() = %d, want %d", hc.HealthyCount(), tt.tunnelCount)
			}
		})
	}
}

func TestHealthChecker_RecordFailure(t *testing.T) {
	// Use default thresholds: unhealthy=2, healthy=1
	hc := NewHealthChecker("test_rule", 3, nil, nil)

	// All tunnels start healthy
	if !hc.IsHealthy(0) {
		t.Error("tunnel 0 should start healthy")
	}

	// First failure should not mark unhealthy
	becameUnhealthy := hc.RecordFailure(0)
	if becameUnhealthy {
		t.Error("first failure should not mark unhealthy")
	}
	if !hc.IsHealthy(0) {
		t.Error("tunnel 0 should still be healthy after 1 failure")
	}

	// Second failure should mark unhealthy
	becameUnhealthy = hc.RecordFailure(0)
	if !becameUnhealthy {
		t.Error("second failure should mark unhealthy")
	}
	if hc.IsHealthy(0) {
		t.Error("tunnel 0 should be unhealthy after 2 failures")
	}

	// Other tunnels should remain healthy
	if !hc.IsHealthy(1) || !hc.IsHealthy(2) {
		t.Error("other tunnels should remain healthy")
	}

	// HealthyCount should be 2
	if hc.HealthyCount() != 2 {
		t.Errorf("HealthyCount() = %d, want 2", hc.HealthyCount())
	}
}

func TestHealthChecker_RecordSuccess(t *testing.T) {
	// Use default thresholds: unhealthy=2, healthy=1
	hc := NewHealthChecker("test_rule", 2, nil, nil)

	// Mark tunnel 0 as unhealthy
	hc.RecordFailure(0)
	hc.RecordFailure(0)
	if hc.IsHealthy(0) {
		t.Error("tunnel 0 should be unhealthy")
	}

	// One success should restore health (healthy threshold = 1)
	becameHealthy := hc.RecordSuccess(0)
	if !becameHealthy {
		t.Error("success should mark tunnel healthy")
	}
	if !hc.IsHealthy(0) {
		t.Error("tunnel 0 should be healthy after success")
	}
}

func TestHealthChecker_RecordSuccessResetsFailureCount(t *testing.T) {
	hc := NewHealthChecker("test_rule", 1, nil, nil)

	// Record one failure (not enough to mark unhealthy)
	hc.RecordFailure(0)

	// Success should reset failure count
	hc.RecordSuccess(0)

	// Now one more failure should not mark unhealthy
	becameUnhealthy := hc.RecordFailure(0)
	if becameUnhealthy {
		t.Error("failure count should have been reset by success")
	}
}

func TestHealthChecker_GetHealthyIndices(t *testing.T) {
	hc := NewHealthChecker("test_rule", 3, nil, nil)

	// All healthy initially
	indices := hc.GetHealthyIndices()
	if len(indices) != 3 {
		t.Errorf("GetHealthyIndices() length = %d, want 3", len(indices))
	}

	// Mark tunnel 1 unhealthy
	hc.RecordFailure(1)
	hc.RecordFailure(1)

	indices = hc.GetHealthyIndices()
	if len(indices) != 2 {
		t.Errorf("GetHealthyIndices() length = %d, want 2", len(indices))
	}

	// Should contain 0 and 2, not 1
	foundZero, foundTwo := false, false
	for _, idx := range indices {
		if idx == 0 {
			foundZero = true
		}
		if idx == 2 {
			foundTwo = true
		}
		if idx == 1 {
			t.Error("GetHealthyIndices() should not contain unhealthy tunnel 1")
		}
	}
	if !foundZero || !foundTwo {
		t.Error("GetHealthyIndices() should contain indices 0 and 2")
	}
}

func TestHealthChecker_InvalidIndex(t *testing.T) {
	hc := NewHealthChecker("test_rule", 2, nil, nil)

	// Out of range indices should not panic and return false
	if hc.RecordFailure(-1) {
		t.Error("RecordFailure(-1) should return false")
	}
	if hc.RecordFailure(5) {
		t.Error("RecordFailure(5) should return false")
	}
	if hc.RecordSuccess(-1) {
		t.Error("RecordSuccess(-1) should return false")
	}
	if hc.RecordSuccess(5) {
		t.Error("RecordSuccess(5) should return false")
	}
	if hc.IsHealthy(-1) {
		t.Error("IsHealthy(-1) should return false")
	}
	if hc.IsHealthy(5) {
		t.Error("IsHealthy(5) should return false")
	}
}

func TestHealthChecker_CustomThresholds(t *testing.T) {
	config := &forward.HealthCheckConfig{
		UnhealthyThreshold: 3,
		HealthyThreshold:   2,
	}
	hc := NewHealthChecker("test_rule", 1, config, nil)

	// Need 3 failures to become unhealthy
	hc.RecordFailure(0)
	if !hc.IsHealthy(0) {
		t.Error("should still be healthy after 1 failure")
	}
	hc.RecordFailure(0)
	if !hc.IsHealthy(0) {
		t.Error("should still be healthy after 2 failures")
	}
	hc.RecordFailure(0)
	if hc.IsHealthy(0) {
		t.Error("should be unhealthy after 3 failures")
	}

	// Need 2 successes to become healthy
	hc.RecordSuccess(0)
	if hc.IsHealthy(0) {
		t.Error("should still be unhealthy after 1 success")
	}
	hc.RecordSuccess(0)
	if !hc.IsHealthy(0) {
		t.Error("should be healthy after 2 successes")
	}
}

func TestHealthChecker_OnHealthChangeCallback(t *testing.T) {
	exitAgentIDs := []string{"fa_agent1", "fa_agent2"}
	hc := NewHealthChecker("fr_test_rule", 2, nil, exitAgentIDs)

	reportCh := make(chan *forward.TunnelHealthReport, 10)
	hc.SetOnHealthChange(func(report *forward.TunnelHealthReport) {
		reportCh <- report
	})

	// Mark tunnel 0 unhealthy (need 2 failures)
	// Use error message that contains "connection refused" which will be sanitized
	hc.RecordFailureWithError(0, "dial tcp 10.0.0.1:8080: connection refused")
	hc.RecordFailureWithError(0, "dial tcp 10.0.0.1:8080: connection refused")

	// Wait for async callback with timeout
	select {
	case report := <-reportCh:
		if report.RuleID != "fr_test_rule" {
			t.Errorf("RuleID = %q, want %q", report.RuleID, "fr_test_rule")
		}
		if report.ExitAgentID != "fa_agent1" {
			t.Errorf("ExitAgentID = %q, want %q", report.ExitAgentID, "fa_agent1")
		}
		if report.Healthy {
			t.Error("Healthy should be false")
		}
		if report.FailCount != 2 {
			t.Errorf("FailCount = %d, want 2", report.FailCount)
		}
		// Error should be sanitized to remove IP addresses
		if report.Error != "connection refused" {
			t.Errorf("Error = %q, want %q (sanitized)", report.Error, "connection refused")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for unhealthy report")
	}

	// Restore health
	hc.RecordSuccess(0)

	// Wait for async callback with timeout
	select {
	case report := <-reportCh:
		if !report.Healthy {
			t.Error("Healthy should be true after recovery")
		}
		if report.FailCount != 0 {
			t.Errorf("FailCount = %d, want 0", report.FailCount)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for healthy report")
	}
}

func TestSanitizeErrorMessage(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "connection refused with IP",
			input: "dial tcp 192.168.1.100:8080: connection refused",
			want:  "connection refused",
		},
		{
			name:  "timeout with IP",
			input: "dial tcp 10.0.0.1:443: i/o timeout",
			want:  "timeout",
		},
		{
			name:  "connection reset",
			input: "read tcp: connection reset by peer",
			want:  "connection reset",
		},
		{
			name:  "EOF",
			input: "read: unexpected EOF",
			want:  "connection closed",
		},
		{
			name:  "network unreachable",
			input: "dial tcp: network unreachable",
			want:  "network unreachable",
		},
		{
			name:  "unknown error",
			input: "some unknown error occurred",
			want:  "connection error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeErrorMessage(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeErrorMessage(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
