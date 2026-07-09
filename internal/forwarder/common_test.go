package forwarder

import (
	"testing"
	"time"
)

// targetDialTimeout must leave enough margin for a single dropped SYN
// (typical Linux initial retransmit RTO is ~1s) plus ordinary cross-region
// handshake latency (100-300ms is common). A sub-second budget turns
// routine latency jitter into spurious circuit-breaker trips against
// otherwise-reachable targets.
func TestTargetDialTimeoutHasReasonableMargin(t *testing.T) {
	const minAcceptable = 3 * time.Second
	if targetDialTimeout < minAcceptable {
		t.Fatalf("targetDialTimeout = %s, want at least %s to tolerate a dropped SYN + retransmit on real-world routes",
			targetDialTimeout, minAcceptable)
	}
}
