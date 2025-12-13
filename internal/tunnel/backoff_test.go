package tunnel

import (
	"testing"
	"time"
)

func TestBackoff_Next(t *testing.T) {
	b := &Backoff{
		Initial:    1 * time.Second,
		Max:        60 * time.Second,
		Multiplier: 2.0,
		Jitter:     0, // disable jitter for predictable testing
	}

	expected := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		60 * time.Second, // capped at max
		60 * time.Second, // stays at max
	}

	for i, want := range expected {
		got := b.Next()
		if got != want {
			t.Errorf("attempt %d: got %v, want %v", i+1, got, want)
		}
	}
}

func TestBackoff_Reset(t *testing.T) {
	b := &Backoff{
		Initial:    1 * time.Second,
		Max:        60 * time.Second,
		Multiplier: 2.0,
		Jitter:     0,
	}

	// Advance a few attempts
	b.Next() // 1s
	b.Next() // 2s
	b.Next() // 4s

	if b.Attempt() != 3 {
		t.Errorf("attempt should be 3, got %d", b.Attempt())
	}

	// Reset and verify
	b.Reset()

	if b.Attempt() != 0 {
		t.Errorf("attempt should be 0 after reset, got %d", b.Attempt())
	}

	// Should start from initial again
	got := b.Next()
	if got != 1*time.Second {
		t.Errorf("after reset, got %v, want %v", got, 1*time.Second)
	}
}

func TestBackoff_Jitter(t *testing.T) {
	b := &Backoff{
		Initial:    10 * time.Second,
		Max:        60 * time.Second,
		Multiplier: 2.0,
		Jitter:     0.2, // ±20%
	}

	// Run multiple times to verify jitter is applied
	min := 8 * time.Second  // 10s - 20%
	max := 12 * time.Second // 10s + 20%

	for i := 0; i < 100; i++ {
		b.Reset()
		got := b.Next()
		if got < min || got > max {
			t.Errorf("iteration %d: got %v, want between %v and %v", i, got, min, max)
		}
	}
}

func TestDefaultBackoff(t *testing.T) {
	b := DefaultBackoff()

	if b.Initial != 1*time.Second {
		t.Errorf("Initial: got %v, want %v", b.Initial, 1*time.Second)
	}
	if b.Max != 60*time.Second {
		t.Errorf("Max: got %v, want %v", b.Max, 60*time.Second)
	}
	if b.Multiplier != 2.0 {
		t.Errorf("Multiplier: got %v, want %v", b.Multiplier, 2.0)
	}
	if b.Jitter != 0.2 {
		t.Errorf("Jitter: got %v, want %v", b.Jitter, 0.2)
	}
}
