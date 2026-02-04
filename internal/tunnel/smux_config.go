package tunnel

import (
	"time"

	"github.com/xtaci/smux"
)

// SmuxConfig holds SMUX configuration parameters.
type SmuxConfig struct {
	Version           int           // SMUX version (1 or 2)
	KeepAliveInterval time.Duration // Interval for sending keep-alive packets
	KeepAliveTimeout  time.Duration // Timeout for keep-alive response
	MaxFrameSize      int           // Maximum frame size
	MaxReceiveBuffer  int           // Maximum receive buffer per session
	MaxStreamBuffer   int           // Maximum receive buffer per stream
}

// DefaultSmuxConfig returns the default SMUX configuration.
// These values are optimized for high-throughput tunnel forwarding.
// Buffer sizes are critical for throughput: throughput ≈ buffer_size / RTT
// With 2MB stream buffer and 5ms RTT, theoretical max is ~3.2 Gbps.
func DefaultSmuxConfig() *SmuxConfig {
	return &SmuxConfig{
		Version:           2,
		KeepAliveInterval: 10 * time.Second,
		KeepAliveTimeout:  30 * time.Second,
		MaxFrameSize:      65535,            // max allowed by smux (must be <= 65535)
		MaxReceiveBuffer:  32 * 1024 * 1024, // 32MB session buffer
		MaxStreamBuffer:   2 * 1024 * 1024,  // 2MB stream buffer
	}
}

// ToSmuxConfig converts SmuxConfig to smux.Config.
func (c *SmuxConfig) ToSmuxConfig() *smux.Config {
	return &smux.Config{
		Version:           c.Version,
		KeepAliveInterval: c.KeepAliveInterval,
		KeepAliveTimeout:  c.KeepAliveTimeout,
		MaxFrameSize:      c.MaxFrameSize,
		MaxReceiveBuffer:  c.MaxReceiveBuffer,
		MaxStreamBuffer:   c.MaxStreamBuffer,
	}
}
