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
// These values are optimized for low-latency tunnel forwarding.
func DefaultSmuxConfig() *SmuxConfig {
	return &SmuxConfig{
		Version:           2,
		KeepAliveInterval: 10 * time.Second,
		KeepAliveTimeout:  30 * time.Second,
		MaxFrameSize:      32 * 1024,       // 32KB
		MaxReceiveBuffer:  4 * 1024 * 1024, // 4MB session buffer
		MaxStreamBuffer:   256 * 1024,      // 256KB stream buffer
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
