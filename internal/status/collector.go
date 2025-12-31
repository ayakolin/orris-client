package status

import (
	"context"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/version"
)

// Collector collects system status information.
type Collector struct {
	startTime time.Time
}

// NewCollector creates a new status collector.
func NewCollector() *Collector {
	return &Collector{
		startTime: time.Now(),
	}
}

// Collect gathers current system status.
func (c *Collector) Collect(ctx context.Context) (*forward.AgentStatus, error) {
	status := &forward.AgentStatus{}

	// CPU usage
	cpuPercent, err := cpu.PercentWithContext(ctx, 0, false)
	if err == nil && len(cpuPercent) > 0 {
		status.CPUPercent = cpuPercent[0]
	}

	// Memory usage
	memInfo, err := mem.VirtualMemoryWithContext(ctx)
	if err == nil {
		status.MemoryPercent = memInfo.UsedPercent
		status.MemoryUsed = memInfo.Used
		status.MemoryTotal = memInfo.Total
	}

	// Disk usage (root partition)
	diskInfo, err := disk.UsageWithContext(ctx, "/")
	if err == nil {
		status.DiskPercent = diskInfo.UsedPercent
		status.DiskUsed = diskInfo.Used
		status.DiskTotal = diskInfo.Total
	}

	// Uptime
	bootTime, err := host.BootTimeWithContext(ctx)
	if err == nil {
		status.UptimeSeconds = int64(time.Now().Unix() - int64(bootTime))
	}

	// Network connections
	conns, err := net.ConnectionsWithContext(ctx, "all")
	if err == nil {
		for _, conn := range conns {
			switch conn.Type {
			case 1: // SOCK_STREAM (TCP)
				status.TCPConnections++
			case 2: // SOCK_DGRAM (UDP)
				status.UDPConnections++
			}
		}
	}

	// Agent info
	status.AgentVersion = version.Version
	status.Platform = version.Platform()
	status.Arch = version.Arch()

	return status, nil
}

// SetActiveStats sets the active rules and connections count.
func (c *Collector) SetActiveStats(status *forward.AgentStatus, activeRules, activeConns int) {
	status.ActiveRules = activeRules
	status.ActiveConnections = activeConns
}

// SetTunnelStatus sets the tunnel connection states.
func (c *Collector) SetTunnelStatus(status *forward.AgentStatus, tunnelStatus map[string]forward.TunnelState) {
	status.TunnelStatus = tunnelStatus
}
