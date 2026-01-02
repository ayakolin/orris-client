package status

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/procfs"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/version"
)

// partitionPattern matches partition names like sda1, nvme0n1p1, vda1, xvda1
var partitionPattern = regexp.MustCompile(`^(sd[a-z]+|vd[a-z]+|xvd[a-z]+|hd[a-z]+)\d+$|^nvme\d+n\d+p\d+$|^mmcblk\d+p\d+$`)

// Collector collects system status information.
type Collector struct {
	fs        procfs.FS
	startTime time.Time

	mu sync.Mutex

	// CPU usage calculation (requires two samples)
	lastCPUTime  time.Time
	lastCPUIdle  float64
	lastCPUTotal float64

	// Network rate calculation
	lastNetTime    time.Time
	lastNetRxBytes uint64
	lastNetTxBytes uint64

	// Disk I/O rate calculation
	lastDiskTime       time.Time
	lastDiskReadBytes  uint64
	lastDiskWriteBytes uint64
	lastDiskIOCount    uint64

	// Public IP cache
	publicIPv4     string
	publicIPv6     string
	lastIPFetch    time.Time
	ipFetchPending bool

	// CPU info cache (static, fetch once)
	cpuInfoCached bool
	cpuCores      int
	cpuModelName  string
	cpuMHz        float64

	// Kernel info cache (static, fetch once)
	kernelVersion string
	hostname      string
}

// NewCollector creates a new status collector.
func NewCollector() *Collector {
	fs, _ := procfs.NewDefaultFS()
	return &Collector{
		fs:        fs,
		startTime: time.Now(),
	}
}

// Collect gathers current system status.
func (c *Collector) Collect(_ context.Context) (*forward.AgentStatus, error) {
	status := &forward.AgentStatus{}

	// CPU usage and details
	c.collectCPU(status)
	c.collectCPUInfo(status)

	// Memory and swap
	c.collectMemory(status)

	// Disk usage and I/O
	c.collectDisk(status)
	c.collectDiskIO(status)

	// System load, uptime, and process stats
	c.collectSystemStats(status)

	// Pressure Stall Information
	c.collectPSI(status)

	// Network statistics
	c.collectNetworkStats(status)
	c.collectNetworkConnections(status)
	c.collectSocketStats(status)

	// File descriptors
	c.collectFileNr(status)

	// Virtual memory statistics
	c.collectVMStat(status)

	// Entropy
	c.collectEntropy(status)

	// Kernel info
	c.collectKernelInfo(status)

	// Agent info
	status.AgentVersion = version.Version
	status.Platform = version.Platform()
	status.Arch = version.Arch()

	// Public IP addresses (async fetch, use cached values)
	c.collectPublicIPs(status)

	return status, nil
}

// collectCPU calculates CPU usage percentage from /proc/stat.
func (c *Collector) collectCPU(status *forward.AgentStatus) {
	stat, err := c.fs.Stat()
	if err != nil {
		return
	}

	// Calculate total and idle CPU time
	cpu := stat.CPUTotal
	idle := cpu.Idle + cpu.Iowait
	total := cpu.User + cpu.Nice + cpu.System + cpu.Idle + cpu.Iowait + cpu.IRQ + cpu.SoftIRQ + cpu.Steal

	c.mu.Lock()
	defer c.mu.Unlock()

	// Need previous sample to calculate percentage
	if !c.lastCPUTime.IsZero() && total > c.lastCPUTotal {
		idleDelta := idle - c.lastCPUIdle
		totalDelta := total - c.lastCPUTotal
		if totalDelta > 0 {
			cpuPercent := 100.0 * (1.0 - idleDelta/totalDelta)
			// Clamp to valid range [0, 100]
			if cpuPercent < 0 {
				cpuPercent = 0
			} else if cpuPercent > 100 {
				cpuPercent = 100
			}
			status.CPUPercent = cpuPercent
		}
	}

	c.lastCPUTime = time.Now()
	c.lastCPUIdle = idle
	c.lastCPUTotal = total
}

// collectCPUInfo gets CPU details (cached after first call).
func (c *Collector) collectCPUInfo(status *forward.AgentStatus) {
	c.mu.Lock()
	if c.cpuInfoCached {
		status.CPUCores = c.cpuCores
		status.CPUModelName = c.cpuModelName
		status.CPUMHz = c.cpuMHz
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	// Read /proc/cpuinfo directly
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return
	}

	var cores int
	var modelName string
	var mhz float64

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		fields := strings.SplitN(line, ":", 2)
		if len(fields) != 2 {
			continue
		}
		key := strings.TrimSpace(fields[0])
		value := strings.TrimSpace(fields[1])

		switch key {
		case "processor":
			cores++
		case "model name":
			if modelName == "" {
				modelName = value
			}
		case "cpu MHz":
			if mhz == 0 {
				fmt.Sscanf(value, "%f", &mhz)
			}
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.cpuCores = cores
	c.cpuModelName = modelName
	c.cpuMHz = mhz
	c.cpuInfoCached = true

	status.CPUCores = c.cpuCores
	status.CPUModelName = c.cpuModelName
	status.CPUMHz = c.cpuMHz
}

// collectMemory reads memory and swap info from /proc/meminfo.
func (c *Collector) collectMemory(status *forward.AgentStatus) {
	meminfo, err := c.fs.Meminfo()
	if err != nil {
		return
	}

	total := ptrToUint64(meminfo.MemTotal)
	free := ptrToUint64(meminfo.MemFree)
	buffers := ptrToUint64(meminfo.Buffers)
	cached := ptrToUint64(meminfo.Cached)
	sReclaimable := ptrToUint64(meminfo.SReclaimable)

	// Available memory
	available := ptrToUint64(meminfo.MemAvailable)
	if available == 0 {
		available = free + buffers + cached + sReclaimable
	}

	// Prevent underflow if available > total (shouldn't happen but be safe)
	var used uint64
	if total > available {
		used = total - available
	}
	if total > 0 {
		status.MemoryPercent = 100.0 * float64(used) / float64(total)
	}

	// Convert from KB to bytes
	status.MemoryTotal = total * 1024
	status.MemoryUsed = used * 1024
	status.MemoryAvail = available * 1024

	// Swap
	swapTotal := ptrToUint64(meminfo.SwapTotal)
	swapFree := ptrToUint64(meminfo.SwapFree)
	// Prevent underflow
	var swapUsed uint64
	if swapTotal > swapFree {
		swapUsed = swapTotal - swapFree
	}

	status.SwapTotal = swapTotal * 1024
	status.SwapUsed = swapUsed * 1024
	if swapTotal > 0 {
		status.SwapPercent = 100.0 * float64(swapUsed) / float64(swapTotal)
	}
}

// collectDisk reads disk usage for root partition.
func (c *Collector) collectDisk(status *forward.AgentStatus) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs("/", &stat); err != nil {
		return
	}

	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bfree * uint64(stat.Bsize)
	// Prevent underflow
	var used uint64
	if total > free {
		used = total - free
	}

	status.DiskTotal = total
	status.DiskUsed = used
	if total > 0 {
		status.DiskPercent = 100.0 * float64(used) / float64(total)
	}
}

// collectDiskIO reads disk I/O statistics from /proc/diskstats.
func (c *Collector) collectDiskIO(status *forward.AgentStatus) {
	file, err := os.Open("/proc/diskstats")
	if err != nil {
		return
	}
	defer file.Close()

	// Sum all disk I/O (excluding partitions, only whole disks)
	var totalRead, totalWrite, totalIOCount uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 14 {
			continue
		}

		name := fields[2]
		// Skip partitions (sda1, nvme0n1p1, vda1, etc.) and loop devices
		if partitionPattern.MatchString(name) || strings.HasPrefix(name, "loop") {
			continue
		}

		// Fields: reads_completed, sectors_read, writes_completed, sectors_written
		// Sector size is typically 512 bytes
		readsCompleted, _ := strconv.ParseUint(fields[3], 10, 64)
		sectorsRead, _ := strconv.ParseUint(fields[5], 10, 64)
		writesCompleted, _ := strconv.ParseUint(fields[7], 10, 64)
		sectorsWritten, _ := strconv.ParseUint(fields[9], 10, 64)

		totalRead += sectorsRead * 512
		totalWrite += sectorsWritten * 512
		totalIOCount += readsCompleted + writesCompleted
	}

	status.DiskReadBytes = totalRead
	status.DiskWriteBytes = totalWrite

	// Calculate rate
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if !c.lastDiskTime.IsZero() {
		elapsed := now.Sub(c.lastDiskTime).Seconds()
		// Check all counters to prevent underflow
		if elapsed > 0 && totalRead >= c.lastDiskReadBytes && totalWrite >= c.lastDiskWriteBytes && totalIOCount >= c.lastDiskIOCount {
			status.DiskReadRate = uint64(float64(totalRead-c.lastDiskReadBytes) / elapsed)
			status.DiskWriteRate = uint64(float64(totalWrite-c.lastDiskWriteBytes) / elapsed)
			status.DiskIOPS = uint64(float64(totalIOCount-c.lastDiskIOCount) / elapsed)
		}
	}

	c.lastDiskTime = now
	c.lastDiskReadBytes = totalRead
	c.lastDiskWriteBytes = totalWrite
	c.lastDiskIOCount = totalIOCount
}

// collectSystemStats reads load average, uptime, and process stats from /proc.
func (c *Collector) collectSystemStats(status *forward.AgentStatus) {
	// Load average
	loadavg, err := c.fs.LoadAvg()
	if err == nil {
		status.LoadAvg1 = loadavg.Load1
		status.LoadAvg5 = loadavg.Load5
		status.LoadAvg15 = loadavg.Load15
	}

	// Uptime, process stats, context switches, interrupts
	stat, err := c.fs.Stat()
	if err == nil {
		status.UptimeSeconds = time.Now().Unix() - int64(stat.BootTime)
		status.ProcessesTotal = stat.ProcessCreated
		status.ProcessesRunning = stat.ProcessesRunning
		status.ProcessesBlocked = stat.ProcessesBlocked
		status.ContextSwitches = stat.ContextSwitches
		status.Interrupts = stat.IRQTotal
	}
}

// collectPSI reads Pressure Stall Information from /proc/pressure.
func (c *Collector) collectPSI(status *forward.AgentStatus) {
	// CPU pressure (note: CPU only has "some", not "full")
	if cpuPSI, err := c.fs.PSIStatsForResource("cpu"); err == nil {
		if cpuPSI.Some != nil {
			status.PSICPUSome = cpuPSI.Some.Avg10
		}
		// CPU PSI does not have "full" metric, it's always nil
	}

	// Memory pressure
	if memPSI, err := c.fs.PSIStatsForResource("memory"); err == nil {
		if memPSI.Some != nil {
			status.PSIMemorySome = memPSI.Some.Avg10
		}
		if memPSI.Full != nil {
			status.PSIMemoryFull = memPSI.Full.Avg10
		}
	}

	// I/O pressure
	if ioPSI, err := c.fs.PSIStatsForResource("io"); err == nil {
		if ioPSI.Some != nil {
			status.PSIIOSome = ioPSI.Some.Avg10
		}
		if ioPSI.Full != nil {
			status.PSIIOFull = ioPSI.Full.Avg10
		}
	}
}

// collectNetworkStats reads network I/O from /proc/net/dev.
func (c *Collector) collectNetworkStats(status *forward.AgentStatus) {
	netDev, err := c.fs.NetDev()
	if err != nil {
		return
	}

	// Sum all interfaces (excluding loopback)
	var totalRx, totalTx uint64
	var totalRxPackets, totalTxPackets uint64
	var totalRxErrors, totalTxErrors uint64
	var totalRxDropped, totalTxDropped uint64

	for name, dev := range netDev {
		if name == "lo" {
			continue
		}
		totalRx += dev.RxBytes
		totalTx += dev.TxBytes
		totalRxPackets += dev.RxPackets
		totalTxPackets += dev.TxPackets
		totalRxErrors += dev.RxErrors
		totalTxErrors += dev.TxErrors
		totalRxDropped += dev.RxDropped
		totalTxDropped += dev.TxDropped
	}

	status.NetworkRxBytes = totalRx
	status.NetworkTxBytes = totalTx
	status.NetworkRxPackets = totalRxPackets
	status.NetworkTxPackets = totalTxPackets
	status.NetworkRxErrors = totalRxErrors
	status.NetworkTxErrors = totalTxErrors
	status.NetworkRxDropped = totalRxDropped
	status.NetworkTxDropped = totalTxDropped

	// Calculate rate
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if !c.lastNetTime.IsZero() {
		elapsed := now.Sub(c.lastNetTime).Seconds()
		if elapsed > 0 && totalRx >= c.lastNetRxBytes && totalTx >= c.lastNetTxBytes {
			status.NetworkRxRate = uint64(float64(totalRx-c.lastNetRxBytes) / elapsed)
			status.NetworkTxRate = uint64(float64(totalTx-c.lastNetTxBytes) / elapsed)
		}
	}

	c.lastNetTime = now
	c.lastNetRxBytes = totalRx
	c.lastNetTxBytes = totalTx
}

// collectNetworkConnections counts TCP and UDP connections from /proc/net.
func (c *Collector) collectNetworkConnections(status *forward.AgentStatus) {
	// Count TCP connections (IPv4 + IPv6)
	if tcp4, err := c.fs.NetTCP(); err == nil {
		status.TCPConnections += len(tcp4)
	}
	if tcp6, err := c.fs.NetTCP6(); err == nil {
		status.TCPConnections += len(tcp6)
	}

	// Count UDP connections (IPv4 + IPv6)
	if udp4, err := c.fs.NetUDP(); err == nil {
		status.UDPConnections += len(udp4)
	}
	if udp6, err := c.fs.NetUDP6(); err == nil {
		status.UDPConnections += len(udp6)
	}
}

// collectSocketStats reads socket statistics from /proc/net/sockstat.
func (c *Collector) collectSocketStats(status *forward.AgentStatus) {
	sockstat, err := c.fs.NetSockstat()
	if err != nil {
		return
	}

	if sockstat.Used != nil {
		status.SocketsUsed = *sockstat.Used
	}

	for _, proto := range sockstat.Protocols {
		switch proto.Protocol {
		case "TCP":
			status.SocketsTCPInUse = proto.InUse
			if proto.Orphan != nil {
				status.SocketsTCPOrphan = *proto.Orphan
			}
			if proto.TW != nil {
				status.SocketsTCPTW = *proto.TW
			}
		case "UDP":
			status.SocketsUDPInUse = proto.InUse
		}
	}
}

// collectFileNr reads file descriptor usage from /proc/sys/fs/file-nr.
func (c *Collector) collectFileNr(status *forward.AgentStatus) {
	data, err := os.ReadFile("/proc/sys/fs/file-nr")
	if err != nil {
		return
	}

	fields := strings.Fields(string(data))
	if len(fields) >= 3 {
		status.FileNrAllocated, _ = strconv.ParseUint(fields[0], 10, 64)
		status.FileNrMax, _ = strconv.ParseUint(fields[2], 10, 64)
	}
}

// collectVMStat reads virtual memory statistics from /proc/vmstat.
func (c *Collector) collectVMStat(status *forward.AgentStatus) {
	file, err := os.Open("/proc/vmstat")
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 {
			continue
		}

		value, _ := strconv.ParseUint(fields[1], 10, 64)
		switch fields[0] {
		case "pgpgin":
			status.VMPageIn = value
		case "pgpgout":
			status.VMPageOut = value
		case "pswpin":
			status.VMSwapIn = value
		case "pswpout":
			status.VMSwapOut = value
		case "oom_kill":
			status.VMOOMKill = value
		}
	}
}

// collectEntropy reads available entropy from /proc/sys/kernel/random/entropy_avail.
func (c *Collector) collectEntropy(status *forward.AgentStatus) {
	random, err := c.fs.KernelRandom()
	if err != nil {
		return
	}

	// Note: procfs has a typo in field name (EntropyAvaliable instead of EntropyAvailable)
	if random.EntropyAvaliable != nil {
		status.EntropyAvailable = *random.EntropyAvaliable
	}
}

// collectKernelInfo gets kernel version and hostname (cached after first call).
func (c *Collector) collectKernelInfo(status *forward.AgentStatus) {
	c.mu.Lock()
	if c.kernelVersion != "" {
		status.KernelVersion = c.kernelVersion
		status.Hostname = c.hostname
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	// Read kernel version from /proc/version
	if data, err := os.ReadFile("/proc/version"); err == nil {
		parts := strings.Fields(string(data))
		if len(parts) >= 3 {
			c.mu.Lock()
			c.kernelVersion = parts[2]
			c.mu.Unlock()
		}
	}

	// Get hostname
	if hostname, err := os.Hostname(); err == nil {
		c.mu.Lock()
		c.hostname = hostname
		c.mu.Unlock()
	}

	c.mu.Lock()
	status.KernelVersion = c.kernelVersion
	status.Hostname = c.hostname
	c.mu.Unlock()
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

const (
	ipFetchInterval = 5 * time.Minute
	ipFetchTimeout  = 3 * time.Second
)

// collectPublicIPs fetches and caches public IP addresses.
func (c *Collector) collectPublicIPs(status *forward.AgentStatus) {
	c.mu.Lock()
	// Use cached values
	status.PublicIPv4 = c.publicIPv4
	status.PublicIPv6 = c.publicIPv6

	// Check if we need to refresh
	needFetch := time.Since(c.lastIPFetch) > ipFetchInterval && !c.ipFetchPending
	if needFetch {
		c.ipFetchPending = true
	}
	c.mu.Unlock()

	if !needFetch {
		return
	}

	// Fetch in background to avoid blocking
	go c.fetchPublicIPs()
}

// fetchPublicIPs fetches public IPv4 and IPv6 addresses from external services.
func (c *Collector) fetchPublicIPs() {
	defer func() {
		c.mu.Lock()
		c.ipFetchPending = false
		c.lastIPFetch = time.Now()
		c.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), ipFetchTimeout)
	defer cancel()

	// Fetch IPv4 and IPv6 in parallel
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if ip := fetchIP(ctx, "https://api.ipify.org"); ip != "" {
			c.mu.Lock()
			c.publicIPv4 = ip
			c.mu.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		if ip := fetchIP(ctx, "https://api6.ipify.org"); ip != "" {
			c.mu.Lock()
			c.publicIPv6 = ip
			c.mu.Unlock()
		}
	}()

	wg.Wait()
}

// fetchIP fetches IP address from the given URL.
func fetchIP(ctx context.Context, url string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ""
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ""
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(body))
}

// ptrToUint64 safely dereferences a *uint64 pointer, returning 0 if nil.
func ptrToUint64(p *uint64) uint64 {
	if p == nil {
		return 0
	}
	return *p
}
