package agent

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/orris-inc/orris-client/internal/api"
	"github.com/orris-inc/orris-client/internal/config"
	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/forwarder"
	"github.com/orris-inc/orris-client/internal/logger"
	"github.com/orris-inc/orris-client/internal/status"
	"github.com/orris-inc/orris-client/internal/tunnel"
)

// ruleStatus tracks sync and runtime status of a rule.
type ruleStatus struct {
	syncStatus   string // synced, pending, failed
	runStatus    string // running, stopped, error, starting
	errorMessage string
	syncedAt     int64
}

type Agent struct {
	cfg       *config.Config
	client    *api.Client
	collector *status.Collector

	// Lock ordering to prevent deadlocks: rulesMu -> forwardersMu -> ruleStatusMu -> tunnelsMu
	// CRITICAL: Always acquire locks in this order when multiple locks are needed.
	// Never acquire locks in reverse order to avoid circular wait conditions.
	//
	// Lock acquisition rules:
	// 1. Single lock: Can acquire any lock independently
	// 2. Multiple locks: MUST follow the ordering above
	// 3. Keep critical sections as small as possible
	// 4. Release locks as soon as possible to reduce contention
	//
	// Examples:
	//   ✓ Correct: forwardersMu.Lock() -> ruleStatusMu.Lock()
	//   ✗ Wrong:   ruleStatusMu.Lock() -> forwardersMu.Lock() (violates ordering)

	rulesMu          sync.RWMutex
	rules            []forward.Rule
	clientToken      string   // agent's own token for tunnel handshake (from API response)
	blockedProtocols []string // agent-level blocked protocols (from full sync)

	forwardersMu sync.RWMutex
	forwarders   map[string]forwarder.Forwarder

	tunnelsMu sync.RWMutex
	tunnels   map[string]tunnel.TunnelClient // ruleID -> tunnel (WS or TLS)

	// Cache of resolved exit agent endpoints. Used as a fallback when the
	// control server is unreachable and a new tunnel needs to be established
	// (see getExitEndpoint).
	endpointCacheMu sync.RWMutex
	endpointCache   map[string]forward.ExitEndpoint // agentID -> last resolved endpoint

	// Health check configurations for load balancing failover
	healthCheckMu      sync.RWMutex
	healthCheckConfigs map[string]*forward.HealthCheckConfig // ruleID -> config

	// Rule status tracking
	ruleStatusMu sync.RWMutex
	ruleStatus   map[string]*ruleStatus // ruleID -> status

	tunnelServerMu    sync.Mutex // protects tunnelServer initialization
	tunnelServer      *tunnel.Server
	tlsTunnelServerMu sync.Mutex // protects tlsTunnelServer initialization
	tlsTunnelServer   *tunnel.TLSServer

	// Note: SMUX support is integrated into tunnelServer and tlsTunnelServer.
	// They now support both message-based protocol and SMUX multiplexing on the same port,
	// distinguished by EnableSmux field in the handshake message.

	configVersion uint64 // current config version from server

	hubConnMu sync.RWMutex     // protects hubConn access
	hubConn   *forward.HubConn // current hub WebSocket connection

	// Pending traffic for retry on failure
	pendingTrafficMu sync.Mutex
	pendingTraffic   []forward.TrafficItem

	// Debounce channel for rule status reporting
	// When a status change occurs, a signal is sent to this channel
	// A single goroutine drains and coalesces multiple updates
	ruleStatusReportCh chan struct{}

	ctx      context.Context
	cancelFn context.CancelFunc
	wg       sync.WaitGroup
}

func New(cfg *config.Config) *Agent {
	client := api.NewClient(cfg.ServerURL, cfg.Token, cfg.HTTPTimeout)

	return &Agent{
		cfg:                cfg,
		client:             client,
		collector:          status.NewCollector(),
		forwarders:         make(map[string]forwarder.Forwarder),
		tunnels:            make(map[string]tunnel.TunnelClient),
		endpointCache:      make(map[string]forward.ExitEndpoint),
		healthCheckConfigs: make(map[string]*forward.HealthCheckConfig),
		ruleStatus:         make(map[string]*ruleStatus),
		ruleStatusReportCh: make(chan struct{}, 1), // buffered to avoid blocking
	}
}

func (a *Agent) Start(ctx context.Context) error {
	a.ctx, a.cancelFn = context.WithCancel(ctx)

	if err := a.syncRules(); err != nil {
		return fmt.Errorf("initial sync failed: %w", err)
	}

	a.wg.Add(5)
	go a.syncLoop()
	go a.trafficLoop()
	go a.statusLoop()
	go a.hubLoop()
	go a.ruleStatusReportLoop()

	return nil
}

func (a *Agent) Stop() {
	if a.cancelFn != nil {
		a.cancelFn()
	}

	a.reportFinalTraffic()
	a.stopAll()
	a.wg.Wait()
}

func (a *Agent) Wait() {
	a.wg.Wait()
}

// updateBlockedProtocols updates blocked protocols and stops affected forwarders.
func (a *Agent) updateBlockedProtocols(protocols []string) {
	a.rulesMu.Lock()
	oldProtocols := a.blockedProtocols
	a.blockedProtocols = protocols
	a.rulesMu.Unlock()

	// Log if changed
	if len(protocols) > 0 || len(oldProtocols) > 0 {
		logger.Info("blocked protocols updated", "protocols", protocols)
	}

	// Stop forwarders using newly blocked protocols
	if len(protocols) > 0 {
		a.stopBlockedProtocolForwarders()
	}
}

// stopBlockedProtocolForwarders stops all forwarders using blocked protocols.
// Lock order: rulesMu -> forwardersMu -> ruleStatusMu
func (a *Agent) stopBlockedProtocolForwarders() {
	// First, collect rules and blocked protocols under rulesMu
	a.rulesMu.RLock()
	rules := make(map[string]forward.Rule)
	for _, r := range a.rules {
		rules[r.ID] = r
	}
	blockedProtocols := a.blockedProtocols
	a.rulesMu.RUnlock()

	// Helper to check if protocol is blocked (no lock needed, uses local copy)
	isBlocked := func(protocol string) bool {
		if protocol == "" {
			protocol = "tcp"
		}
		for _, blocked := range blockedProtocols {
			if strings.EqualFold(blocked, protocol) {
				return true
			}
		}
		return false
	}

	// Then, stop forwarders under forwardersMu
	a.forwardersMu.Lock()
	defer a.forwardersMu.Unlock()

	for ruleID, f := range a.forwarders {
		rule, exists := rules[ruleID]
		if !exists {
			continue
		}
		if isBlocked(rule.Protocol) {
			logger.Info("stopping forwarder for blocked protocol", "rule_id", ruleID, "protocol", rule.Protocol)
			f.Stop()
			delete(a.forwarders, ruleID)
			a.setRuleStatus(ruleID, forward.SyncStatusFailed, forward.RunStatusError, "protocol blocked")
		}
	}
}

// isProtocolBlocked checks if the given protocol is in the blocked list.
// Empty protocol defaults to "tcp" for comparison.
// Comparison is case-insensitive.
func (a *Agent) isProtocolBlocked(protocol string) bool {
	a.rulesMu.RLock()
	defer a.rulesMu.RUnlock()
	return a.isProtocolBlockedUnsafe(protocol)
}

// isProtocolBlockedUnsafe checks if the protocol is blocked without locking.
// Caller must hold rulesMu.
func (a *Agent) isProtocolBlockedUnsafe(protocol string) bool {
	if protocol == "" {
		protocol = "tcp"
	}
	for _, blocked := range a.blockedProtocols {
		if strings.EqualFold(blocked, protocol) {
			return true
		}
	}
	return false
}

// saveHealthCheckConfig saves the health check config for a rule.
func (a *Agent) saveHealthCheckConfig(ruleID string, config *forward.HealthCheckConfig) {
	if config == nil {
		return
	}
	a.healthCheckMu.Lock()
	a.healthCheckConfigs[ruleID] = config
	a.healthCheckMu.Unlock()
}

// getHealthCheckConfig returns the health check config for a rule.
func (a *Agent) getHealthCheckConfig(ruleID string) *forward.HealthCheckConfig {
	a.healthCheckMu.RLock()
	defer a.healthCheckMu.RUnlock()
	return a.healthCheckConfigs[ruleID]
}

// deleteHealthCheckConfig removes the health check config for a rule.
func (a *Agent) deleteHealthCheckConfig(ruleID string) {
	a.healthCheckMu.Lock()
	delete(a.healthCheckConfigs, ruleID)
	a.healthCheckMu.Unlock()
}
