package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/orris-inc/orris-client/internal/forward"

	"github.com/orris-inc/orris-client/internal/api"
	"github.com/orris-inc/orris-client/internal/config"
	"github.com/orris-inc/orris-client/internal/forwarder"
	"github.com/orris-inc/orris-client/internal/status"
	"github.com/orris-inc/orris-client/internal/tunnel"
)

type Agent struct {
	cfg       *config.Config
	client    *api.Client
	collector *status.Collector

	// Lock ordering to prevent deadlocks: rulesMu -> forwardersMu -> tunnelsMu
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
	//   ✓ Correct: rulesMu.Lock() -> forwardersMu.Lock() -> tunnelsMu.Lock()
	//   ✗ Wrong:   forwardersMu.Lock() -> rulesMu.Lock() (violates ordering)
	//   ✗ Wrong:   tunnelsMu.Lock() -> forwardersMu.Lock() (violates ordering)

	rulesMu     sync.RWMutex
	rules       []forward.Rule
	clientToken string // agent's own token for tunnel handshake (from API response)

	forwardersMu sync.RWMutex
	forwarders   map[string]forwarder.Forwarder

	tunnelsMu sync.RWMutex
	tunnels   map[string]tunnel.TunnelClient // ruleID -> tunnel (WS or TLS)

	tunnelServerMu    sync.Mutex // protects tunnelServer initialization
	tunnelServer      *tunnel.Server
	tlsTunnelServerMu sync.Mutex // protects tlsTunnelServer initialization
	tlsTunnelServer   *tunnel.TLSServer
	configVersion     uint64 // current config version from server

	ctx      context.Context
	cancelFn context.CancelFunc
	wg       sync.WaitGroup
}

func New(cfg *config.Config) *Agent {
	client := api.NewClient(cfg.ServerURL, cfg.Token, cfg.HTTPTimeout)

	return &Agent{
		cfg:        cfg,
		client:     client,
		collector:  status.NewCollector(),
		forwarders: make(map[string]forwarder.Forwarder),
		tunnels:    make(map[string]tunnel.TunnelClient),
	}
}

func (a *Agent) Start(ctx context.Context) error {
	a.ctx, a.cancelFn = context.WithCancel(ctx)

	if err := a.syncRules(); err != nil {
		return fmt.Errorf("initial sync failed: %w", err)
	}

	a.wg.Add(4)
	go a.syncLoop()
	go a.trafficLoop()
	go a.statusLoop()
	go a.hubLoop()

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
