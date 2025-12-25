package agent

import (
	"fmt"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/forwarder"
	"github.com/orris-inc/orris-client/internal/logger"
	"github.com/orris-inc/orris-client/internal/tunnel"
)

// syncLoop is a fallback mechanism for rule synchronization.
// Primary sync is done via WebSocket events from hub, but this loop ensures
// rules are eventually consistent even if WebSocket connection is unstable.
func (a *Agent) syncLoop() {
	defer a.wg.Done()

	// Use a longer interval since primary sync is via WebSocket events
	fallbackInterval := a.cfg.SyncInterval * 10 // 5 minutes by default
	if fallbackInterval < 5*time.Minute {
		fallbackInterval = 5 * time.Minute
	}

	ticker := time.NewTicker(fallbackInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			if err := a.syncRules(); err != nil {
				logger.Error("fallback sync rules failed", "error", err)
			}
		}
	}
}

func (a *Agent) syncRules() error {
	logger.Info("requesting enabled rules")
	resp, err := a.client.GetRules(a.ctx)
	if err != nil {
		return err
	}

	rules := resp.Rules
	logger.Info("rules synced successfully", "count", len(rules))

	// Save client token and rules for handshake verification
	a.rulesMu.Lock()
	if resp.ClientToken != "" {
		a.clientToken = resp.ClientToken
		logger.Debug("clientToken synced from API", "token_prefix", tokenPrefix(resp.ClientToken))
	} else {
		logger.Debug("API returned empty clientToken")
	}
	a.rules = rules
	a.rulesMu.Unlock()

	// Update tunnel servers' rules if they exist
	if a.tunnelServer != nil {
		a.tunnelServer.UpdateRules(rules)
	}
	if a.tlsTunnelServer != nil {
		a.tlsTunnelServer.UpdateRules(rules)
	}

	ruleMap := make(map[string]*forward.Rule)
	for i := range rules {
		ruleMap[rules[i].ID] = &rules[i]
	}

	// Stop forwarders for removed rules
	a.forwardersMu.Lock()
	for ruleID, f := range a.forwarders {
		if _, exists := ruleMap[ruleID]; !exists {
			logger.Info("stopping forwarder for removed rule", "rule_id", ruleID)
			f.Stop()
			delete(a.forwarders, ruleID)
		}
	}
	a.forwardersMu.Unlock()

	// Start forwarders for new rules
	for _, rule := range rules {
		a.forwardersMu.RLock()
		_, exists := a.forwarders[rule.ID]
		a.forwardersMu.RUnlock()

		if !exists {
			r := rule
			if err := a.startForwarder(&r); err != nil {
				logger.Error("start forwarder failed", "rule_id", rule.ID, "error", err)
			}
		}
	}

	return nil
}

func (a *Agent) startForwarder(rule *forward.Rule) error {
	var f forwarder.Forwarder

	switch rule.RuleType {
	case forward.RuleTypeDirect:
		df := forwarder.NewDirectForwarder(rule)
		if err := df.Start(a.ctx); err != nil {
			return err
		}
		f = df

	case forward.RuleTypeEntry:
		// Handle based on agent's role in this rule
		switch rule.Role {
		case "entry":
			// Entry role: establish tunnel to exit agent
			var t tunnel.TunnelClient
			var err error

			// If NextHopAddress is already provided, use it directly
			// Otherwise, query endpoint via GetExitEndpoint if NextHopAgentID is set
			if rule.NextHopAddress != "" {
				t, err = a.getOrCreateTunnelByAddress(rule)
			} else if rule.NextHopAgentID != "" {
				t, err = a.getOrCreateTunnel(rule)
			} else {
				return fmt.Errorf("entry rule missing next hop info")
			}
			if err != nil {
				return fmt.Errorf("create tunnel: %w", err)
			}

			ef := forwarder.NewEntryForwarder(rule, t)
			t.SetHandler(ef)
			if err := ef.Start(a.ctx); err != nil {
				// Cleanup: stop tunnel on forwarder start failure
				if stopErr := t.Stop(); stopErr != nil {
					logger.Error("failed to stop tunnel after forwarder start failure", "rule_id", rule.ID, "error", stopErr)
				}
				return err
			}
			f = ef

		case "exit":
			// Exit role: accept tunnel connections and forward to target
			// Start the appropriate tunnel server based on TunnelType
			if err := a.ensureTunnelServerByType(rule.TunnelType); err != nil {
				return err
			}

			ef := forwarder.NewExitForwarder(rule)
			server := a.getTunnelServer(rule.TunnelType)
			server.AddHandler(rule.ID, ef)
			if err := ef.Start(a.ctx); err != nil {
				// Cleanup: remove handler on forwarder start failure
				server.RemoveHandler(rule.ID)
				return err
			}
			f = ef

		default:
			return fmt.Errorf("unknown role %q for entry rule", rule.Role)
		}

	case forward.RuleTypeChain:
		// Handle chain rule based on agent's role
		switch rule.Role {
		case "entry":
			// Chain entry: connect to next hop
			t, err := a.getOrCreateTunnelByAddress(rule)
			if err != nil {
				return fmt.Errorf("create tunnel to next hop: %w", err)
			}

			ef := forwarder.NewEntryForwarder(rule, t)
			t.SetHandler(ef)
			if err := ef.Start(a.ctx); err != nil {
				// Cleanup: stop tunnel on forwarder start failure
				if stopErr := t.Stop(); stopErr != nil {
					logger.Error("failed to stop tunnel after forwarder start failure", "rule_id", rule.ID, "error", stopErr)
				}
				return err
			}
			f = ef

		case "relay":
			// Chain relay: accept from previous hop, forward to next hop
			// Start the appropriate tunnel server based on TunnelType
			if err := a.ensureTunnelServerByType(rule.TunnelType); err != nil {
				return err
			}

			// Connect to next hop
			t, err := a.getOrCreateTunnelByAddress(rule)
			if err != nil {
				return fmt.Errorf("create tunnel to next hop: %w", err)
			}

			rf := forwarder.NewRelayForwarder(rule, t)
			server := a.getTunnelServer(rule.TunnelType)
			server.AddHandler(rule.ID, rf)
			if err := rf.Start(a.ctx); err != nil {
				// Cleanup: stop tunnel and remove handler on forwarder start failure
				if stopErr := t.Stop(); stopErr != nil {
					logger.Error("failed to stop tunnel after forwarder start failure", "rule_id", rule.ID, "error", stopErr)
				}
				server.RemoveHandler(rule.ID)
				return err
			}
			f = rf

		case "exit":
			// Chain exit: accept from previous hop, forward to target
			// Start the appropriate tunnel server based on TunnelType
			if err := a.ensureTunnelServerByType(rule.TunnelType); err != nil {
				return err
			}

			ef := forwarder.NewExitForwarder(rule)
			server := a.getTunnelServer(rule.TunnelType)
			server.AddHandler(rule.ID, ef)
			if err := ef.Start(a.ctx); err != nil {
				// Cleanup: remove handler on forwarder start failure
				server.RemoveHandler(rule.ID)
				return err
			}
			f = ef

		default:
			return fmt.Errorf("unknown role %q for chain rule", rule.Role)
		}

	case forward.RuleTypeDirectChain:
		// Handle direct chain rule - uses direct TCP/UDP connections instead of WS tunnels
		// All roles (entry, relay, exit) use the same DirectChainForwarder
		// The difference is in NextHopAddress/NextHopPort vs TargetAddress/TargetPort
		dcf := forwarder.NewDirectChainForwarder(rule)
		if err := dcf.Start(a.ctx); err != nil {
			return err
		}
		f = dcf

	default:
		return fmt.Errorf("unknown rule type: %s", rule.RuleType)
	}

	a.forwardersMu.Lock()
	a.forwarders[rule.ID] = f
	a.forwardersMu.Unlock()

	logger.Info("forwarder started", "rule_id", rule.ID, "rule_type", rule.RuleType, "tunnel_type", rule.TunnelType)
	return nil
}

// stopForwarder stops and removes a forwarder by rule ID.
// Acquires locks in order: forwardersMu -> tunnelsMu
func (a *Agent) stopForwarder(ruleID string) {
	// Acquire both locks in correct order to prevent deadlock
	a.forwardersMu.Lock()
	a.tunnelsMu.Lock()

	// Stop forwarder if exists
	if f, exists := a.forwarders[ruleID]; exists {
		logger.Info("stopping forwarder", "rule_id", ruleID)
		f.Stop()
		delete(a.forwarders, ruleID)
	}

	// Stop tunnel if exists
	if t, exists := a.tunnels[ruleID]; exists {
		t.Stop()
		delete(a.tunnels, ruleID)
	}

	a.tunnelsMu.Unlock()
	a.forwardersMu.Unlock()
}

// stopAll stops all forwarders and tunnels.
// Acquires locks in order: forwardersMu -> tunnelsMu
func (a *Agent) stopAll() {
	// Acquire both locks in correct order to prevent deadlock
	a.forwardersMu.Lock()
	a.tunnelsMu.Lock()

	// Stop all forwarders
	for _, f := range a.forwarders {
		f.Stop()
	}
	a.forwarders = make(map[string]forwarder.Forwarder)

	// Stop all tunnels
	for _, t := range a.tunnels {
		t.Stop()
	}
	a.tunnels = make(map[string]tunnel.TunnelClient)

	a.tunnelsMu.Unlock()
	a.forwardersMu.Unlock()

	// Stop tunnel servers (no lock needed)
	if a.tunnelServer != nil {
		a.tunnelServer.Stop()
		a.tunnelServer = nil
	}
	if a.tlsTunnelServer != nil {
		a.tlsTunnelServer.Stop()
		a.tlsTunnelServer = nil
	}
}

// updateRulesList updates the internal rules list based on sync data.
func (a *Agent) updateRulesList(data *forward.ConfigSyncData) {
	a.rulesMu.Lock()
	defer a.rulesMu.Unlock()

	// Build map from current rules
	ruleMap := make(map[string]*forward.Rule)
	for i := range a.rules {
		ruleMap[a.rules[i].ID] = &a.rules[i]
	}

	// Remove
	for _, ruleID := range data.Removed {
		delete(ruleMap, ruleID)
	}

	// Add/Update
	for i := range data.Added {
		rule := ruleSyncDataToRule(&data.Added[i])
		ruleMap[rule.ID] = rule
	}
	for i := range data.Updated {
		rule := ruleSyncDataToRule(&data.Updated[i])
		ruleMap[rule.ID] = rule
	}

	// Rebuild rules slice
	a.rules = make([]forward.Rule, 0, len(ruleMap))
	for _, rule := range ruleMap {
		a.rules = append(a.rules, *rule)
	}

	// Update tunnel servers
	if a.tunnelServer != nil {
		a.tunnelServer.UpdateRules(a.rules)
	}
	if a.tlsTunnelServer != nil {
		a.tlsTunnelServer.UpdateRules(a.rules)
	}
}
