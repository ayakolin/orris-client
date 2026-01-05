package agent

import (
	"fmt"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/forwarder"
	"github.com/orris-inc/orris-client/internal/logger"
	"github.com/orris-inc/orris-client/internal/tunnel"
)

// setRuleStatus updates the sync and runtime status of a rule.
// It signals the debounce goroutine to report status (coalesces multiple updates).
func (a *Agent) setRuleStatus(ruleID, syncStatus, runStatus, errMsg string) {
	a.ruleStatusMu.Lock()
	if a.ruleStatus[ruleID] == nil {
		a.ruleStatus[ruleID] = &ruleStatus{}
	}
	a.ruleStatus[ruleID].syncStatus = syncStatus
	a.ruleStatus[ruleID].runStatus = runStatus
	a.ruleStatus[ruleID].errorMessage = errMsg
	if syncStatus == forward.SyncStatusSynced {
		a.ruleStatus[ruleID].syncedAt = time.Now().Unix()
	}
	a.ruleStatusMu.Unlock()

	// Signal debounce goroutine (non-blocking)
	select {
	case a.ruleStatusReportCh <- struct{}{}:
	default:
		// Channel already has a pending signal, skip
	}
}

// deleteRuleStatus removes the status entry for a rule.
func (a *Agent) deleteRuleStatus(ruleID string) {
	a.ruleStatusMu.Lock()
	defer a.ruleStatusMu.Unlock()
	delete(a.ruleStatus, ruleID)
}

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

	// Build new rules map
	ruleMap := make(map[string]*forward.Rule)
	for i := range rules {
		ruleMap[rules[i].ID] = &rules[i]
	}

	// Save client token and build old rules map for comparison
	a.rulesMu.Lock()
	if resp.ClientToken != "" {
		a.clientToken = resp.ClientToken
		logger.Debug("clientToken synced from API", "token_prefix", tokenPrefix(resp.ClientToken))
	} else {
		logger.Debug("API returned empty clientToken")
	}

	// Build old rules map for comparison (detect changed rules)
	oldRules := make(map[string]*forward.Rule)
	for i := range a.rules {
		oldRules[a.rules[i].ID] = &a.rules[i]
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

	// Stop forwarders for removed or changed rules
	var toRestart []*forward.Rule
	a.forwardersMu.Lock()
	for ruleID, f := range a.forwarders {
		newRule, exists := ruleMap[ruleID]
		if !exists {
			// Rule removed
			logger.Info("stopping forwarder for removed rule", "rule_id", ruleID)
			f.Stop()
			delete(a.forwarders, ruleID)
			a.deleteRuleStatus(ruleID)
		} else if oldRule, hadOld := oldRules[ruleID]; hadOld && ruleConfigChanged(oldRule, newRule) {
			// Rule exists but config changed - need restart
			logger.Info("stopping forwarder for changed rule", "rule_id", ruleID)
			f.Stop()
			delete(a.forwarders, ruleID)
			toRestart = append(toRestart, newRule)
		}
	}
	a.forwardersMu.Unlock()

	// Restart forwarders with changed config
	for _, rule := range toRestart {
		if err := a.startForwarder(rule); err != nil {
			logger.Error("restart forwarder failed", "rule_id", rule.ID, "error", err)
		}
	}

	// Collect existing forwarder IDs once to avoid repeated lock acquisition
	a.forwardersMu.RLock()
	existingIDs := make(map[string]struct{}, len(a.forwarders))
	for id := range a.forwarders {
		existingIDs[id] = struct{}{}
	}
	a.forwardersMu.RUnlock()

	// Start forwarders for new rules (no lock needed for existingIDs check)
	for _, rule := range rules {
		if _, exists := existingIDs[rule.ID]; !exists {
			r := rule
			if err := a.startForwarder(&r); err != nil {
				logger.Error("start forwarder failed", "rule_id", rule.ID, "error", err)
			}
		}
	}

	return nil
}

func (a *Agent) startForwarder(rule *forward.Rule) error {
	// Check if protocol is blocked at agent level
	if a.isProtocolBlocked(rule.Protocol) {
		err := fmt.Errorf("protocol %q is blocked", rule.Protocol)
		a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
		logger.Warn("skipping forwarder for blocked protocol", "rule_id", rule.ID, "protocol", rule.Protocol)
		return err
	}

	// Set initial status: pending sync, starting run
	a.setRuleStatus(rule.ID, forward.SyncStatusPending, forward.RunStatusStarting, "")

	var f forwarder.Forwarder

	switch rule.RuleType {
	case forward.RuleTypeDirect:
		df := forwarder.NewDirectForwarder(rule)
		if err := df.Start(a.ctx); err != nil {
			a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
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
				err := fmt.Errorf("entry rule missing next hop info")
				a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
				return err
			}
			if err != nil {
				errMsg := fmt.Errorf("create tunnel: %w", err)
				a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, errMsg.Error())
				return errMsg
			}

			ef := forwarder.NewEntryForwarder(rule, t)
			t.SetHandler(ef)
			if err := ef.Start(a.ctx); err != nil {
				// Cleanup: stop tunnel on forwarder start failure
				if stopErr := t.Stop(); stopErr != nil {
					logger.Error("failed to stop tunnel after forwarder start failure", "rule_id", rule.ID, "error", stopErr)
				}
				a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
				return err
			}
			f = ef

		case "exit":
			// Exit role: accept tunnel connections and forward to target
			// Start the appropriate tunnel server based on TunnelType
			if err := a.ensureTunnelServerByType(rule.TunnelType); err != nil {
				a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
				return err
			}

			ef := forwarder.NewExitForwarder(rule)
			server := a.getTunnelServer(rule.TunnelType)
			server.AddHandler(rule.ID, ef)
			if err := ef.Start(a.ctx); err != nil {
				// Cleanup: remove handler on forwarder start failure
				server.RemoveHandler(rule.ID)
				a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
				return err
			}
			f = ef

		default:
			err := fmt.Errorf("unknown role %q for entry rule", rule.Role)
			a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
			return err
		}

	case forward.RuleTypeChain:
		// Handle chain rule based on agent's role
		switch rule.Role {
		case "entry":
			// Chain entry: connect to next hop
			t, err := a.getOrCreateTunnelByAddress(rule)
			if err != nil {
				errMsg := fmt.Errorf("create tunnel to next hop: %w", err)
				a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, errMsg.Error())
				return errMsg
			}

			ef := forwarder.NewEntryForwarder(rule, t)
			t.SetHandler(ef)
			if err := ef.Start(a.ctx); err != nil {
				// Cleanup: stop tunnel on forwarder start failure
				if stopErr := t.Stop(); stopErr != nil {
					logger.Error("failed to stop tunnel after forwarder start failure", "rule_id", rule.ID, "error", stopErr)
				}
				a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
				return err
			}
			f = ef

		case "relay":
			// Chain relay: accept from previous hop, forward to next hop
			// Log hop mode for debugging
			logger.Info("chain relay forwarder selection",
				"rule_id", rule.ID,
				"hop_mode", rule.HopMode,
				"inbound_mode", rule.InboundMode,
				"outbound_mode", rule.OutboundMode,
				"chain_position", rule.ChainPosition,
				"listen_port", rule.ListenPort,
				"next_hop_address", rule.NextHopAddress,
				"next_hop_port", rule.NextHopPort)

			// Check HopMode to determine connection types
			if rule.HopMode == "direct" {
				// Direct relay: direct inbound -> direct outbound
				// Uses DirectChainForwarder which handles TCP/UDP listening and forwarding
				logger.Info("using direct relay forwarder", "rule_id", rule.ID)
				dcf := forwarder.NewDirectChainForwarder(rule)
				if err := dcf.Start(a.ctx); err != nil {
					a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
					return err
				}
				f = dcf
			} else if rule.HopMode == "boundary" && rule.OutboundMode == "direct" {
				// Boundary relay: tunnel inbound -> direct outbound
				logger.Info("using boundary relay forwarder", "rule_id", rule.ID)
				if err := a.ensureTunnelServerByType(rule.TunnelType); err != nil {
					a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
					return err
				}
				server := a.getTunnelServer(rule.TunnelType)

				brf := forwarder.NewBoundaryRelayForwarder(rule)
				server.AddHandler(rule.ID, brf)
				if err := brf.Start(a.ctx); err != nil {
					server.RemoveHandler(rule.ID)
					a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
					return err
				}
				f = brf
			} else {
				// Normal relay: tunnel inbound -> tunnel outbound
				logger.Info("using tunnel relay forwarder", "rule_id", rule.ID)
				if err := a.ensureTunnelServerByType(rule.TunnelType); err != nil {
					a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
					return err
				}
				server := a.getTunnelServer(rule.TunnelType)

				t, err := a.getOrCreateTunnelByAddress(rule)
				if err != nil {
					errMsg := fmt.Errorf("create tunnel to next hop: %w", err)
					a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, errMsg.Error())
					return errMsg
				}

				rf := forwarder.NewRelayForwarder(rule, t)
				server.AddHandler(rule.ID, rf)
				if err := rf.Start(a.ctx); err != nil {
					// Cleanup: stop tunnel and remove handler on forwarder start failure
					if stopErr := t.Stop(); stopErr != nil {
						logger.Error("failed to stop tunnel after forwarder start failure", "rule_id", rule.ID, "error", stopErr)
					}
					server.RemoveHandler(rule.ID)
					a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
					return err
				}
				f = rf
			}

		case "exit":
			// Chain exit: accept from previous hop, forward to target
			// Log hop mode for debugging
			logger.Info("chain exit forwarder selection",
				"rule_id", rule.ID,
				"hop_mode", rule.HopMode,
				"inbound_mode", rule.InboundMode,
				"chain_position", rule.ChainPosition,
				"is_last_in_chain", rule.IsLastInChain,
				"listen_port", rule.ListenPort,
				"target_address", rule.TargetAddress,
				"target_port", rule.TargetPort)

			// Check InboundMode to determine connection type
			if rule.InboundMode == "direct" || rule.HopMode == "direct" {
				// Direct exit: direct inbound -> target
				// Uses DirectChainForwarder with IsLastInChain=true
				logger.Info("using direct exit forwarder", "rule_id", rule.ID)
				dcf := forwarder.NewDirectChainForwarder(rule)
				if err := dcf.Start(a.ctx); err != nil {
					a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
					return err
				}
				f = dcf
			} else {
				// Normal exit: tunnel inbound -> target
				logger.Info("using tunnel exit forwarder", "rule_id", rule.ID)
				if err := a.ensureTunnelServerByType(rule.TunnelType); err != nil {
					a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
					return err
				}

				ef := forwarder.NewExitForwarder(rule)
				server := a.getTunnelServer(rule.TunnelType)
				server.AddHandler(rule.ID, ef)
				if err := ef.Start(a.ctx); err != nil {
					// Cleanup: remove handler on forwarder start failure
					server.RemoveHandler(rule.ID)
					a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
					return err
				}
				f = ef
			}

		default:
			err := fmt.Errorf("unknown role %q for chain rule", rule.Role)
			a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
			return err
		}

	case forward.RuleTypeDirectChain:
		// Handle direct chain rule - uses direct TCP/UDP connections instead of WS tunnels
		// All roles (entry, relay, exit) use the same DirectChainForwarder
		// The difference is in NextHopAddress/NextHopPort vs TargetAddress/TargetPort
		dcf := forwarder.NewDirectChainForwarder(rule)
		if err := dcf.Start(a.ctx); err != nil {
			a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
			return err
		}
		f = dcf

	default:
		err := fmt.Errorf("unknown rule type: %s", rule.RuleType)
		a.setRuleStatus(rule.ID, forward.SyncStatusFailed, forward.RunStatusError, err.Error())
		return err
	}

	a.forwardersMu.Lock()
	a.forwarders[rule.ID] = f
	a.forwardersMu.Unlock()

	// Set success status: synced and running
	a.setRuleStatus(rule.ID, forward.SyncStatusSynced, forward.RunStatusRunning, "")

	logger.Info("forwarder started", "rule_id", rule.ID, "rule_type", rule.RuleType, "tunnel_type", rule.TunnelType)
	return nil
}

// stopForwarder stops and removes a forwarder by rule ID.
// Acquires locks in order: forwardersMu -> tunnelsMu
// If updateStatus is true, sets the rule status to stopped after stopping.
// Pass false when the rule is being removed or will be restarted immediately.
func (a *Agent) stopForwarder(ruleID string, updateStatus bool) {
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

	// Update status: synced but stopped (skip if rule is being removed or restarted)
	if updateStatus {
		a.setRuleStatus(ruleID, forward.SyncStatusSynced, forward.RunStatusStopped, "")
	}
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
