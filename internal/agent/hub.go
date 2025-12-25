package agent

import (
	"fmt"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
)

// hubLoop manages the Hub WebSocket connection with automatic reconnection.
func (a *Agent) hubLoop() {
	defer a.wg.Done()

	reconnectCfg := forward.DefaultReconnectConfig()
	reconnectCfg.OnConnected = func() {
		logger.Info("hub connected")
	}
	reconnectCfg.OnDisconnected = func(err error) {
		if err != nil && a.ctx.Err() == nil {
			logger.Warn("hub disconnected", "error", err)
		}
	}
	reconnectCfg.OnReconnecting = func(attempt uint64, delay time.Duration) {
		logger.Info("reconnecting to hub...", "attempt", attempt, "delay", delay)
	}

	a.runHubWithReconnect(reconnectCfg)
}

// runHubWithReconnect runs the hub connection loop with reconnection logic.
func (a *Agent) runHubWithReconnect(reconnectCfg *forward.ReconnectConfig) {
	backoff := time.Second
	maxBackoff := reconnectCfg.MaxInterval
	if maxBackoff == 0 {
		maxBackoff = 60 * time.Second
	}

	for {
		select {
		case <-a.ctx.Done():
			return
		default:
		}

		err := a.runHubOnce(reconnectCfg)
		if a.ctx.Err() != nil {
			return
		}

		if reconnectCfg.OnDisconnected != nil {
			reconnectCfg.OnDisconnected(err)
		}

		// Exponential backoff
		logger.Info("reconnecting to hub...", "delay", backoff)
		select {
		case <-a.ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff = time.Duration(float64(backoff) * reconnectCfg.Multiplier)
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// runHubOnce runs a single hub connection lifecycle.
func (a *Agent) runHubOnce(reconnectCfg *forward.ReconnectConfig) error {
	conn, err := a.client.ForwardClient().ConnectHub(a.ctx)
	if err != nil {
		return fmt.Errorf("connect hub: %w", err)
	}
	defer conn.Close()

	if reconnectCfg.OnConnected != nil {
		reconnectCfg.OnConnected()
	}

	// Start connection read/write pumps in background
	connErrCh := make(chan error, 1)
	go func() {
		connErrCh <- conn.Run(a.ctx)
	}()

	// Process events from hub
	for {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()

		case err := <-connErrCh:
			return err

		case event, ok := <-conn.Events:
			if !ok {
				return fmt.Errorf("events channel closed")
			}
			a.handleHubEvent(conn, event)
		}
	}
}

// handleHubEvent processes events from hub connection.
func (a *Agent) handleHubEvent(conn *forward.HubConn, event *forward.HubEvent) {
	switch event.Type {
	case forward.HubEventConfigSync:
		if event.ConfigSync != nil {
			a.handleConfigSync(conn, event.ConfigSync)
		}

	case forward.HubEventProbeTask:
		if event.ProbeTask != nil {
			go func() {
				result := a.executeProbe(event.ProbeTask)
				if result != nil {
					conn.SendProbeResult(result)
				}
			}()
		}
	}
}

// handleConfigSync processes configuration sync events from hub.
func (a *Agent) handleConfigSync(conn *forward.HubConn, data *forward.ConfigSyncData) {
	logger.Info("received config sync",
		"version", data.Version,
		"current_version", a.configVersion,
		"full_sync", data.FullSync,
		"added", len(data.Added),
		"updated", len(data.Updated),
		"removed", len(data.Removed))

	// Version check logic:
	// - For FullSync: always accept (server-initiated forced sync)
	// - For incremental sync: only accept if received version is strictly greater than current version
	// - This prevents out-of-order updates (e.g., receiving v103 after v105)
	if !data.FullSync {
		if data.Version <= a.configVersion {
			logger.Warn("skipping outdated config sync",
				"current_version", a.configVersion,
				"received_version", data.Version,
				"reason", "received version not newer than current")
			conn.SendConfigAck(&forward.ConfigAckData{
				Version: data.Version,
				Success: true,
			})
			return
		}
	} else {
		// FullSync can override any version (forced synchronization from server)
		logger.Info("accepting full sync regardless of version",
			"current_version", a.configVersion,
			"received_version", data.Version)
	}

	var syncErr error

	// Handle full sync - stop all and restart
	if data.FullSync {
		syncErr = a.handleFullSync(data)
	} else {
		syncErr = a.handleIncrementalSync(data)
	}

	// Send acknowledgment
	ack := &forward.ConfigAckData{
		Version: data.Version,
		Success: syncErr == nil,
	}
	if syncErr != nil {
		ack.Error = syncErr.Error()
		logger.Error("config sync failed",
			"version", data.Version,
			"error", syncErr)
	} else {
		oldVersion := a.configVersion
		a.configVersion = data.Version
		logger.Info("config sync completed",
			"old_version", oldVersion,
			"new_version", data.Version,
			"full_sync", data.FullSync)
	}

	conn.SendConfigAck(ack)
}

// handleFullSync handles full configuration sync.
func (a *Agent) handleFullSync(data *forward.ConfigSyncData) error {
	// Build rule map from added rules (no lock needed for local variable)
	newRules := make(map[string]*forward.Rule)
	for i := range data.Added {
		rule := ruleSyncDataToRule(&data.Added[i])
		newRules[rule.ID] = rule
	}

	// Acquire locks in order: rulesMu -> forwardersMu
	// This prevents deadlock by ensuring consistent lock ordering
	a.rulesMu.Lock()
	a.forwardersMu.Lock()

	// Update token info from full sync (ensures agent always has correct token)
	if data.ClientToken != "" {
		a.clientToken = data.ClientToken
	}

	// Update rules list for tunnel server
	a.rules = make([]forward.Rule, 0, len(newRules))
	for _, rule := range newRules {
		a.rules = append(a.rules, *rule)
	}

	// Stop forwarders not in new rules
	for ruleID, f := range a.forwarders {
		if _, exists := newRules[ruleID]; !exists {
			logger.Info("stopping forwarder for removed rule", "rule_id", ruleID)
			f.Stop()
			delete(a.forwarders, ruleID)
		}
	}

	// Copy rules for tunnel server update (to avoid holding lock during update)
	rulesCopy := make([]forward.Rule, len(a.rules))
	copy(rulesCopy, a.rules)

	// Release locks before tunnel server update and starting forwarders
	a.forwardersMu.Unlock()
	a.rulesMu.Unlock()

	// Update tunnel server with copied rules (no lock held)
	if a.tunnelServer != nil {
		a.tunnelServer.UpdateRules(rulesCopy)
	}

	// Start forwarders for new rules
	for _, rule := range newRules {
		a.forwardersMu.RLock()
		_, exists := a.forwarders[rule.ID]
		a.forwardersMu.RUnlock()

		if !exists {
			if err := a.startForwarder(rule); err != nil {
				logger.Error("start forwarder failed", "rule_id", rule.ID, "error", err)
			}
		}
	}

	return nil
}

// handleIncrementalSync handles incremental configuration sync.
func (a *Agent) handleIncrementalSync(data *forward.ConfigSyncData) error {
	// Handle removed rules
	for _, ruleID := range data.Removed {
		a.stopForwarder(ruleID)
	}

	// Handle updated rules (stop then start)
	for i := range data.Updated {
		rule := ruleSyncDataToRule(&data.Updated[i])
		a.stopForwarder(rule.ID)
		if err := a.startForwarder(rule); err != nil {
			logger.Error("restart forwarder failed", "rule_id", rule.ID, "error", err)
		}
	}

	// Handle added rules
	for i := range data.Added {
		rule := ruleSyncDataToRule(&data.Added[i])
		if err := a.startForwarder(rule); err != nil {
			logger.Error("start forwarder failed", "rule_id", rule.ID, "error", err)
		}
	}

	// Update rules list
	a.updateRulesList(data)

	return nil
}

// ruleSyncDataToRule converts RuleSyncData to forward.Rule.
func ruleSyncDataToRule(data *forward.RuleSyncData) *forward.Rule {
	return &forward.Rule{
		ID:                     data.ID,
		AgentID:                data.AgentID,
		RuleType:               forward.RuleType(data.RuleType),
		ListenPort:             data.ListenPort,
		TargetAddress:          data.TargetAddress,
		TargetPort:             data.TargetPort,
		BindIP:                 data.BindIP,
		Protocol:               data.Protocol,
		Role:                   data.Role,
		TunnelType:             forward.TunnelType(data.TunnelType),
		NextHopAgentID:         data.NextHopAgentID,
		NextHopAddress:         data.NextHopAddress,
		NextHopWsPort:          data.NextHopWsPort,
		NextHopTlsPort:         data.NextHopTlsPort,
		NextHopPort:            data.NextHopPort,
		NextHopConnectionToken: data.NextHopConnectionToken,
		ChainAgentIDs:          data.ChainAgentIDs,
		ChainPosition:          data.ChainPosition,
		IsLastInChain:          data.IsLastInChain,
	}
}
