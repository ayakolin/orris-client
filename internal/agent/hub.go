package agent

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/orris-inc/orris-client/internal/config"
	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
	"github.com/orris-inc/orris-client/internal/updater"
	"github.com/orris-inc/orris-client/internal/version"
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
	defer func() {
		a.hubConnMu.Lock()
		a.hubConn = nil
		a.hubConnMu.Unlock()
		conn.Close()
	}()

	// Store hubConn for status reporting via WebSocket
	a.hubConnMu.Lock()
	a.hubConn = conn
	a.hubConnMu.Unlock()

	// Set up message handler for command messages
	// Note: Events channel handles config_sync and probe_task, SetMessageHandler handles command
	conn.SetMessageHandler(func(msg *forward.HubMessage) {
		if msg.Type == forward.MsgTypeCommand {
			a.handleCommand(conn, msg.Data)
		}
	})

	if reconnectCfg.OnConnected != nil {
		reconnectCfg.OnConnected()
	}

	// Report agent version on connect
	if err := conn.SendEvent(forward.EventTypeConnected, "agent connected", map[string]any{
		"version":    version.Version,
		"commit":     version.Commit,
		"build_time": version.BuildTime,
		"platform":   version.Platform(),
		"arch":       version.Arch(),
	}); err != nil {
		logger.Warn("failed to send connected event", "error", err)
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
				result := a.executeProbe(a.ctx, event.ProbeTask)
				if result != nil {
					conn.SendProbeResult(result)
				}
			}()
		}

	case forward.HubEventAPIURLChanged:
		if event.APIURLChanged != nil {
			a.handleAPIURLChanged(conn, event.APIURLChanged.NewURL, event.APIURLChanged.Reason)
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

	// Update blocked protocols from full sync
	a.blockedProtocols = data.BlockedProtocols
	if len(data.BlockedProtocols) > 0 {
		logger.Info("blocked protocols updated", "protocols", data.BlockedProtocols)
	}

	// Build old rules map for comparison (detect changed rules)
	oldRules := make(map[string]*forward.Rule)
	for i := range a.rules {
		oldRules[a.rules[i].ID] = &a.rules[i]
	}

	// Update rules list for tunnel server
	a.rules = make([]forward.Rule, 0, len(newRules))
	for _, rule := range newRules {
		a.rules = append(a.rules, *rule)
	}

	// Collect forwarders to restart (rule exists but config changed)
	var toRestart []*forward.Rule
	for ruleID, f := range a.forwarders {
		newRule, exists := newRules[ruleID]
		if !exists {
			// Rule removed - stop forwarder and clean up status
			// Lock order: forwardersMu (held) -> ruleStatusMu (acquired by deleteRuleStatus)
			logger.Info("stopping forwarder for removed rule", "rule_id", ruleID)
			f.Stop()
			delete(a.forwarders, ruleID)
			a.deleteRuleStatus(ruleID)
		} else if a.isProtocolBlockedUnsafe(newRule.Protocol) {
			// Protocol blocked - stop forwarder (no restart)
			// Lock order: rulesMu (held) -> forwardersMu (held) -> ruleStatusMu (acquired by setRuleStatus)
			logger.Info("stopping forwarder for blocked protocol", "rule_id", ruleID, "protocol", newRule.Protocol)
			f.Stop()
			delete(a.forwarders, ruleID)
			a.setRuleStatus(ruleID, forward.SyncStatusFailed, forward.RunStatusError, "protocol blocked")
		} else if oldRule, hadOld := oldRules[ruleID]; hadOld && ruleConfigChanged(oldRule, newRule) {
			// Rule exists but config changed - need restart
			logger.Info("stopping forwarder for changed rule", "rule_id", ruleID)
			f.Stop()
			delete(a.forwarders, ruleID)
			toRestart = append(toRestart, newRule)
		}
	}

	// Copy rules for tunnel server update (to avoid holding lock during update)
	rulesCopy := make([]forward.Rule, len(a.rules))
	copy(rulesCopy, a.rules)

	// Release locks before tunnel server update and starting forwarders
	a.forwardersMu.Unlock()
	a.rulesMu.Unlock()

	// Update tunnel servers with copied rules (no lock held)
	if a.tunnelServer != nil {
		a.tunnelServer.UpdateRules(rulesCopy)
	}
	if a.tlsTunnelServer != nil {
		a.tlsTunnelServer.UpdateRules(rulesCopy)
	}

	// Restart forwarders with changed config
	for _, rule := range toRestart {
		if err := a.startForwarder(rule); err != nil {
			logger.Error("restart forwarder failed", "rule_id", rule.ID, "error", err)
		}
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

// ruleConfigChanged checks if rule configuration that affects forwarder behavior has changed.
// Returns true if the forwarder needs to be restarted.
func ruleConfigChanged(old, new *forward.Rule) bool {
	// Check connection-related fields
	if old.NextHopAddress != new.NextHopAddress ||
		old.NextHopPort != new.NextHopPort ||
		old.NextHopWsPort != new.NextHopWsPort ||
		old.NextHopTlsPort != new.NextHopTlsPort ||
		old.TargetAddress != new.TargetAddress ||
		old.TargetPort != new.TargetPort ||
		old.BindIP != new.BindIP ||
		old.ListenPort != new.ListenPort ||
		old.Protocol != new.Protocol ||
		old.TunnelType != new.TunnelType ||
		old.IsLastInChain != new.IsLastInChain ||
		old.HopMode != new.HopMode ||
		old.OutboundMode != new.OutboundMode {
		return true
	}
	return false
}

// handleIncrementalSync handles incremental configuration sync.
func (a *Agent) handleIncrementalSync(data *forward.ConfigSyncData) error {
	// Update blocked protocols if provided
	if data.BlockedProtocols != nil {
		a.updateBlockedProtocols(data.BlockedProtocols)
	}

	// Handle removed rules (updateStatus=false since we delete status immediately after)
	for _, ruleID := range data.Removed {
		a.stopForwarder(ruleID, false)
		a.deleteRuleStatus(ruleID)
	}

	// Handle updated rules (stop then start)
	// updateStatus=false since startForwarder will set the new status
	for i := range data.Updated {
		rule := ruleSyncDataToRule(&data.Updated[i])
		a.stopForwarder(rule.ID, false)
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
		TunnelHops:             data.TunnelHops,
		HopMode:                data.HopMode,
		InboundMode:            data.InboundMode,
		OutboundMode:           data.OutboundMode,
		ChainAgentIDs:          data.ChainAgentIDs,
		ChainPosition:          data.ChainPosition,
		IsLastInChain:          data.IsLastInChain,
	}
}

// handleCommand processes command messages from hub.
func (a *Agent) handleCommand(conn *forward.HubConn, data any) {
	cmd := parseCommandData(data)
	if cmd == nil {
		logger.Warn("failed to parse command data")
		return
	}

	logger.Info("received command", "command_id", cmd.CommandID, "action", cmd.Action)

	switch cmd.Action {
	case forward.CmdActionReloadConfig:
		a.handleReloadConfigCommand(conn, cmd)
	case forward.CmdActionRestartRule:
		a.handleRestartRuleCommand(conn, cmd)
	case forward.CmdActionStopRule:
		a.handleStopRuleCommand(conn, cmd)
	case forward.CmdActionProbe:
		a.handleProbeCommand(conn, cmd)
	case forward.CmdActionUpdate:
		a.handleUpdateCommand(conn, cmd)
	case forward.CmdActionAPIURLChanged:
		a.handleAPIURLChangedCommand(conn, cmd)
	default:
		logger.Warn("unknown command action", "action", cmd.Action)
	}
}

// parseCommandData parses CommandData from message data.
func parseCommandData(data any) *forward.CommandData {
	dataMap, ok := data.(map[string]any)
	if !ok {
		return nil
	}

	cmd := &forward.CommandData{}
	if v, ok := dataMap["command_id"].(string); ok {
		cmd.CommandID = v
	}
	if v, ok := dataMap["action"].(string); ok {
		cmd.Action = v
	}
	if v, ok := dataMap["payload"]; ok {
		cmd.Payload = v
	}
	return cmd
}

// handleReloadConfigCommand handles reload_config command.
func (a *Agent) handleReloadConfigCommand(conn *forward.HubConn, cmd *forward.CommandData) {
	logger.Info("reloading config", "command_id", cmd.CommandID)
	// Trigger a config resync by sending an event
	if err := conn.SendEvent(forward.EventTypeConfigChange, "config reload requested", map[string]any{
		"command_id": cmd.CommandID,
	}); err != nil {
		logger.Warn("failed to send config reload event", "error", err)
	}
}

// handleRestartRuleCommand handles restart_rule command.
func (a *Agent) handleRestartRuleCommand(_ *forward.HubConn, cmd *forward.CommandData) {
	payload, ok := cmd.Payload.(map[string]any)
	if !ok {
		logger.Warn("invalid restart_rule payload", "command_id", cmd.CommandID)
		return
	}

	ruleID, ok := payload["rule_id"].(string)
	if !ok {
		logger.Warn("missing rule_id in restart_rule payload", "command_id", cmd.CommandID)
		return
	}

	logger.Info("restarting rule", "command_id", cmd.CommandID, "rule_id", ruleID)

	// Stop and restart the forwarder
	a.stopForwarder(ruleID, false)

	// Get the rule from the rules list (copy to avoid data race)
	a.rulesMu.RLock()
	var ruleCopy forward.Rule
	var found bool
	for i := range a.rules {
		if a.rules[i].ID == ruleID {
			ruleCopy = a.rules[i]
			found = true
			break
		}
	}
	a.rulesMu.RUnlock()

	if found {
		if err := a.startForwarder(&ruleCopy); err != nil {
			logger.Error("failed to restart rule", "rule_id", ruleID, "error", err)
		}
	} else {
		logger.Warn("rule not found for restart", "rule_id", ruleID)
	}
}

// handleStopRuleCommand handles stop_rule command.
func (a *Agent) handleStopRuleCommand(_ *forward.HubConn, cmd *forward.CommandData) {
	payload, ok := cmd.Payload.(map[string]any)
	if !ok {
		logger.Warn("invalid stop_rule payload", "command_id", cmd.CommandID)
		return
	}

	ruleID, ok := payload["rule_id"].(string)
	if !ok {
		logger.Warn("missing rule_id in stop_rule payload", "command_id", cmd.CommandID)
		return
	}

	logger.Info("stopping rule", "command_id", cmd.CommandID, "rule_id", ruleID)
	a.stopForwarder(ruleID, true)
}

// handleProbeCommand handles probe command.
func (a *Agent) handleProbeCommand(_ *forward.HubConn, cmd *forward.CommandData) {
	logger.Debug("probe command received via command channel", "command_id", cmd.CommandID)
	// Probe tasks are typically handled via the probe_task message type,
	// but this command can be used for on-demand probes if needed.
}

// updateTimeout is the maximum time allowed for update download and installation.
const updateTimeout = 10 * time.Minute

// handleUpdateCommand handles update command.
func (a *Agent) handleUpdateCommand(conn *forward.HubConn, cmd *forward.CommandData) {
	logger.Info("update command received", "command_id", cmd.CommandID)

	// Parse update payload
	payload, err := updater.ParsePayload(cmd.Payload)
	if err != nil {
		logger.Error("failed to parse update payload", "error", err)
		if err := conn.SendEvent(forward.EventTypeError, "update failed: invalid payload", map[string]any{
			"command_id": cmd.CommandID,
			"error":      err.Error(),
		}); err != nil {
			logger.Warn("failed to send update error event", "error", err)
		}
		return
	}

	// Perform update in background with timeout
	go func() {
		// Create timeout context for update operation
		updateCtx, cancel := context.WithTimeout(context.Background(), updateTimeout)
		defer cancel()

		// Run update with timeout
		resultCh := make(chan struct {
			needsRestart bool
			err          error
		}, 1)

		go func() {
			needsRestart, err := updater.Update(payload)
			resultCh <- struct {
				needsRestart bool
				err          error
			}{needsRestart, err}
		}()

		select {
		case <-updateCtx.Done():
			logger.Error("update timed out", "timeout", updateTimeout)
			a.sendUpdateEvent(forward.EventTypeError, "update failed: timeout", map[string]any{
				"command_id": cmd.CommandID,
				"error":      "update operation timed out",
			})
			return

		case result := <-resultCh:
			if result.err != nil {
				logger.Error("update failed", "error", result.err)
				a.sendUpdateEvent(forward.EventTypeError, "update failed", map[string]any{
					"command_id": cmd.CommandID,
					"error":      result.err.Error(),
				})
				return
			}

			if result.needsRestart {
				logger.Info("update successful, restarting agent")
				a.sendUpdateEvent(forward.EventTypeConfigChange, "update successful, restarting", map[string]any{
					"command_id":  cmd.CommandID,
					"old_version": version.Version,
					"new_version": payload.Version,
				})

				// Give time for event to be sent
				time.Sleep(500 * time.Millisecond)

				// Exit to let systemd/supervisor restart the process
				os.Exit(0)
			}
		}
	}()
}

// handleAPIURLChangedCommand handles api_url_changed command from server.
func (a *Agent) handleAPIURLChangedCommand(conn *forward.HubConn, cmd *forward.CommandData) {
	payload, ok := cmd.Payload.(map[string]any)
	if !ok {
		logger.Warn("invalid api_url_changed payload", "command_id", cmd.CommandID)
		return
	}

	newURL, _ := payload["new_url"].(string)
	reason, _ := payload["reason"].(string)

	if newURL == "" {
		logger.Warn("missing new_url in api_url_changed payload", "command_id", cmd.CommandID)
		return
	}

	a.handleAPIURLChanged(conn, newURL, reason)
}

// handleAPIURLChanged handles API URL change notification.
// It validates the new URL, saves it to config file, and exits to let systemd/supervisor restart.
func (a *Agent) handleAPIURLChanged(_ *forward.HubConn, newURL, reason string) {
	// Use redacted URL in logs to avoid leaking credentials
	redactedURL := config.RedactURL(newURL)

	logger.Info("received API URL change notification",
		"new_url", redactedURL,
		"reason", reason)

	// Validate URL before accepting
	if err := config.ValidateServerURL(newURL); err != nil {
		logger.Error("rejected invalid server URL from server",
			"new_url", redactedURL,
			"error", err)
		return
	}

	// Save new URL to config file
	if err := config.SaveServerURL(newURL); err != nil {
		logger.Error("failed to save new server URL to config", "error", err)
		// Do not restart if we can't save the config - would cause infinite restart loop
		return
	}

	logger.Info("saved new server URL to config", "path", config.ConfigFilePath())

	// Give time for log to be written
	time.Sleep(100 * time.Millisecond)

	// Exit to let systemd/supervisor restart the process with new URL
	logger.Info("exiting for restart with new API URL")
	os.Exit(0)
}

// sendUpdateEvent sends an update-related event via the current hub connection.
// It safely handles the case where the connection may have been closed or reconnected.
func (a *Agent) sendUpdateEvent(eventType, message string, extra map[string]any) {
	a.hubConnMu.RLock()
	conn := a.hubConn
	a.hubConnMu.RUnlock()

	if conn == nil {
		logger.Warn("cannot send update event: hub not connected", "event_type", eventType)
		return
	}

	if err := conn.SendEvent(eventType, message, extra); err != nil {
		logger.Warn("failed to send update event", "event_type", eventType, "error", err)
	}
}
