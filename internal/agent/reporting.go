package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
)

func (a *Agent) trafficLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.cfg.TrafficInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.reportTraffic()
		}
	}
}

func (a *Agent) statusLoop() {
	defer a.wg.Done()

	// Start with WebSocket interval, adjust dynamically based on connection mode
	currentInterval := a.cfg.StatusInterval
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			usedWs := a.reportStatus()

			// Adjust interval based on current mode
			var targetInterval time.Duration
			if usedWs {
				targetInterval = a.cfg.StatusInterval
			} else {
				targetInterval = a.cfg.StatusIntervalRest
			}

			if targetInterval != currentInterval {
				currentInterval = targetInterval
				ticker.Reset(currentInterval)
				logger.Debug("status interval adjusted", "interval", currentInterval)
			}
		}
	}
}

// reportStatus collects and reports agent status. Returns true if WebSocket was used.
func (a *Agent) reportStatus() bool {
	st, err := a.collector.Collect(a.ctx)
	if err != nil {
		logger.Error("collect status failed", "error", err)
		return false
	}

	// Set active rules and connections
	a.forwardersMu.RLock()
	activeRules := len(a.forwarders)
	a.forwardersMu.RUnlock()

	a.collector.SetActiveStats(st, activeRules, 0)

	// Set tunnel status
	a.tunnelsMu.RLock()
	tunnelStatus := make(map[string]forward.TunnelState)
	for exitAgentID, t := range a.tunnels {
		if t.IsConnected() {
			tunnelStatus[exitAgentID] = forward.TunnelStateConnected
		} else {
			tunnelStatus[exitAgentID] = forward.TunnelStateDisconnected
		}
	}
	a.tunnelsMu.RUnlock()

	a.collector.SetTunnelStatus(st, tunnelStatus)

	// Set tunnel listen ports if configured (for exit/relay agents)
	if a.cfg.WsListenPort > 0 {
		st.WsListenPort = a.cfg.WsListenPort
	}
	if a.cfg.TlsListenPort > 0 {
		st.TlsListenPort = a.cfg.TlsListenPort
	}

	// Report status via WebSocket if hub connection is available
	a.hubConnMu.RLock()
	conn := a.hubConn
	a.hubConnMu.RUnlock()

	if conn != nil {
		if err := conn.SendStatus(st); err != nil {
			logger.Warn("report status via websocket failed, falling back to HTTP", "error", err)
		} else {
			logger.Debug("status reported via websocket",
				"cpu", fmt.Sprintf("%.1f%%", st.CPUPercent),
				"mem", fmt.Sprintf("%.1f%%", st.MemoryPercent),
				"rules", st.ActiveRules)
			// Report rule sync status via WebSocket
			a.reportRuleSyncStatus()
			return true
		}
	}

	// Fallback to HTTP POST when WebSocket is not available or failed
	if err := a.client.ReportStatus(a.ctx, st); err != nil {
		logger.Error("report status via HTTP failed", "error", err)
		return false
	}

	logger.Debug("status reported via HTTP",
		"cpu", fmt.Sprintf("%.1f%%", st.CPUPercent),
		"mem", fmt.Sprintf("%.1f%%", st.MemoryPercent),
		"rules", st.ActiveRules)
	return false
}

// maxPendingTrafficRules limits pending traffic items to prevent memory bloat.
const maxPendingTrafficRules = 1000

func (a *Agent) reportTraffic() {
	// Collect current traffic
	a.forwardersMu.RLock()
	items := make([]forward.TrafficItem, 0, len(a.forwarders))
	for _, f := range a.forwarders {
		upload, download := f.Traffic().GetAndReset()
		if upload > 0 || download > 0 {
			items = append(items, forward.TrafficItem{
				RuleID:        f.RuleID(),
				UploadBytes:   upload,
				DownloadBytes: download,
			})
		}
	}
	a.forwardersMu.RUnlock()

	// Merge with pending traffic from previous failed attempts
	a.pendingTrafficMu.Lock()
	if len(a.pendingTraffic) > 0 {
		items = mergeTrafficItems(a.pendingTraffic, items)
		a.pendingTraffic = nil
	}
	a.pendingTrafficMu.Unlock()

	if len(items) == 0 {
		return
	}

	var totalUpload, totalDownload int64
	for _, item := range items {
		totalUpload += item.UploadBytes
		totalDownload += item.DownloadBytes
	}

	// Try WebSocket first
	a.hubConnMu.RLock()
	conn := a.hubConn
	a.hubConnMu.RUnlock()

	if conn != nil {
		if err := a.sendTrafficViaWs(conn, items); err != nil {
			logger.Warn("report traffic via websocket failed, falling back to HTTP", "error", err)
		} else {
			logger.Debug("traffic reported via websocket",
				"rules", len(items),
				"upload_bytes", totalUpload,
				"download_bytes", totalDownload)
			return
		}
	}

	// Fallback to HTTP POST
	if err := a.client.ReportTraffic(a.ctx, items); err != nil {
		logger.Warn("report traffic via HTTP failed, will retry next interval", "error", err)
		a.savePendingTraffic(items)
		return
	}
	logger.Debug("traffic reported via HTTP",
		"rules", len(items),
		"upload_bytes", totalUpload,
		"download_bytes", totalDownload)
}

// savePendingTraffic saves failed traffic items for retry.
func (a *Agent) savePendingTraffic(items []forward.TrafficItem) {
	a.pendingTrafficMu.Lock()
	defer a.pendingTrafficMu.Unlock()

	// Merge with existing pending items
	a.pendingTraffic = mergeTrafficItems(a.pendingTraffic, items)

	// Limit pending items to prevent memory bloat
	if len(a.pendingTraffic) > maxPendingTrafficRules {
		dropped := len(a.pendingTraffic) - maxPendingTrafficRules
		a.pendingTraffic = a.pendingTraffic[dropped:]
		logger.Warn("pending traffic exceeded limit, dropped oldest items", "dropped", dropped)
	}
}

// mergeTrafficItems merges two traffic item slices, combining items with same RuleID.
func mergeTrafficItems(existing, new []forward.TrafficItem) []forward.TrafficItem {
	if len(existing) == 0 {
		return new
	}
	if len(new) == 0 {
		return existing
	}

	// Build map from existing items
	merged := make(map[string]*forward.TrafficItem, len(existing)+len(new))
	for i := range existing {
		item := &existing[i]
		merged[item.RuleID] = item
	}

	// Merge new items
	for _, item := range new {
		if m, ok := merged[item.RuleID]; ok {
			m.UploadBytes += item.UploadBytes
			m.DownloadBytes += item.DownloadBytes
		} else {
			itemCopy := item
			merged[item.RuleID] = &itemCopy
		}
	}

	// Convert back to slice
	result := make([]forward.TrafficItem, 0, len(merged))
	for _, item := range merged {
		result = append(result, *item)
	}
	return result
}

// sendTrafficViaWs sends traffic data via WebSocket connection.
func (a *Agent) sendTrafficViaWs(conn *forward.HubConn, items []forward.TrafficItem) error {
	data := &forward.TrafficEventData{
		Rules: items,
	}
	logger.Debug("sending traffic event via websocket",
		"event_type", forward.EventTypeTraffic,
		"rules_count", len(items),
		"data", data)
	return conn.SendEvent(forward.EventTypeTraffic, "", data)
}

func (a *Agent) reportFinalTraffic() {
	// Collect current traffic
	a.forwardersMu.RLock()
	items := make([]forward.TrafficItem, 0, len(a.forwarders))
	for _, f := range a.forwarders {
		upload, download := f.Traffic().GetAndReset()
		if upload > 0 || download > 0 {
			items = append(items, forward.TrafficItem{
				RuleID:        f.RuleID(),
				UploadBytes:   upload,
				DownloadBytes: download,
			})
		}
	}
	a.forwardersMu.RUnlock()

	// Include pending traffic from previous failed attempts
	a.pendingTrafficMu.Lock()
	if len(a.pendingTraffic) > 0 {
		items = mergeTrafficItems(a.pendingTraffic, items)
		a.pendingTraffic = nil
	}
	a.pendingTrafficMu.Unlock()

	if len(items) == 0 {
		return
	}

	var totalUpload, totalDownload int64
	for _, item := range items {
		totalUpload += item.UploadBytes
		totalDownload += item.DownloadBytes
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.client.ReportTraffic(ctx, items); err != nil {
		logger.Error("report final traffic failed", "error", err,
			"rules", len(items),
			"upload_bytes", totalUpload,
			"download_bytes", totalDownload)
		return
	}
	logger.Info("final traffic reported",
		"rules", len(items),
		"upload_bytes", totalUpload,
		"download_bytes", totalDownload)
}

// reportRuleSyncStatus sends rule sync status to the server via WebSocket.
func (a *Agent) reportRuleSyncStatus() {
	a.hubConnMu.RLock()
	conn := a.hubConn
	a.hubConnMu.RUnlock()

	if conn == nil {
		return
	}

	items := a.collectRuleSyncStatus()
	if len(items) == 0 {
		return
	}

	if err := conn.SendRuleSyncStatus(items); err != nil {
		logger.Warn("report rule sync status failed", "error", err)
		return
	}

	logger.Debug("rule sync status reported", "count", len(items))
}

// collectRuleSyncStatus collects the current sync and runtime status of all rules.
func (a *Agent) collectRuleSyncStatus() []forward.RuleSyncStatusItem {
	// Lock order: forwardersMu -> ruleStatusMu (consistent with syncRules)
	a.forwardersMu.RLock()
	a.ruleStatusMu.RLock()
	defer a.forwardersMu.RUnlock()
	defer a.ruleStatusMu.RUnlock()

	if len(a.ruleStatus) == 0 {
		return nil
	}

	items := make([]forward.RuleSyncStatusItem, 0, len(a.ruleStatus))
	for ruleID, status := range a.ruleStatus {
		item := forward.RuleSyncStatusItem{
			RuleID:       ruleID,
			SyncStatus:   status.syncStatus,
			RunStatus:    status.runStatus,
			ErrorMessage: status.errorMessage,
			SyncedAt:     status.syncedAt,
		}
		// Get dynamic data from forwarder if available
		if f, ok := a.forwarders[ruleID]; ok {
			item.ListenPort = f.ListenPort()
			item.Connections = f.Connections()
		}
		items = append(items, item)
	}
	return items
}
