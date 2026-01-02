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

func (a *Agent) reportTraffic() {
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

	if len(items) == 0 {
		return
	}

	var totalUpload, totalDownload int64
	for _, item := range items {
		totalUpload += item.UploadBytes
		totalDownload += item.DownloadBytes
	}

	if err := a.client.ReportTraffic(a.ctx, items); err != nil {
		logger.Error("report traffic failed", "error", err)
		return
	}
	logger.Info("traffic reported",
		"rules", len(items),
		"upload_bytes", totalUpload,
		"download_bytes", totalDownload)
}

func (a *Agent) reportFinalTraffic() {
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
		logger.Error("report final traffic failed", "error", err)
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
