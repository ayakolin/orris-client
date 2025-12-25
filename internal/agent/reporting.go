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

	ticker := time.NewTicker(a.cfg.StatusInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.reportStatus()
		}
	}
}

func (a *Agent) reportStatus() {
	st, err := a.collector.Collect(a.ctx)
	if err != nil {
		logger.Error("collect status failed", "error", err)
		return
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
			return
		}
	}

	// Fallback to HTTP POST when WebSocket is not available or failed
	if err := a.client.ReportStatus(a.ctx, st); err != nil {
		logger.Error("report status via HTTP failed", "error", err)
		return
	}

	logger.Debug("status reported via HTTP",
		"cpu", fmt.Sprintf("%.1f%%", st.CPUPercent),
		"mem", fmt.Sprintf("%.1f%%", st.MemoryPercent),
		"rules", st.ActiveRules)
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
