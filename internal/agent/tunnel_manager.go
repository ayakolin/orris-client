package agent

import (
	"fmt"
	"net"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
	"github.com/orris-inc/orris-client/internal/tunnel"
)

// getHandshakeToken returns the token for tunnel handshake.
// Prefers clientToken from API response, falls back to cfg.Token.
func (a *Agent) getHandshakeToken() string {
	a.rulesMu.RLock()
	token := a.clientToken
	a.rulesMu.RUnlock()
	if token != "" {
		logger.Debug("using clientToken for handshake", "token_prefix", tokenPrefix(token))
		return token
	}
	logger.Debug("using cfg.Token for handshake (clientToken empty)", "token_prefix", tokenPrefix(a.cfg.Token))
	return a.cfg.Token
}

// tokenPrefix returns the first 10 chars of token for logging (safe prefix).
func tokenPrefix(token string) string {
	if len(token) <= 10 {
		return token
	}
	return token[:10] + "..."
}

// getOrCreateTunnel creates a single tunnel connection using TunnelType from rule.
// Returns TunnelClient interface that can be either WS or TLS client.
// For single exit agent scenarios only. Use getOrCreateTunnels for load balancing.
func (a *Agent) getOrCreateTunnel(rule *forward.Rule) (tunnel.TunnelClient, error) {
	a.tunnelsMu.Lock()
	defer a.tunnelsMu.Unlock()

	// Use ruleID as key since each rule needs its own tunnel connection for handshake
	if t, exists := a.tunnels[rule.ID]; exists {
		return t, nil
	}

	// Get the single exit agent ID (priority: NextHopAgentID > ExitAgentID > ExitAgents[0])
	agentID := rule.NextHopAgentID
	if agentID == "" {
		agentID = rule.ExitAgentID
	}
	if agentID == "" && len(rule.ExitAgents) > 0 {
		agentID = rule.ExitAgents[0].AgentID
	}
	if agentID == "" {
		return nil, fmt.Errorf("no exit agent ID specified")
	}

	endpoint, err := a.getExitEndpoint(agentID)
	if err != nil {
		return nil, fmt.Errorf("get exit endpoint: %w", err)
	}

	var t tunnel.TunnelClient

	// Create tunnel based on TunnelType
	if rule.TunnelType.IsTLS() {
		t, err = a.createTLSClient(rule, endpoint.Address, endpoint.TlsPort, agentID)
	} else {
		t, err = a.createWSClient(rule, endpoint.Address, endpoint.WsPort, agentID)
	}
	if err != nil {
		return nil, err
	}

	a.tunnels[rule.ID] = t
	return t, nil
}

// getOrCreateTunnels creates multiple tunnel connections for load balancing.
// Returns tunnels and their corresponding weights.
// Each exit agent gets one tunnel connection.
func (a *Agent) getOrCreateTunnels(rule *forward.Rule) ([]tunnel.TunnelClient, []uint16, error) {
	if len(rule.ExitAgents) == 0 {
		return nil, nil, fmt.Errorf("no exit agents configured for load balancing")
	}

	tunnels := make([]tunnel.TunnelClient, 0, len(rule.ExitAgents))
	weights := make([]uint16, 0, len(rule.ExitAgents))

	for _, agent := range rule.ExitAgents {
		endpoint, err := a.getExitEndpoint(agent.AgentID)
		if err != nil {
			// Clean up already created tunnels on error
			for _, t := range tunnels {
				t.Stop()
			}
			return nil, nil, fmt.Errorf("get exit endpoint for %s: %w", agent.AgentID, err)
		}

		var t tunnel.TunnelClient

		// Create tunnel based on TunnelType
		if rule.TunnelType.IsTLS() {
			t, err = a.createTLSClient(rule, endpoint.Address, endpoint.TlsPort, agent.AgentID)
		} else {
			t, err = a.createWSClient(rule, endpoint.Address, endpoint.WsPort, agent.AgentID)
		}
		if err != nil {
			// Clean up already created tunnels on error
			for _, t := range tunnels {
				t.Stop()
			}
			return nil, nil, fmt.Errorf("create tunnel to %s: %w", agent.AgentID, err)
		}

		tunnels = append(tunnels, t)
		weights = append(weights, agent.Weight)

		logger.Info("tunnel created for load balancing",
			"rule_id", rule.ID,
			"agent_id", agent.AgentID,
			"weight", agent.Weight)
	}

	return tunnels, weights, nil
}

// storeMultiTunnels stores multiple tunnels for a rule using indexed keys.
// Keys are formatted as "ruleID:0", "ruleID:1", etc.
func (a *Agent) storeMultiTunnels(ruleID string, tunnels []tunnel.TunnelClient) {
	a.tunnelsMu.Lock()
	defer a.tunnelsMu.Unlock()

	for i, t := range tunnels {
		key := fmt.Sprintf("%s:%d", ruleID, i)
		a.tunnels[key] = t
	}
}

// cleanupTunnelsForRule stops and removes all tunnels associated with a rule.
// This includes both single tunnel (keyed by ruleID) and multi-tunnels (keyed by "ruleID:0", "ruleID:1", etc.).
// Used to clean up tunnels when forwarder fails to start.
func (a *Agent) cleanupTunnelsForRule(ruleID string) {
	a.tunnelsMu.Lock()
	defer a.tunnelsMu.Unlock()

	// Stop single tunnel if exists
	if t, exists := a.tunnels[ruleID]; exists {
		if stopErr := t.Stop(); stopErr != nil {
			logger.Error("failed to stop tunnel during cleanup", "rule_id", ruleID, "error", stopErr)
		}
		delete(a.tunnels, ruleID)
	}

	// Stop multi-tunnels (indexed keys like "ruleID:0", "ruleID:1", ...)
	prefix := ruleID + ":"
	for key, t := range a.tunnels {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			if stopErr := t.Stop(); stopErr != nil {
				logger.Error("failed to stop tunnel during cleanup", "key", key, "error", stopErr)
			}
			delete(a.tunnels, key)
		}
	}
}

// createWSClient creates a WebSocket tunnel client.
func (a *Agent) createWSClient(rule *forward.Rule, address string, port uint16, agentID string) (*tunnel.Client, error) {
	wsURL := fmt.Sprintf("wss://%s/ws", net.JoinHostPort(address, fmt.Sprintf("%d", port)))

	// Create endpoint refresher to handle exit agent restarts with port changes
	refresher := func() (string, string, error) {
		ep, err := a.getExitEndpoint(agentID)
		if err != nil {
			return "", "", err
		}
		newURL := fmt.Sprintf("wss://%s/ws", net.JoinHostPort(ep.Address, fmt.Sprintf("%d", ep.WsPort)))
		return newURL, a.getHandshakeToken(), nil
	}

	// Build client options
	opts := []tunnel.ClientOption{
		tunnel.WithHeartbeatInterval(30 * time.Second),
		tunnel.WithEndpointRefresher(refresher, 3), // refresh after 3 failed attempts
		tunnel.WithInitialRetry(6),                 // retry up to 6 times for initial connection
	}

	// Use agent's own token for handshake authentication (prefer API-provided token)
	t := tunnel.NewClient(wsURL, a.getHandshakeToken(), rule.ID, opts...)

	if err := t.Start(a.ctx); err != nil {
		return nil, fmt.Errorf("start ws tunnel: %w", err)
	}

	return t, nil
}

// createTLSClient creates a TLS tunnel client using utls.
func (a *Agent) createTLSClient(rule *forward.Rule, address string, port uint16, agentID string) (*tunnel.TLSClient, error) {
	endpoint := net.JoinHostPort(address, fmt.Sprintf("%d", port))

	// Create endpoint refresher to handle exit agent restarts with port changes
	refresher := func() (string, string, error) {
		ep, err := a.getExitEndpoint(agentID)
		if err != nil {
			return "", "", err
		}
		newEndpoint := net.JoinHostPort(ep.Address, fmt.Sprintf("%d", ep.TlsPort))
		return newEndpoint, a.getHandshakeToken(), nil
	}

	// Build client options
	opts := []tunnel.TLSClientOption{
		tunnel.WithTLSHeartbeatInterval(30 * time.Second),
		tunnel.WithTLSEndpointRefresher(refresher, 3), // refresh after 3 failed attempts
		tunnel.WithTLSInitialRetry(6),                 // retry up to 6 times for initial connection
	}

	// Use agent's own token for handshake authentication (prefer API-provided token)
	t := tunnel.NewTLSClient(endpoint, a.getHandshakeToken(), rule.ID, opts...)

	if err := t.Start(a.ctx); err != nil {
		return nil, fmt.Errorf("start tls tunnel: %w", err)
	}

	return t, nil
}

// getOrCreateTunnelByAddress creates a tunnel connection to a specific address.
// Used for chain rules where the next hop address is explicitly provided.
func (a *Agent) getOrCreateTunnelByAddress(rule *forward.Rule) (tunnel.TunnelClient, error) {
	a.tunnelsMu.Lock()
	defer a.tunnelsMu.Unlock()

	// Use ruleID as key since each rule needs its own tunnel connection for handshake
	if t, exists := a.tunnels[rule.ID]; exists {
		return t, nil
	}

	// Use NextHopConnectionToken if available (short-term token for chain authentication),
	// otherwise fall back to agent's own token (prefer API-provided token)
	token := rule.NextHopConnectionToken
	if token == "" {
		logger.Debug("NextHopConnectionToken empty, falling back to handshake token",
			"rule_id", rule.ID, "rule_type", rule.RuleType, "role", rule.Role)
		token = a.getHandshakeToken()
	} else {
		logger.Debug("using NextHopConnectionToken",
			"rule_id", rule.ID, "token_prefix", tokenPrefix(token))
	}

	var t tunnel.TunnelClient
	var err error

	// Create tunnel based on TunnelType
	if rule.TunnelType.IsTLS() {
		t, err = a.createTLSClientByAddress(rule, token)
	} else {
		t, err = a.createWSClientByAddress(rule, token)
	}
	if err != nil {
		return nil, err
	}

	a.tunnels[rule.ID] = t
	return t, nil
}

// createWSClientByAddress creates a WebSocket tunnel client using explicit address.
func (a *Agent) createWSClientByAddress(rule *forward.Rule, token string) (*tunnel.Client, error) {
	wsURL := fmt.Sprintf("wss://%s/ws", net.JoinHostPort(rule.NextHopAddress, fmt.Sprintf("%d", rule.NextHopWsPort)))

	// Create endpoint refresher for chain rules
	ruleID := rule.ID
	refresher := func() (string, string, error) {
		refreshedRule, err := a.client.RefreshRule(a.ctx, ruleID)
		if err != nil {
			return "", "", fmt.Errorf("refresh rule: %w", err)
		}

		// Update local rule cache
		a.updateRuleCache(refreshedRule)

		newURL := fmt.Sprintf("wss://%s/ws",
			net.JoinHostPort(refreshedRule.NextHopAddress, fmt.Sprintf("%d", refreshedRule.NextHopWsPort)))

		// Use refreshed token if available
		newToken := refreshedRule.NextHopConnectionToken
		if newToken == "" {
			newToken = a.getHandshakeToken()
		}

		logger.Info("endpoint refreshed for chain rule",
			"rule_id", ruleID,
			"new_endpoint", newURL)

		return newURL, newToken, nil
	}

	// Build client options
	opts := []tunnel.ClientOption{
		tunnel.WithHeartbeatInterval(30 * time.Second),
		tunnel.WithEndpointRefresher(refresher, 3), // refresh after 3 failed attempts
		tunnel.WithInitialRetry(6),                 // retry up to 6 times for initial connection
	}

	t := tunnel.NewClient(wsURL, token, rule.ID, opts...)

	if err := t.Start(a.ctx); err != nil {
		return nil, fmt.Errorf("start ws tunnel: %w", err)
	}

	return t, nil
}

// createTLSClientByAddress creates a TLS tunnel client using explicit address.
func (a *Agent) createTLSClientByAddress(rule *forward.Rule, token string) (*tunnel.TLSClient, error) {
	endpoint := net.JoinHostPort(rule.NextHopAddress, fmt.Sprintf("%d", rule.NextHopTlsPort))

	// Create endpoint refresher for chain rules
	ruleID := rule.ID
	refresher := func() (string, string, error) {
		refreshedRule, err := a.client.RefreshRule(a.ctx, ruleID)
		if err != nil {
			return "", "", fmt.Errorf("refresh rule: %w", err)
		}

		// Update local rule cache
		a.updateRuleCache(refreshedRule)

		newEndpoint := net.JoinHostPort(refreshedRule.NextHopAddress, fmt.Sprintf("%d", refreshedRule.NextHopTlsPort))

		// Use refreshed token if available
		newToken := refreshedRule.NextHopConnectionToken
		if newToken == "" {
			newToken = a.getHandshakeToken()
		}

		logger.Info("tls endpoint refreshed for chain rule",
			"rule_id", ruleID,
			"new_endpoint", newEndpoint)

		return newEndpoint, newToken, nil
	}

	// Build client options
	opts := []tunnel.TLSClientOption{
		tunnel.WithTLSHeartbeatInterval(30 * time.Second),
		tunnel.WithTLSEndpointRefresher(refresher, 3), // refresh after 3 failed attempts
		tunnel.WithTLSInitialRetry(6),                 // retry up to 6 times for initial connection
	}

	t := tunnel.NewTLSClient(endpoint, token, rule.ID, opts...)

	if err := t.Start(a.ctx); err != nil {
		return nil, fmt.Errorf("start tls tunnel: %w", err)
	}

	return t, nil
}

// updateRuleCache updates the local rule cache with refreshed rule data.
func (a *Agent) updateRuleCache(rule *forward.Rule) {
	a.rulesMu.Lock()
	defer a.rulesMu.Unlock()

	for i := range a.rules {
		if a.rules[i].ID == rule.ID {
			a.rules[i] = *rule
			logger.Debug("rule cache updated", "rule_id", rule.ID)
			return
		}
	}
}

// ensureTunnelServer ensures the WebSocket tunnel server is started.
// If WsListenPort is 0, a random available port will be used.
func (a *Agent) ensureTunnelServer() error {
	a.tunnelServerMu.Lock()
	defer a.tunnelServerMu.Unlock()

	if a.tunnelServer != nil {
		return nil
	}

	// Pass forward.Client for server-side handshake verification
	server := tunnel.NewServer(a.cfg.WsListenPort, a.client.ForwardClient(), a.rules)
	if err := server.Start(a.ctx); err != nil {
		return fmt.Errorf("start ws tunnel server: %w", err)
	}

	a.tunnelServer = server

	// Update config with actual port (important when port was 0)
	a.cfg.WsListenPort = a.tunnelServer.Port()

	// Immediately report status with new port so entry agents can reconnect
	go a.reportStatus()

	return nil
}

// ensureTlsTunnelServer ensures the TLS tunnel server is started.
// If TlsListenPort is 0, a random available port will be used.
func (a *Agent) ensureTlsTunnelServer() error {
	a.tlsTunnelServerMu.Lock()
	defer a.tlsTunnelServerMu.Unlock()

	if a.tlsTunnelServer != nil {
		return nil
	}

	// Pass forward.Client for server-side handshake verification
	server := tunnel.NewTLSServer(a.cfg.TlsListenPort, a.client.ForwardClient(), a.rules)
	if err := server.Start(a.ctx); err != nil {
		return fmt.Errorf("start tls tunnel server: %w", err)
	}

	a.tlsTunnelServer = server

	// Update config with actual port (important when port was 0)
	a.cfg.TlsListenPort = a.tlsTunnelServer.Port()

	// Immediately report status with new port so entry agents can reconnect
	go a.reportStatus()

	return nil
}

// ensureTunnelServerByType ensures the appropriate tunnel server is started based on TunnelType.
func (a *Agent) ensureTunnelServerByType(tunnelType forward.TunnelType) error {
	if tunnelType.IsTLS() {
		return a.ensureTlsTunnelServer()
	}
	return a.ensureTunnelServer()
}

// getTunnelServer returns the appropriate tunnel server based on TunnelType.
func (a *Agent) getTunnelServer(tunnelType forward.TunnelType) tunnel.TunnelServer {
	if tunnelType.IsTLS() {
		return a.tlsTunnelServer
	}
	return a.tunnelServer
}

// createSmuxClient creates a SMUX tunnel client.
// For tls_smux: endpoint is "host:port"
// For ws_smux: endpoint is "wss://host:port/ws"
func (a *Agent) createSmuxClient(rule *forward.Rule, address string, port uint16, agentID string) (*tunnel.SmuxClient, error) {
	useTLS := rule.TunnelType.IsTLSSmux()

	var endpoint string
	if useTLS {
		endpoint = net.JoinHostPort(address, fmt.Sprintf("%d", port))
	} else {
		endpoint = fmt.Sprintf("wss://%s/ws", net.JoinHostPort(address, fmt.Sprintf("%d", port)))
	}

	// Create endpoint refresher
	refresher := func() (string, string, error) {
		ep, err := a.getExitEndpoint(agentID)
		if err != nil {
			return "", "", err
		}
		var newEndpoint string
		if useTLS {
			newEndpoint = net.JoinHostPort(ep.Address, fmt.Sprintf("%d", ep.TlsPort))
		} else {
			newEndpoint = fmt.Sprintf("wss://%s/ws", net.JoinHostPort(ep.Address, fmt.Sprintf("%d", ep.WsPort)))
		}
		return newEndpoint, a.getHandshakeToken(), nil
	}

	opts := []tunnel.SmuxClientOption{
		tunnel.WithSmuxEndpointRefresher(refresher, 3),
		tunnel.WithSmuxInitialRetry(6),
	}

	client := tunnel.NewSmuxClient(endpoint, a.getHandshakeToken(), rule.ID, useTLS, opts...)

	if err := client.Start(a.ctx); err != nil {
		return nil, fmt.Errorf("start smux tunnel: %w", err)
	}

	return client, nil
}

// getOrCreateSmuxClient creates a SMUX client for the rule.
func (a *Agent) getOrCreateSmuxClient(rule *forward.Rule) (tunnel.SmuxTunnelClient, error) {
	// Get the exit agent ID
	agentID := rule.NextHopAgentID
	if agentID == "" {
		agentID = rule.ExitAgentID
	}
	if agentID == "" && len(rule.ExitAgents) > 0 {
		agentID = rule.ExitAgents[0].AgentID
	}
	if agentID == "" {
		return nil, fmt.Errorf("no exit agent ID specified")
	}

	endpoint, err := a.getExitEndpoint(agentID)
	if err != nil {
		return nil, fmt.Errorf("get exit endpoint: %w", err)
	}

	// Use TLS port for tls_smux, WS port for ws_smux
	port := endpoint.WsPort
	if rule.TunnelType.IsTLSSmux() {
		port = endpoint.TlsPort
	}

	return a.createSmuxClient(rule, endpoint.Address, port, agentID)
}

// ensureTunnelServerByType now handles both regular tunnels and SMUX tunnels.
// For SMUX tunnel types (ws_smux, tls_smux), it uses the same server as regular tunnels
// since SMUX support is now integrated into Server and TLSServer.
// The EnableSmux field in handshake distinguishes between the two protocols.

// createSmuxClientByAddress creates a SMUX client using explicit NextHopAddress from rule.
// Used for chain rules where the next hop address is explicitly provided.
func (a *Agent) createSmuxClientByAddress(rule *forward.Rule) (*tunnel.SmuxClient, error) {
	useTLS := rule.TunnelType.IsTLSSmux()

	var endpoint string
	var port uint16
	if useTLS {
		port = rule.NextHopTlsPort
		endpoint = net.JoinHostPort(rule.NextHopAddress, fmt.Sprintf("%d", port))
	} else {
		port = rule.NextHopWsPort
		endpoint = fmt.Sprintf("wss://%s/ws", net.JoinHostPort(rule.NextHopAddress, fmt.Sprintf("%d", port)))
	}

	// Use NextHopConnectionToken if available, otherwise fall back to handshake token
	token := rule.NextHopConnectionToken
	if token == "" {
		logger.Debug("NextHopConnectionToken empty for smux, falling back to handshake token",
			"rule_id", rule.ID, "rule_type", rule.RuleType, "role", rule.Role)
		token = a.getHandshakeToken()
	} else {
		logger.Debug("using NextHopConnectionToken for smux",
			"rule_id", rule.ID, "token_prefix", tokenPrefix(token))
	}

	// Create endpoint refresher for chain rules
	ruleID := rule.ID
	refresher := func() (string, string, error) {
		refreshedRule, err := a.client.RefreshRule(a.ctx, ruleID)
		if err != nil {
			return "", "", fmt.Errorf("refresh rule: %w", err)
		}

		// Update local rule cache
		a.updateRuleCache(refreshedRule)

		var newEndpoint string
		if useTLS {
			newEndpoint = net.JoinHostPort(refreshedRule.NextHopAddress, fmt.Sprintf("%d", refreshedRule.NextHopTlsPort))
		} else {
			newEndpoint = fmt.Sprintf("wss://%s/ws",
				net.JoinHostPort(refreshedRule.NextHopAddress, fmt.Sprintf("%d", refreshedRule.NextHopWsPort)))
		}

		// Use refreshed token if available
		newToken := refreshedRule.NextHopConnectionToken
		if newToken == "" {
			newToken = a.getHandshakeToken()
		}

		logger.Info("smux endpoint refreshed for chain rule",
			"rule_id", ruleID,
			"new_endpoint", newEndpoint)

		return newEndpoint, newToken, nil
	}

	opts := []tunnel.SmuxClientOption{
		tunnel.WithSmuxEndpointRefresher(refresher, 3),
		tunnel.WithSmuxInitialRetry(6),
	}

	client := tunnel.NewSmuxClient(endpoint, token, rule.ID, useTLS, opts...)

	if err := client.Start(a.ctx); err != nil {
		return nil, fmt.Errorf("start smux tunnel: %w", err)
	}

	return client, nil
}
