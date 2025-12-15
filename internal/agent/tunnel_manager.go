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

func (a *Agent) getOrCreateTunnel(rule *forward.Rule) (*tunnel.Client, error) {
	a.tunnelsMu.Lock()
	defer a.tunnelsMu.Unlock()

	// Use ruleID as key since each rule needs its own tunnel connection for handshake
	if t, exists := a.tunnels[rule.ID]; exists {
		return t, nil
	}

	// Use NextHopAgentID to get endpoint (ExitAgentID is deprecated in RuleSyncData)
	agentID := rule.NextHopAgentID
	if agentID == "" {
		agentID = rule.ExitAgentID // fallback for legacy API response
	}
	if agentID == "" {
		return nil, fmt.Errorf("no next hop agent ID specified")
	}

	endpoint, err := a.client.GetExitEndpoint(a.ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("get exit endpoint: %w", err)
	}

	wsURL := fmt.Sprintf("ws://%s/tunnel", net.JoinHostPort(endpoint.Address, fmt.Sprintf("%d", endpoint.WsPort)))

	// Create endpoint refresher to handle exit agent restarts with port changes
	refresher := func() (string, string, error) {
		ep, err := a.client.GetExitEndpoint(a.ctx, agentID)
		if err != nil {
			return "", "", err
		}
		newURL := fmt.Sprintf("ws://%s/tunnel", net.JoinHostPort(ep.Address, fmt.Sprintf("%d", ep.WsPort)))
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
		return nil, fmt.Errorf("start tunnel: %w", err)
	}

	a.tunnels[rule.ID] = t
	return t, nil
}

// getOrCreateTunnelByAddress creates a tunnel connection to a specific address.
// Used for chain rules where the next hop address is explicitly provided.
func (a *Agent) getOrCreateTunnelByAddress(rule *forward.Rule) (*tunnel.Client, error) {
	a.tunnelsMu.Lock()
	defer a.tunnelsMu.Unlock()

	// Use ruleID as key since each rule needs its own tunnel connection for handshake
	if t, exists := a.tunnels[rule.ID]; exists {
		return t, nil
	}

	wsURL := fmt.Sprintf("ws://%s/tunnel", net.JoinHostPort(rule.NextHopAddress, fmt.Sprintf("%d", rule.NextHopWsPort)))

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

	// Create endpoint refresher for chain rules
	ruleID := rule.ID
	refresher := func() (string, string, error) {
		refreshedRule, err := a.client.RefreshRule(a.ctx, ruleID)
		if err != nil {
			return "", "", fmt.Errorf("refresh rule: %w", err)
		}

		// Update local rule cache
		a.updateRuleCache(refreshedRule)

		newURL := fmt.Sprintf("ws://%s/tunnel",
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
		return nil, fmt.Errorf("start tunnel: %w", err)
	}

	a.tunnels[rule.ID] = t
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

// ensureTunnelServer ensures the tunnel server is started.
// If WsListenPort is 0, a random available port will be used.
func (a *Agent) ensureTunnelServer() error {
	if a.tunnelServer != nil {
		return nil
	}

	// Pass forward.Client for server-side handshake verification
	a.tunnelServer = tunnel.NewServer(a.cfg.WsListenPort, a.client.ForwardClient(), a.rules)
	if err := a.tunnelServer.Start(a.ctx); err != nil {
		return fmt.Errorf("start tunnel server: %w", err)
	}

	// Update config with actual port (important when port was 0)
	a.cfg.WsListenPort = a.tunnelServer.Port()

	// Immediately report status with new port so entry agents can reconnect
	go a.reportStatus()

	return nil
}
