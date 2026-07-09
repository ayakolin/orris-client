package agent

import (
	"fmt"
	"time"

	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
	"github.com/orris-inc/orris-client/internal/rulecache"
)

// startFromCache loads the local rule cache and starts forwarders from it.
// It is used as a fallback when the initial sync to the control server
// fails, so the agent can keep serving previously known rules instead of
// refusing to start. A single rule failing to start does not abort the rest.
func (a *Agent) startFromCache() error {
	snap, err := rulecache.Load()
	if err != nil {
		return fmt.Errorf("load rule cache: %w", err)
	}

	a.rulesMu.Lock()
	a.rules = snap.Rules
	a.clientToken = snap.ClientToken
	a.blockedProtocols = snap.BlockedProtocols
	a.rulesMu.Unlock()

	a.endpointCacheMu.Lock()
	a.endpointCache = make(map[string]forward.ExitEndpoint, len(snap.Endpoints))
	for id, ep := range snap.Endpoints {
		a.endpointCache[id] = ep
	}
	a.endpointCacheMu.Unlock()

	logger.Warn("using cached rules to start, control server is unreachable",
		"rule_count", len(snap.Rules),
		"cached_at", time.Unix(snap.SavedAt, 0).Format(time.RFC3339))

	for i := range snap.Rules {
		rule := snap.Rules[i]
		if err := a.startForwarder(&rule); err != nil {
			logger.Error("start forwarder from cached rule failed", "rule_id", rule.ID, "error", err)
		}
	}

	return nil
}

// getExitEndpoint resolves the connection endpoint for an exit agent.
// On success it updates the local endpoint cache and returns the fresh result.
// On failure it falls back to the last known-good cached endpoint, if any, so
// tunnels to previously reachable exit agents can still be established while
// the control server is unreachable.
func (a *Agent) getExitEndpoint(agentID string) (*forward.ExitEndpoint, error) {
	endpoint, err := a.client.GetExitEndpoint(a.ctx, agentID)
	if err == nil {
		a.endpointCacheMu.Lock()
		a.endpointCache[agentID] = *endpoint
		a.endpointCacheMu.Unlock()
		return endpoint, nil
	}

	a.endpointCacheMu.RLock()
	cached, ok := a.endpointCache[agentID]
	a.endpointCacheMu.RUnlock()

	if !ok {
		return nil, err
	}

	logger.Warn("control server unreachable, using cached exit endpoint",
		"agent_id", agentID, "address", cached.Address, "error", err)
	return &cached, nil
}
