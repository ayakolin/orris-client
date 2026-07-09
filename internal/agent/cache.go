package agent

import (
	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
)

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
