package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/orris-inc/orris-client/internal/config"
	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/rulecache"
)

func TestGetExitEndpointSuccessUpdatesCache(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data": map[string]any{
				"address":  "5.6.7.8",
				"ws_port":  1234,
				"tls_port": 5678,
			},
		})
	}))
	defer srv.Close()

	cfg := config.DefaultConfig()
	cfg.ServerURL = srv.URL
	cfg.Token = "test-token"

	a := New(cfg)
	a.ctx = context.Background()

	endpoint, err := a.getExitEndpoint("fa_1")
	if err != nil {
		t.Fatalf("getExitEndpoint() error = %v", err)
	}
	if endpoint.Address != "5.6.7.8" || endpoint.WsPort != 1234 {
		t.Fatalf("endpoint = %+v, want {5.6.7.8 1234 5678}", endpoint)
	}

	a.endpointCacheMu.RLock()
	cached, ok := a.endpointCache["fa_1"]
	a.endpointCacheMu.RUnlock()
	if !ok || cached.Address != "5.6.7.8" {
		t.Fatalf("endpointCache[fa_1] = %+v, ok=%v, want cached entry", cached, ok)
	}
}

func TestGetExitEndpointFallsBackToCacheOnFailure(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ServerURL = "http://127.0.0.1:1" // connection refused, deterministic
	cfg.Token = "test-token"
	cfg.HTTPTimeout = 2 * time.Second

	a := New(cfg)
	a.ctx = context.Background()

	a.endpointCacheMu.Lock()
	a.endpointCache["fa_1"] = forward.ExitEndpoint{Address: "9.9.9.9", WsPort: 4242}
	a.endpointCacheMu.Unlock()

	endpoint, err := a.getExitEndpoint("fa_1")
	if err != nil {
		t.Fatalf("getExitEndpoint() error = %v, want nil (should fall back to cache)", err)
	}
	if endpoint.Address != "9.9.9.9" || endpoint.WsPort != 4242 {
		t.Fatalf("endpoint = %+v, want {9.9.9.9 4242 0}", endpoint)
	}
}

func TestGetExitEndpointReturnsErrorWhenNoCache(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ServerURL = "http://127.0.0.1:1" // connection refused, deterministic
	cfg.Token = "test-token"
	cfg.HTTPTimeout = 2 * time.Second

	a := New(cfg)
	a.ctx = context.Background()

	if _, err := a.getExitEndpoint("fa_unknown"); err == nil {
		t.Fatal("getExitEndpoint() error = nil, want error when no cache entry exists")
	}
}

func TestStartFallsBackToCacheWhenSyncFails(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ORRIS_CONFIG_FILE", filepath.Join(dir, "client.env"))

	snap := &rulecache.Snapshot{
		Rules: []forward.Rule{{
			ID:            "fr_cached",
			RuleType:      forward.RuleTypeDirect,
			Protocol:      "tcp",
			TargetAddress: "127.0.0.1",
			TargetPort:    9,
			ListenPort:    0,
		}},
		ClientToken: "fwd_cached_token",
		SavedAt:     1,
	}
	if err := rulecache.Save(snap); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.ServerURL = "http://127.0.0.1:1" // connection refused, deterministic
	cfg.Token = "test-token"
	cfg.HTTPTimeout = 2 * time.Second

	a := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := a.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v, want nil (should fall back to cache)", err)
	}
	defer a.Stop()

	a.forwardersMu.RLock()
	_, ok := a.forwarders["fr_cached"]
	count := len(a.forwarders)
	a.forwardersMu.RUnlock()

	if !ok {
		t.Fatalf("forwarders = %d entries, missing fr_cached", count)
	}

	a.rulesMu.RLock()
	token := a.clientToken
	a.rulesMu.RUnlock()
	if token != "fwd_cached_token" {
		t.Errorf("clientToken = %q, want fwd_cached_token", token)
	}
}

func TestStartFromCacheContinuesAfterOneRuleFails(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ORRIS_CONFIG_FILE", filepath.Join(dir, "client.env"))

	snap := &rulecache.Snapshot{
		Rules: []forward.Rule{
			{ID: "fr_bad", RuleType: "bogus_type"},
			{
				ID:            "fr_good",
				RuleType:      forward.RuleTypeDirect,
				Protocol:      "tcp",
				TargetAddress: "127.0.0.1",
				TargetPort:    9,
				ListenPort:    0,
			},
		},
		ClientToken:      "fwd_cached_token",
		BlockedProtocols: []string{"udp"},
		Endpoints:        map[string]forward.ExitEndpoint{"fa_1": {Address: "2.2.2.2", WsPort: 500}},
		SavedAt:          1,
	}
	if err := rulecache.Save(snap); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.ServerURL = "http://127.0.0.1:1" // connection refused, deterministic
	cfg.Token = "test-token"
	cfg.HTTPTimeout = 2 * time.Second

	a := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := a.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v, want nil (one bad rule should not abort startup)", err)
	}
	defer a.Stop()

	a.forwardersMu.RLock()
	_, goodOK := a.forwarders["fr_good"]
	_, badOK := a.forwarders["fr_bad"]
	a.forwardersMu.RUnlock()

	if !goodOK {
		t.Error("forwarders[fr_good] missing, want started")
	}
	if badOK {
		t.Error("forwarders[fr_bad] present, want not started (unknown rule type)")
	}

	a.rulesMu.RLock()
	blocked := a.blockedProtocols
	a.rulesMu.RUnlock()
	if len(blocked) != 1 || blocked[0] != "udp" {
		t.Errorf("blockedProtocols = %v, want [udp]", blocked)
	}

	a.endpointCacheMu.RLock()
	ep, ok := a.endpointCache["fa_1"]
	a.endpointCacheMu.RUnlock()
	if !ok || ep.Address != "2.2.2.2" {
		t.Errorf("endpointCache[fa_1] = %+v, ok=%v, want cached entry", ep, ok)
	}
}

func TestStartFailsWhenSyncFailsAndNoCache(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ORRIS_CONFIG_FILE", filepath.Join(dir, "client.env"))

	cfg := config.DefaultConfig()
	cfg.ServerURL = "http://127.0.0.1:1" // connection refused, deterministic
	cfg.Token = "test-token"
	cfg.HTTPTimeout = 2 * time.Second

	a := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := a.Start(ctx); err == nil {
		t.Fatal("Start() error = nil, want error when sync fails and no cache exists")
	}
}
