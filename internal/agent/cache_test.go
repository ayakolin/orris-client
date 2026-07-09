package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/orris-inc/orris-client/internal/config"
	"github.com/orris-inc/orris-client/internal/forward"
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
