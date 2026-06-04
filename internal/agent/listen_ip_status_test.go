package agent

import (
	"context"
	"testing"

	"github.com/orris-inc/orris-client/internal/config"
	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/forwarder"
)

type fakeStatusForwarder struct {
	ruleID      string
	listenIP    string
	listenPort  uint16
	connections int
}

func (f *fakeStatusForwarder) Start(context.Context) error        { return nil }
func (f *fakeStatusForwarder) Stop() error                        { return nil }
func (f *fakeStatusForwarder) Traffic() *forwarder.TrafficCounter { return &forwarder.TrafficCounter{} }
func (f *fakeStatusForwarder) RuleID() string                     { return f.ruleID }
func (f *fakeStatusForwarder) ListenIP() string                   { return f.listenIP }
func (f *fakeStatusForwarder) ListenPort() uint16                 { return f.listenPort }
func (f *fakeStatusForwarder) Connections() int                   { return f.connections }

func TestCollectRuleSyncStatusIncludesListenIP(t *testing.T) {
	a := New(config.DefaultConfig())
	a.forwarders["fr_listen_ip"] = &fakeStatusForwarder{
		ruleID:      "fr_listen_ip",
		listenIP:    "127.0.0.1",
		listenPort:  13306,
		connections: 2,
	}
	a.ruleStatus["fr_listen_ip"] = &ruleStatus{
		syncStatus: forward.SyncStatusSynced,
		runStatus:  forward.RunStatusRunning,
		syncedAt:   123,
	}

	items := a.collectRuleSyncStatus()
	if len(items) != 1 {
		t.Fatalf("collectRuleSyncStatus() length = %d, want 1", len(items))
	}
	if got := items[0].ListenIP; got != "127.0.0.1" {
		t.Fatalf("ListenIP = %q, want 127.0.0.1", got)
	}
	if got := items[0].ListenPort; got != 13306 {
		t.Fatalf("ListenPort = %d, want 13306", got)
	}
}

func TestRuleSyncDataToRulePreservesListenIP(t *testing.T) {
	rule := ruleSyncDataToRule(&forward.RuleSyncData{
		ID:         "fr_listen_ip",
		RuleType:   string(forward.RuleTypeDirect),
		ListenIP:   "127.0.0.1",
		ListenPort: 13306,
		Protocol:   "tcp",
	})

	if got := rule.ListenIP; got != "127.0.0.1" {
		t.Fatalf("ListenIP = %q, want 127.0.0.1", got)
	}
}
