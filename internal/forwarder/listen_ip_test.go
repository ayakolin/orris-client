package forwarder

import (
	"context"
	"net"
	"testing"

	"github.com/orris-inc/orris-client/internal/forward"
)

func TestDirectForwarderBindsTCPToConfiguredListenIP(t *testing.T) {
	rule := &forward.Rule{
		ID:            "fr_listen_ip",
		RuleType:      forward.RuleTypeDirect,
		ListenIP:      "127.0.0.1",
		ListenPort:    0,
		TargetAddress: "192.0.2.1",
		TargetPort:    80,
		Protocol:      "tcp",
	}

	f := NewDirectForwarder(rule)
	if err := f.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer f.Stop()

	addr, ok := f.tcpListener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T, want *net.TCPAddr", f.tcpListener.Addr())
	}
	if !addr.IP.Equal(net.ParseIP("127.0.0.1")) {
		t.Fatalf("listener IP = %s, want 127.0.0.1", addr.IP)
	}
	if got := f.ListenIP(); got != "127.0.0.1" {
		t.Fatalf("ListenIP() = %q, want 127.0.0.1", got)
	}
}
