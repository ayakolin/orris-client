# 主控机不可达时的规则本地缓存兜底 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 让 `orris-client` 在启动/重启时如果连不上主控机（orris server），能够用本地缓存的"最后一次成功同步"的规则和出口端点信息继续拉起转发器，而不是直接失败退出、被 systemd `Restart=always` 拖入死循环（详见设计文档 `docs/superpowers/specs/2026-07-09-master-unreachable-rule-cache-design.md`）。

**架构：**
- 新增 `internal/rulecache` 包，负责本地缓存文件的原子读写。缓存文件路径 = 当前 Agent 的配置文件路径（`config.ConfigFilePath()`）+ `.rules_cache.json` 后缀（而不是配置目录下的固定文件名——`install.sh` 支持同一台主机部署多个 Agent 实例，各自有独立的 `client.env` / `client-<NAME>.env`，固定文件名会导致多实例互相覆盖对方的规则缓存）。
- `Agent` 新增内存态的出口端点缓存 `endpointCache`，以及一个防抖持久化协程 `cachePersistLoop`，在规则/端点每次成功刷新后异步落盘。
- `Agent.Start()` 在初始同步失败时改为调用 `startFromCache()` 用缓存兜底启动，而不是直接返回错误。
- `tunnel_manager.go` 里所有直接解析出口 Agent 地址的调用（6 处）统一改走带缓存兜底的 `getExitEndpoint`。
- `scripts/install.sh` 卸载时同步清理规则缓存文件，避免残留含 token 的文件。

**技术栈：** Go 1.24，标准库 `testing`（本仓库不使用 testify），标准库 `net/http/httptest`。

**验证说明：** 本计划里的每一段代码都已经在临时 worktree 中实际写入、编译、跑过 `go build`/`go vet`/`go test`/`go test -race`，全部通过（包括新增的多实例路径隔离测试）。按顺序照抄执行即可。

---

### 任务 1：`internal/rulecache` 包

**文件：**
- 创建：`internal/rulecache/rulecache.go`
- 测试：`internal/rulecache/rulecache_test.go`

- [ ] **步骤 1：编写失败的测试**

创建 `internal/rulecache/rulecache_test.go`：

```go
package rulecache

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/orris-inc/orris-client/internal/forward"
)

// setConfigFile points ORRIS_CONFIG_FILE at a fresh temp file for the
// duration of the test, so FilePath() resolves to an isolated location.
func setConfigFile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "client.env")
	t.Setenv("ORRIS_CONFIG_FILE", configPath)
	return configPath
}

func TestFilePathIsSuffixedConfigPath(t *testing.T) {
	configPath := setConfigFile(t)

	want := configPath + cacheSuffix
	if got := FilePath(); got != want {
		t.Fatalf("FilePath() = %q, want %q", got, want)
	}
}

func TestFilePathDoesNotCollideAcrossInstances(t *testing.T) {
	dir := t.TempDir()

	t.Setenv("ORRIS_CONFIG_FILE", filepath.Join(dir, "client.env"))
	primary := FilePath()

	t.Setenv("ORRIS_CONFIG_FILE", filepath.Join(dir, "client-second.env"))
	secondary := FilePath()

	if primary == secondary {
		t.Fatalf("FilePath() collided across instances: %q == %q", primary, secondary)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	setConfigFile(t)

	snap := &Snapshot{
		Rules:            []forward.Rule{{ID: "fr_1", RuleType: forward.RuleTypeDirect, Protocol: "tcp"}},
		ClientToken:      "fwd_abc",
		BlockedProtocols: []string{"udp"},
		Endpoints:        map[string]forward.ExitEndpoint{"fa_1": {Address: "1.2.3.4", WsPort: 9000}},
		SavedAt:          1234567890,
	}

	if err := Save(snap); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	got, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if len(got.Rules) != 1 || got.Rules[0].ID != "fr_1" {
		t.Fatalf("Rules = %+v, want 1 rule with ID fr_1", got.Rules)
	}
	if got.ClientToken != "fwd_abc" {
		t.Errorf("ClientToken = %q, want fwd_abc", got.ClientToken)
	}
	if len(got.BlockedProtocols) != 1 || got.BlockedProtocols[0] != "udp" {
		t.Errorf("BlockedProtocols = %v, want [udp]", got.BlockedProtocols)
	}
	ep, ok := got.Endpoints["fa_1"]
	if !ok || ep.Address != "1.2.3.4" || ep.WsPort != 9000 {
		t.Errorf("Endpoints[fa_1] = %+v, want {1.2.3.4 9000 0}", ep)
	}
}

func TestLoadMissingFile(t *testing.T) {
	setConfigFile(t)

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error for missing file")
	}
}

func TestLoadCorruptedFile(t *testing.T) {
	setConfigFile(t)

	if err := os.WriteFile(FilePath(), []byte("not json"), 0600); err != nil {
		t.Fatalf("setup write: %v", err)
	}

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error for corrupted file")
	}
}

func TestLoadEmptyRules(t *testing.T) {
	setConfigFile(t)

	if err := Save(&Snapshot{Rules: nil, SavedAt: 1}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error for empty rule set")
	}
}

func TestSaveRejectsSymlink(t *testing.T) {
	setConfigFile(t)

	target := FilePath() + ".real"
	if err := os.WriteFile(target, []byte("{}"), 0600); err != nil {
		t.Fatalf("setup write: %v", err)
	}
	if err := os.Symlink(target, FilePath()); err != nil {
		t.Fatalf("setup symlink: %v", err)
	}

	err := Save(&Snapshot{Rules: []forward.Rule{{ID: "fr_1"}}, SavedAt: 1})
	if !errors.Is(err, ErrSymlinkNotAllowed) {
		t.Fatalf("Save() error = %v, want ErrSymlinkNotAllowed", err)
	}
}

func TestSaveFilePermissions(t *testing.T) {
	setConfigFile(t)

	if err := Save(&Snapshot{Rules: []forward.Rule{{ID: "fr_1"}}, SavedAt: 1}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	info, err := os.Stat(FilePath())
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("file permissions = %o, want 0600", perm)
	}
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`cd /home/rinx/Codespace/orris-client && go test ./internal/rulecache/... -v`

预期：编译失败（`internal/rulecache` 包还没有 `rulecache.go`），报错类似 `undefined: Save`、`undefined: Load`、`undefined: Snapshot`、`undefined: ErrSymlinkNotAllowed`、`undefined: cacheSuffix`。

- [ ] **步骤 3：编写实现代码**

创建 `internal/rulecache/rulecache.go`：

```go
// Package rulecache persists the agent's last successfully synced rule set to
// local disk, so the agent can keep serving previously known rules when it
// cannot reach the control server (e.g. on startup after a restart).
package rulecache

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/orris-inc/orris-client/internal/config"
	"github.com/orris-inc/orris-client/internal/forward"
)

// cacheSuffix is appended to the agent's config file path to derive the rule
// cache file path. Suffixing (rather than a fixed filename in the config
// directory) keeps multi-instance installs from colliding: each instance has
// its own config file (client.env, client-<name>.env, ...) under the same
// directory (see scripts/install.sh), so each instance gets its own cache
// file too.
const cacheSuffix = ".rules_cache.json"

// Snapshot is the persisted view of an agent's last successfully synced state.
type Snapshot struct {
	Rules            []forward.Rule                  `json:"rules"`
	ClientToken      string                           `json:"client_token,omitempty"`
	BlockedProtocols []string                         `json:"blocked_protocols,omitempty"`
	Endpoints        map[string]forward.ExitEndpoint `json:"endpoints,omitempty"`
	SavedAt          int64                            `json:"saved_at"`
}

// ErrSymlinkNotAllowed is returned when the cache file path is a symlink.
var ErrSymlinkNotAllowed = errors.New("rule cache file cannot be a symlink")

// FilePath returns the path to this agent's local rule cache file: the
// agent's config file path (config.ConfigFilePath()) with cacheSuffix
// appended.
func FilePath() string {
	return config.ConfigFilePath() + cacheSuffix
}

// Load reads and decodes the rule cache file. It returns an error if the file
// does not exist, cannot be read, cannot be parsed, or contains no rules.
func Load() (*Snapshot, error) {
	path := FilePath()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read rule cache: %w", err)
	}

	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("parse rule cache: %w", err)
	}

	if len(snap.Rules) == 0 {
		return nil, fmt.Errorf("rule cache is empty")
	}

	return &snap, nil
}

// Save atomically writes the snapshot to the rule cache file (temp file +
// rename). The file is written with 0600 permissions since it may contain a
// client token.
func Save(snap *Snapshot) error {
	path := FilePath()

	// Reject symlinks to prevent symlink attacks (mirrors config.SaveServerURL).
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return ErrSymlinkNotAllowed
		}
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal rule cache: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("write temp rule cache: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename rule cache: %w", err)
	}

	return nil
}
```

- [ ] **步骤 4：运行测试验证通过**

运行：`go test ./internal/rulecache/... -v`

预期：全部 8 个测试 PASS：

```
--- PASS: TestFilePathIsSuffixedConfigPath
--- PASS: TestFilePathDoesNotCollideAcrossInstances
--- PASS: TestSaveLoadRoundTrip
--- PASS: TestLoadMissingFile
--- PASS: TestLoadCorruptedFile
--- PASS: TestLoadEmptyRules
--- PASS: TestSaveRejectsSymlink
--- PASS: TestSaveFilePermissions
PASS
ok  	github.com/orris-inc/orris-client/internal/rulecache
```

- [ ] **步骤 5：格式与静态检查**

运行：`gofmt -l internal/rulecache/ && go vet ./internal/rulecache/...`

预期：`gofmt -l` 无输出（无需格式化），`go vet` 无输出无错误。

- [ ] **步骤 6：Commit**

```bash
git add internal/rulecache/rulecache.go internal/rulecache/rulecache_test.go
git commit -m "feat: add local rule cache package for control-server-unreachable fallback"
```

---

### 任务 2：Agent 出口端点缓存 + `getExitEndpoint`

**文件：**
- 修改：`internal/agent/agent.go`
- 创建：`internal/agent/cache.go`
- 创建：`internal/agent/cache_test.go`

- [ ] **步骤 1：在 `Agent` 结构体中新增出口端点缓存字段**

修改 `internal/agent/agent.go`，找到：

```go
	tunnelsMu sync.RWMutex
	tunnels   map[string]tunnel.TunnelClient // ruleID -> tunnel (WS or TLS)

	// Health check configurations for load balancing failover
```

替换为：

```go
	tunnelsMu sync.RWMutex
	tunnels   map[string]tunnel.TunnelClient // ruleID -> tunnel (WS or TLS)

	// Cache of resolved exit agent endpoints. Used as a fallback when the
	// control server is unreachable and a new tunnel needs to be established
	// (see getExitEndpoint).
	endpointCacheMu sync.RWMutex
	endpointCache   map[string]forward.ExitEndpoint // agentID -> last resolved endpoint

	// Health check configurations for load balancing failover
```

- [ ] **步骤 2：在 `New()` 中初始化该字段**

在同一文件中找到：

```go
		forwarders:         make(map[string]forwarder.Forwarder),
		tunnels:            make(map[string]tunnel.TunnelClient),
		healthCheckConfigs: make(map[string]*forward.HealthCheckConfig),
```

替换为：

```go
		forwarders:         make(map[string]forwarder.Forwarder),
		tunnels:            make(map[string]tunnel.TunnelClient),
		endpointCache:      make(map[string]forward.ExitEndpoint),
		healthCheckConfigs: make(map[string]*forward.HealthCheckConfig),
```

- [ ] **步骤 3：编写失败的测试**

创建 `internal/agent/cache_test.go`：

```go
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
```

注意：`"http://127.0.0.1:1"` 这个技巧在整份计划里会反复用到——连接一个本地必然没有进程监听的低号端口，会立即得到 "connection refused"，比起真的连一个不存在的域名更快、更确定，不依赖外部网络，也不会在沙箱里超时挂起。

- [ ] **步骤 4：运行测试验证失败**

运行：`go test ./internal/agent/... -run TestGetExitEndpoint -v`

预期：编译失败，报错 `a.getExitEndpoint undefined (type *Agent has no field or method getExitEndpoint)`。

- [ ] **步骤 5：编写实现代码**

创建 `internal/agent/cache.go`：

```go
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
```

- [ ] **步骤 6：运行测试验证通过**

运行：`go test ./internal/agent/... -run TestGetExitEndpoint -v`

预期：

```
--- PASS: TestGetExitEndpointSuccessUpdatesCache
--- PASS: TestGetExitEndpointFallsBackToCacheOnFailure
--- PASS: TestGetExitEndpointReturnsErrorWhenNoCache
PASS
ok  	github.com/orris-inc/orris-client/internal/agent
```

- [ ] **步骤 7：全量构建 + 格式检查**

运行：`go build ./... && go vet ./... && gofmt -l internal/agent/`

预期：`go build`/`go vet` 无错误退出；`gofmt -l` 无输出。

- [ ] **步骤 8：Commit**

```bash
git add internal/agent/agent.go internal/agent/cache.go internal/agent/cache_test.go
git commit -m "feat: add endpoint-cache fallback for exit agent resolution"
```

---

### 任务 3：`tunnel_manager.go` 改走 `getExitEndpoint`

**文件：**
- 修改：`internal/agent/tunnel_manager.go`

`tunnel_manager.go` 里一共有 6 处直接调用 `a.client.GetExitEndpoint(a.ctx, ...)` 解析出口 Agent 地址的地方：单出口 entry（`getOrCreateTunnel`）、多出口负载均衡 entry（`getOrCreateTunnels` 循环）、WS/TLS/SMUX 三个 tunnel client 的 endpoint refresher 闭包（`createWSClient`/`createTLSClient`/`createSmuxClient`，在隧道失败 3 次后触发重连刷新）、以及 SMUX entry（`getOrCreateSmuxClient`）。这是纯粹的一对一替换（把 `a.client.GetExitEndpoint(a.ctx, X)` 换成 `a.getExitEndpoint(X)`），不引入新行为，靠已有的完整测试套件 + `go build` 兜底，不需要为这一步单独新增测试。

- [ ] **步骤 1：确认当前基线通过**

运行：`go build ./... && go test ./... 2>&1 | tail -20`

预期：构建成功，所有包 `ok`（含任务 1、2 新增的测试）。

- [ ] **步骤 2：替换 `getOrCreateTunnel` 里的调用**

找到：

```go
	endpoint, err := a.client.GetExitEndpoint(a.ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("get exit endpoint: %w", err)
	}

	var t tunnel.TunnelClient
```

替换为：

```go
	endpoint, err := a.getExitEndpoint(agentID)
	if err != nil {
		return nil, fmt.Errorf("get exit endpoint: %w", err)
	}

	var t tunnel.TunnelClient
```

- [ ] **步骤 3：替换 `getOrCreateTunnels` 循环里的调用**

找到：

```go
	for _, agent := range rule.ExitAgents {
		endpoint, err := a.client.GetExitEndpoint(a.ctx, agent.AgentID)
		if err != nil {
```

替换为：

```go
	for _, agent := range rule.ExitAgents {
		endpoint, err := a.getExitEndpoint(agent.AgentID)
		if err != nil {
```

- [ ] **步骤 4：替换 `createWSClient` refresher 里的调用**

找到：

```go
	refresher := func() (string, string, error) {
		ep, err := a.client.GetExitEndpoint(a.ctx, agentID)
		if err != nil {
			return "", "", err
		}
		newURL := fmt.Sprintf("wss://%s/ws", net.JoinHostPort(ep.Address, fmt.Sprintf("%d", ep.WsPort)))
		return newURL, a.getHandshakeToken(), nil
	}
```

替换为：

```go
	refresher := func() (string, string, error) {
		ep, err := a.getExitEndpoint(agentID)
		if err != nil {
			return "", "", err
		}
		newURL := fmt.Sprintf("wss://%s/ws", net.JoinHostPort(ep.Address, fmt.Sprintf("%d", ep.WsPort)))
		return newURL, a.getHandshakeToken(), nil
	}
```

- [ ] **步骤 5：替换 `createTLSClient` refresher 里的调用**

找到：

```go
	refresher := func() (string, string, error) {
		ep, err := a.client.GetExitEndpoint(a.ctx, agentID)
		if err != nil {
			return "", "", err
		}
		newEndpoint := net.JoinHostPort(ep.Address, fmt.Sprintf("%d", ep.TlsPort))
		return newEndpoint, a.getHandshakeToken(), nil
	}
```

替换为：

```go
	refresher := func() (string, string, error) {
		ep, err := a.getExitEndpoint(agentID)
		if err != nil {
			return "", "", err
		}
		newEndpoint := net.JoinHostPort(ep.Address, fmt.Sprintf("%d", ep.TlsPort))
		return newEndpoint, a.getHandshakeToken(), nil
	}
```

- [ ] **步骤 6：替换 `createSmuxClient` refresher 里的调用**

`createSmuxClient` 内部的 refresher 和上面两处开头两行完全一样（`ep, err := a.client.GetExitEndpoint(a.ctx, agentID)` + `if err != nil { return "", "", err }`），单独这四行在文件里不是唯一的，编辑时请连同紧邻的注释一起匹配：

找到：

```go
	// Create endpoint refresher
	refresher := func() (string, string, error) {
		ep, err := a.client.GetExitEndpoint(a.ctx, agentID)
		if err != nil {
			return "", "", err
		}
```

替换为：

```go
	// Create endpoint refresher
	refresher := func() (string, string, error) {
		ep, err := a.getExitEndpoint(agentID)
		if err != nil {
			return "", "", err
		}
```

- [ ] **步骤 7：替换 `getOrCreateSmuxClient` 里的调用**

找到（注意此函数开头的注释是 `// Get the exit agent ID`，和 `getOrCreateTunnel` 里的注释 `// Get the single exit agent ID (priority: ...)` 不同，可以用来区分两处几乎相同的代码块）：

```go
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

	endpoint, err := a.client.GetExitEndpoint(a.ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("get exit endpoint: %w", err)
	}
```

替换为：

```go
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
```

- [ ] **步骤 8：确认没有遗漏的调用点**

运行：`grep -n "a.client.GetExitEndpoint" internal/agent/*.go`

预期：只剩一处匹配，位于 `internal/agent/cache.go` 的 `getExitEndpoint` 函数内部（这是唯一应该直接调用底层 client 的地方）。

- [ ] **步骤 9：构建与测试验证**

运行：`go build ./... && go vet ./... && go test ./... 2>&1 | tail -20`

预期：构建成功，`go vet` 无输出，所有包测试通过（包括任务 1、2 的测试，行为未变）。

- [ ] **步骤 10：Commit**

```bash
git add internal/agent/tunnel_manager.go
git commit -m "refactor: route exit endpoint resolution through cache-aware getExitEndpoint"
```

---

### 任务 4：`startFromCache` + `Start()` 走缓存兜底

**文件：**
- 修改：`internal/agent/agent.go`
- 修改：`internal/agent/cache.go`
- 修改：`internal/agent/cache_test.go`

- [ ] **步骤 1：编写失败的集成测试**

修改 `internal/agent/cache_test.go`，把 import 块：

```go
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
```

替换为：

```go
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
```

然后在文件末尾（`TestGetExitEndpointReturnsErrorWhenNoCache` 函数之后）追加：

```go

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
```

`TestStartFallsBackToCacheWhenSyncFails` 用的规则是 `RuleTypeDirect`，`ListenPort: 0` 会让 `DirectForwarder` 绑定一个操作系统分配的临时端口，`TargetAddress`/`TargetPort` 只是用来通过 `DirectForwarder.Start()` 里"目标地址不能为空"的校验，测试过程中不会真的有连接打到这个目标——所以不需要真实可达的下游，测试是完全确定性、不依赖外部网络的。

- [ ] **步骤 2：运行测试验证失败**

运行：`go test ./internal/agent/... -run TestStart -v`

预期：编译失败，报错 `a.startFromCache undefined (type *Agent has no field or method startFromCache)`。

- [ ] **步骤 3：实现 `startFromCache`**

修改 `internal/agent/cache.go`，把开头的：

```go
package agent

import (
	"github.com/orris-inc/orris-client/internal/forward"
	"github.com/orris-inc/orris-client/internal/logger"
)

// getExitEndpoint resolves the connection endpoint for an exit agent.
```

替换为：

```go
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
```

- [ ] **步骤 4：把 `Start()` 接入缓存兜底**

修改 `internal/agent/agent.go`，找到：

```go
	if err := a.syncRules(); err != nil {
		return fmt.Errorf("initial sync failed: %w", err)
	}

	a.wg.Add(5)
	go a.syncLoop()
	go a.trafficLoop()
	go a.statusLoop()
	go a.hubLoop()
	go a.ruleStatusReportLoop()

	return nil
}
```

替换为：

```go
	if err := a.syncRules(); err != nil {
		logger.Warn("initial sync failed, falling back to local rule cache", "error", err)
		if cacheErr := a.startFromCache(); cacheErr != nil {
			return fmt.Errorf("initial sync failed (%v) and no usable rule cache available (%w)", err, cacheErr)
		}
	}

	a.wg.Add(5)
	go a.syncLoop()
	go a.trafficLoop()
	go a.statusLoop()
	go a.hubLoop()
	go a.ruleStatusReportLoop()

	return nil
}
```

（这里先保持 `wg.Add(5)` 不变——第 6 个后台协程 `cachePersistLoop` 要到任务 5 才会存在，如果现在就加 `wg.Add(6)` 会导致 `Stop()` 里的 `wg.Wait()` 永久阻塞。）

- [ ] **步骤 5：运行测试验证通过**

运行：`go test ./internal/agent/... -run TestStart -v`

预期：

```
--- PASS: TestStartFallsBackToCacheWhenSyncFails
--- PASS: TestStartFromCacheContinuesAfterOneRuleFails
--- PASS: TestStartFailsWhenSyncFailsAndNoCache
PASS
ok  	github.com/orris-inc/orris-client/internal/agent
```

日志里会看到 `initial sync failed, falling back to local rule cache`、`using cached rules to start, control server is unreachable`、`direct forwarder started rule_id=fr_cached ...` 等行，这是预期的（测试没有关闭日志输出）。

- [ ] **步骤 6：全量测试 + 静态检查**

运行：`go build ./... && go vet ./... && go test ./... 2>&1 | tail -20 && gofmt -l internal/agent/`

预期：全部通过，`gofmt -l` 无输出。

- [ ] **步骤 7：Commit**

```bash
git add internal/agent/agent.go internal/agent/cache.go internal/agent/cache_test.go
git commit -m "feat: start forwarders from local rule cache when initial sync fails"
```

---

### 任务 5：规则缓存持久化

**文件：**
- 修改：`internal/agent/agent.go`
- 修改：`internal/agent/cache.go`
- 修改：`internal/agent/forwarder_manager.go`
- 修改：`internal/agent/hub.go`
- 修改：`internal/agent/cache_test.go`

- [ ] **步骤 1：编写失败的测试**

在 `internal/agent/cache_test.go` 末尾（`TestStartFailsWhenSyncFailsAndNoCache` 之后）追加：

```go

func TestWriteRuleCachePersistsCurrentState(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ORRIS_CONFIG_FILE", filepath.Join(dir, "client.env"))

	a := New(config.DefaultConfig())
	a.rulesMu.Lock()
	a.rules = []forward.Rule{{ID: "fr_1", RuleType: forward.RuleTypeDirect}}
	a.clientToken = "fwd_token"
	a.blockedProtocols = []string{"udp"}
	a.rulesMu.Unlock()
	a.endpointCacheMu.Lock()
	a.endpointCache["fa_1"] = forward.ExitEndpoint{Address: "1.1.1.1", WsPort: 100}
	a.endpointCacheMu.Unlock()

	a.writeRuleCache()

	snap, err := rulecache.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(snap.Rules) != 1 || snap.Rules[0].ID != "fr_1" {
		t.Fatalf("Rules = %+v, want 1 rule fr_1", snap.Rules)
	}
	if snap.ClientToken != "fwd_token" {
		t.Errorf("ClientToken = %q, want fwd_token", snap.ClientToken)
	}
	ep, ok := snap.Endpoints["fa_1"]
	if !ok || ep.Address != "1.1.1.1" {
		t.Errorf("Endpoints[fa_1] = %+v, ok=%v, want cached entry", ep, ok)
	}
}

func TestCachePersistLoopWritesOnSignal(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ORRIS_CONFIG_FILE", filepath.Join(dir, "client.env"))

	a := New(config.DefaultConfig())
	a.rulesMu.Lock()
	a.rules = []forward.Rule{{ID: "fr_async", RuleType: forward.RuleTypeDirect}}
	a.rulesMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	a.ctx = ctx
	a.wg.Add(1)
	go a.cachePersistLoop()

	a.persistRuleCache()

	deadline := time.After(2 * time.Second)
	for {
		if snap, err := rulecache.Load(); err == nil && len(snap.Rules) == 1 && snap.Rules[0].ID == "fr_async" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("cache file was not written within timeout")
		case <-time.After(20 * time.Millisecond):
		}
	}

	cancel()
	a.wg.Wait()
}
```

（`cache_test.go` 已经在任务 4 里 import 了 `path/filepath` 和 `rulecache`，这两个测试不需要再改 import 块。）

- [ ] **步骤 2：运行测试验证失败**

运行：`go test ./internal/agent/... -run 'TestWriteRuleCache|TestCachePersistLoop' -v`

预期：编译失败，报错 `a.writeRuleCache undefined`、`a.cachePersistLoop undefined`、`a.persistRuleCache undefined`。

- [ ] **步骤 3：实现持久化方法**

修改 `internal/agent/cache.go`，找到 `getExitEndpoint` 函数结尾：

```go
	logger.Warn("control server unreachable, using cached exit endpoint",
		"agent_id", agentID, "address", cached.Address, "error", err)
	return &cached, nil
}
```

替换为（新增 `a.persistRuleCache()` 调用 + 追加三个新函数）：

```go
	logger.Warn("control server unreachable, using cached exit endpoint",
		"agent_id", agentID, "address", cached.Address, "error", err)
	return &cached, nil
}

// cachePersistDebounceInterval coalesces rapid successive cache-persist
// requests (e.g. during a burst of incremental syncs) into a single disk write.
const cachePersistDebounceInterval = 200 * time.Millisecond

// persistRuleCache signals the debounce goroutine to persist the current rule
// state. Non-blocking: if a persist is already pending, this is a no-op.
func (a *Agent) persistRuleCache() {
	select {
	case a.cachePersistCh <- struct{}{}:
	default:
	}
}

// cachePersistLoop is a debounced loop that writes the rule cache to disk.
// It coalesces multiple rapid persist requests into a single disk write.
func (a *Agent) cachePersistLoop() {
	defer a.wg.Done()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-a.cachePersistCh:
			time.Sleep(cachePersistDebounceInterval)

			for {
				select {
				case <-a.cachePersistCh:
				default:
					goto write
				}
			}
		write:
			a.writeRuleCache()
		}
	}
}

// writeRuleCache builds a snapshot of the current rule state and saves it to
// disk. Persistence is best-effort: failures are logged but do not affect
// agent operation.
func (a *Agent) writeRuleCache() {
	a.rulesMu.RLock()
	rules := make([]forward.Rule, len(a.rules))
	copy(rules, a.rules)
	clientToken := a.clientToken
	blockedProtocols := a.blockedProtocols
	a.rulesMu.RUnlock()

	a.endpointCacheMu.RLock()
	endpoints := make(map[string]forward.ExitEndpoint, len(a.endpointCache))
	for id, ep := range a.endpointCache {
		endpoints[id] = ep
	}
	a.endpointCacheMu.RUnlock()

	snap := &rulecache.Snapshot{
		Rules:            rules,
		ClientToken:      clientToken,
		BlockedProtocols: blockedProtocols,
		Endpoints:        endpoints,
		SavedAt:          time.Now().Unix(),
	}

	if err := rulecache.Save(snap); err != nil {
		logger.Warn("failed to persist rule cache", "error", err)
	}
}
```

再把 `getExitEndpoint` 成功分支里的：

```go
	endpoint, err := a.client.GetExitEndpoint(a.ctx, agentID)
	if err == nil {
		a.endpointCacheMu.Lock()
		a.endpointCache[agentID] = *endpoint
		a.endpointCacheMu.Unlock()
		return endpoint, nil
	}
```

改为（新增一行持久化触发）：

```go
	endpoint, err := a.client.GetExitEndpoint(a.ctx, agentID)
	if err == nil {
		a.endpointCacheMu.Lock()
		a.endpointCache[agentID] = *endpoint
		a.endpointCacheMu.Unlock()
		a.persistRuleCache()
		return endpoint, nil
	}
```

- [ ] **步骤 4：新增 `cachePersistCh` 字段并接入 `Start()`**

修改 `internal/agent/agent.go`，找到：

```go
	// Debounce channel for rule status reporting
	// When a status change occurs, a signal is sent to this channel
	// A single goroutine drains and coalesces multiple updates
	ruleStatusReportCh chan struct{}
```

替换为：

```go
	// Debounce channel for rule status reporting
	// When a status change occurs, a signal is sent to this channel
	// A single goroutine drains and coalesces multiple updates
	ruleStatusReportCh chan struct{}

	// Debounce channel for local rule-cache persistence.
	// Signaled after every successful sync so the on-disk cache stays fresh.
	cachePersistCh chan struct{}
```

找到：

```go
		ruleStatus:         make(map[string]*ruleStatus),
		ruleStatusReportCh: make(chan struct{}, 1), // buffered to avoid blocking
	}
}
```

替换为：

```go
		ruleStatus:         make(map[string]*ruleStatus),
		ruleStatusReportCh: make(chan struct{}, 1), // buffered to avoid blocking
		cachePersistCh:     make(chan struct{}, 1), // buffered to avoid blocking
	}
}
```

找到：

```go
	a.wg.Add(5)
	go a.syncLoop()
	go a.trafficLoop()
	go a.statusLoop()
	go a.hubLoop()
	go a.ruleStatusReportLoop()

	return nil
}
```

替换为：

```go
	a.wg.Add(6)
	go a.syncLoop()
	go a.trafficLoop()
	go a.statusLoop()
	go a.hubLoop()
	go a.ruleStatusReportLoop()
	go a.cachePersistLoop()

	return nil
}
```

- [ ] **步骤 5：在 `syncRules()` 成功路径上触发持久化**

修改 `internal/agent/forwarder_manager.go`，找到 `syncRules()` 的结尾：

```go
	// Start forwarders for new rules (no lock needed for existingIDs check)
	for _, rule := range rules {
		if _, exists := existingIDs[rule.ID]; !exists {
			r := rule
			if err := a.startForwarder(&r); err != nil {
				logger.Error("start forwarder failed", "rule_id", rule.ID, "error", err)
			}
		}
	}

	return nil
}
```

替换为：

```go
	// Start forwarders for new rules (no lock needed for existingIDs check)
	for _, rule := range rules {
		if _, exists := existingIDs[rule.ID]; !exists {
			r := rule
			if err := a.startForwarder(&r); err != nil {
				logger.Error("start forwarder failed", "rule_id", rule.ID, "error", err)
			}
		}
	}

	a.persistRuleCache()

	return nil
}
```

- [ ] **步骤 6：在 `handleFullSync()` 和 `handleIncrementalSync()` 成功路径上触发持久化**

修改 `internal/agent/hub.go`，找到 `handleFullSync()` 的结尾：

```go
	// Start forwarders for new rules
	for _, rule := range newRules {
		a.forwardersMu.RLock()
		_, exists := a.forwarders[rule.ID]
		a.forwardersMu.RUnlock()

		if !exists {
			if err := a.startForwarder(rule); err != nil {
				logger.Error("start forwarder failed", "rule_id", rule.ID, "error", err)
			}
		}
	}

	return nil
```

替换为：

```go
	// Start forwarders for new rules
	for _, rule := range newRules {
		a.forwardersMu.RLock()
		_, exists := a.forwarders[rule.ID]
		a.forwardersMu.RUnlock()

		if !exists {
			if err := a.startForwarder(rule); err != nil {
				logger.Error("start forwarder failed", "rule_id", rule.ID, "error", err)
			}
		}
	}

	a.persistRuleCache()

	return nil
```

再找到 `handleIncrementalSync()` 的结尾：

```go
	// Update rules list
	a.updateRulesList(data)

	return nil
```

替换为：

```go
	// Update rules list
	a.updateRulesList(data)

	a.persistRuleCache()

	return nil
```

- [ ] **步骤 7：运行测试验证通过**

运行：`go test ./internal/agent/... -run 'TestWriteRuleCache|TestCachePersistLoop' -v`

预期：

```
--- PASS: TestWriteRuleCachePersistsCurrentState
--- PASS: TestCachePersistLoopWritesOnSignal
PASS
ok  	github.com/orris-inc/orris-client/internal/agent
```

- [ ] **步骤 8：全量测试 + 静态检查**

运行：`go build ./... && go vet ./... && go test ./... 2>&1 | tail -20 && gofmt -l internal/agent/`

预期：全部通过，`gofmt -l` 无输出。

- [ ] **步骤 9：Commit**

```bash
git add internal/agent/agent.go internal/agent/cache.go internal/agent/forwarder_manager.go internal/agent/hub.go internal/agent/cache_test.go
git commit -m "feat: persist rule cache to disk on every successful sync"
```

---

### 任务 6：卸载脚本清理规则缓存文件

**文件：**
- 修改：`scripts/install.sh`

规则缓存文件（`${CONFIG_FILE}.rules_cache.json`）里含有 `clientToken`，属于敏感信息。卸载 Agent 实例时如果不清理，会在磁盘上留下残留 token。这一步让卸载流程和现有的 `CONFIG_FILE` 清理保持一致。

- [ ] **步骤 1：定位现有的配置文件清理逻辑**

运行：`grep -n 'Removing config' scripts/install.sh`

预期输出类似：`555:        print_info "Removing config ${CONFIG_FILE}..."`

- [ ] **步骤 2：新增规则缓存文件清理**

修改 `internal/agent/../../scripts/install.sh`（即仓库根目录下的 `scripts/install.sh`）的 `uninstall_one_resources()` 函数，找到：

```bash
    if [ -f "$CONFIG_FILE" ]; then
        print_info "Removing config ${CONFIG_FILE}..."
        rm -f "$CONFIG_FILE"
    fi
    if [ -f "$LOG_FILE" ]; then
```

替换为：

```bash
    if [ -f "$CONFIG_FILE" ]; then
        print_info "Removing config ${CONFIG_FILE}..."
        rm -f "$CONFIG_FILE"
    fi
    if [ -f "${CONFIG_FILE}.rules_cache.json" ]; then
        print_info "Removing rule cache ${CONFIG_FILE}.rules_cache.json..."
        rm -f "${CONFIG_FILE}.rules_cache.json"
    fi
    if [ -f "$LOG_FILE" ]; then
```

注意：这里的字面量后缀 `.rules_cache.json` 必须和 `internal/rulecache/rulecache.go` 里的常量 `cacheSuffix` 保持一致。以后如果修改 `cacheSuffix`，要同步修改这一行。

- [ ] **步骤 3：语法检查**

运行：`bash -n scripts/install.sh`

预期：无输出（脚本语法正确）。

- [ ] **步骤 4：Commit**

```bash
git add scripts/install.sh
git commit -m "chore: remove rule cache file on instance uninstall"
```

---

### 任务 7：全量验证

**文件：** 无新增/修改，仅验证。

- [ ] **步骤 1：全量构建**

运行：`go build ./...`

预期：无输出，退出码 0。

- [ ] **步骤 2：静态检查**

运行：`go vet ./...`

预期：无输出，退出码 0。

- [ ] **步骤 3：格式检查**

运行：`gofmt -l $(find . -name '*.go' -not -path './build/*')`

预期：无输出。（如果看到 `internal/tunnel/smux_config.go`，那是本计划改动之前就存在的既有格式问题，与本次改动无关，不需要在这个计划里处理。）

- [ ] **步骤 4：全量测试**

运行：`go test ./...`

预期：所有包 `ok`，无 `FAIL`。

- [ ] **步骤 5：竞态检测**

运行：`go test -race ./internal/agent/... ./internal/rulecache/...`

预期：`ok`，无 `WARNING: DATA RACE`。

- [ ] **步骤 6：手动烟测（可选，需要一台可运行 systemd 的机器）**

这一步验证的是真实二进制在"进程重启 + 主控机不可达"场景下的行为，自动化测试覆盖不到"systemd 真的把进程拉起来"这个环节，需要人工走一遍：

1. 用 `make build`（或仓库现有的构建方式）编译出 `orris-client` 二进制，正常启动一次，指向一个真实可达的 orris server，确认能同步到规则、转发器正常工作。
2. 确认缓存文件已生成：`cat $(ORRIS_CONFIG_FILE=/path/to/client.env; echo /path/to/client.env.rules_cache.json)`，应该能看到 JSON 格式的规则列表。
3. 停掉 Agent 进程。
4. 修改 `ORRIS_SERVER_URL`（或直接断网/防火墙拦截）让 Agent 连不上主控机。
5. 重新启动 Agent，观察日志：应该看到 `initial sync failed, falling back to local rule cache` 和 `using cached rules to start, control server is unreachable`，随后各条规则对应的转发器正常启动（而不是进程直接退出）。
6. 确认之前配置的端口仍然在监听：`ss -ltnp | grep <listen_port>`。
7. 恢复到主控机可达，观察日志里出现 `rules synced successfully` 或 `config sync completed`，确认自动纠偏正常。

---

## 附：后续可能的延伸（本计划不包含）

- 缓存文件的过期策略（本次按用户要求不设过期）。
- `RefreshRule`（而非 `GetExitEndpoint`）驱动的 chain/relay 隧道运行时重连刷新逻辑的缓存兜底——设计文档中已明确排除，因为这些规则首次建道本就不依赖主控机（`NextHopAddress` 已写在规则里），只有"已建立隧道失败后的重新刷新"这一条边缘路径依赖主控机。
