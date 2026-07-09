# 主控机不可达时的规则本地缓存兜底

## 背景与问题

`orris-client` 是连接主控机（orris server）的转发 Agent。当前架构下：

- **运行时掉线是安全的**：Hub WebSocket 断开后（`internal/agent/hub.go` 的 `hubLoop`/`runHubWithReconnect`）只是带指数退避地不断重连，并不会停止已经在跑的转发器（`stopAll()` 只在 `Agent.Stop()` 里调用）。已建立的转发在这种情况下能继续工作。
- **启动阶段是致命的**：`Agent.Start()`（`internal/agent/agent.go`）在初始 `syncRules()`（HTTP 拉取规则）失败时直接 `return error`，`cmd/orris-client/main.go` 随即 `os.Exit(1)`。规则只存在内存里，从未落盘。
- systemd 单元里配置的是 `Restart=always`（`scripts/install.sh`）。这意味着只要连不上主控机，进程会立刻退出、立刻被拉起、立刻再失败，陷入死循环——全程不转发任何流量，即便此前已经稳定同步过一批规则、并且这些规则本不需要主控机才能继续工作。

此外，部分规则（entry 角色且未预置 `NextHopAddress` 的单/多出口转发、以及走动态解析的 SMUX entry）在**建立新隧道**时需要调用主控机的 `GetExitEndpoint` 接口解析出口 Agent 的地址，这个调用同样会在主控机不可达时失败。

## 目标

当 Agent 启动（含 systemd 重启）时连不上主控机，不再直接失败退出，而是使用本地缓存的"最后一次成功同步"的规则和出口端点信息，把转发器先跑起来；同时后台持续尝试重新连接主控机，一旦恢复，走既有的同步/纠偏逻辑自动对齐最新配置。

不覆盖：进程存活期间的运行时掉线处理——这部分现有代码已经能保持转发不中断，无需改动。

## 架构

### 新增包：`internal/rulecache`

职责单一、不依赖 `Agent`，可独立测试的本地缓存读写模块。

```go
package rulecache

type Snapshot struct {
    Rules            []forward.Rule
    ClientToken      string
    BlockedProtocols []string
    Endpoints        map[string]forward.ExitEndpoint // agentID -> 最近一次解析成功的出口端点
    SavedAt          int64                            // unix 时间戳，仅用于日志展示
}

// FilePath 派生自 config.ConfigFilePath() 所在目录（默认 /etc/orris/），
// 文件名固定为 rules_cache.json。跟随 ORRIS_CONFIG_FILE 的目录变化。
func FilePath() string

// Load 读取并反序列化缓存文件。文件不存在、内容损坏、或为空规则集，均返回 error。
func Load() (*Snapshot, error)

// Save 原子写入（临时文件 + os.Rename），权限 0600（内容含 clientToken，敏感）。
// 目标路径若已是符号链接则拒绝写入（复用 config.SaveServerURL 的安全策略）。
func Save(snap *Snapshot) error
```

不设过期时间——只要能反序列化出非空规则列表，就认为可用，永远信任最后一次成功缓存。

### Agent 改动（`internal/agent`）

**新增字段**

```go
endpointCacheMu sync.RWMutex
endpointCache   map[string]forward.ExitEndpoint // agentID -> 最近一次成功解析的出口端点

cachePersistCh chan struct{} // 防抖信号，复用 ruleStatusReportLoop 的合并写盘模式
```

**`Start()` 流程**

```
syncRules() 失败
  → rulecache.Load()
      → 命中且规则非空 → startFromCache(snapshot)：
          用缓存恢复 a.rules / a.clientToken / a.blockedProtocols / a.endpointCache，
          逐条调用现有 startForwarder()。单条规则起不来只记录该规则的错误状态，
          不影响其它规则启动，也不让整个 Start() 失败。
          日志明确提示："使用本地缓存规则启动，主控机暂不可达，缓存时间 X"
      → 未命中 / 规则为空 → 保持现状：Start() 返回 error，进程退出
（无论走哪条路径）后台循环正常启动：syncLoop / hubLoop / trafficLoop /
statusLoop / ruleStatusReportLoop / cachePersistLoop
```

`syncLoop`（HTTP 兜底轮询，默认 5 分钟一次）与 `hubLoop`（WS 重连，带退避）本身已经会持续重试并在恢复后触发 `handleFullSync` / `handleIncrementalSync` / `syncRules` 的既有 diff 逻辑，自动增删/重启转发器——**不需要为"缓存态恢复到最新态"编写任何新的对账代码**，这是现有机制的自然延伸。

**持久化时机**：`syncRules()`（成功分支）、`handleFullSync()`、`handleIncrementalSync()` 结束时都调用 `persistRuleCache()`（往 `cachePersistCh` 发信号，防抖协程负责真正落盘），保证缓存始终反映"当前 Agent 内存中的规则状态"。

**出口端点解析统一走缓存兜底**

新增：

```go
// getExitEndpoint 包装 a.client.GetExitEndpoint。
// 成功：更新 endpointCache[agentID]，触发防抖持久化，返回结果。
// 失败：查 endpointCache[agentID]；命中则打 warning 日志（使用了陈旧的缓存地址）
//       并返回缓存值；未命中则把原始错误原样返回。
func (a *Agent) getExitEndpoint(agentID string) (*forward.ExitEndpoint, error)
```

`internal/agent/tunnel_manager.go` 中原本 4 处直接调用 `a.client.GetExitEndpoint(...)` 的地方统一改为调用 `a.getExitEndpoint(...)`：
- `getOrCreateTunnel`（单出口 entry）
- `getOrCreateTunnels` 循环（多出口负载均衡 entry）
- `getOrCreateSmuxClient`（SMUX entry）
- SMUX 客户端的 endpoint refresher 闭包（`createSmuxClient` 内部，失败 3 次后触发的重连刷新）

**明确不覆盖的范围**：chain/relay 隧道运行中失败重连所用的 `RefreshRule`（而非 `GetExitEndpoint`）刷新闭包（`createWSClientByAddress` / `createTLSClientByAddress` / `createSmuxClientByAddress` 内部）。这些规则本身因为已经把 `NextHopAddress` 直接写在 `Rule` 里，首次建道天然不需要主控机；只有"已建立的隧道失败后触发的重新刷新"这一条边缘路径依赖主控机，且 `RefreshRule` 返回的是完整 Rule（语义比端点复杂得多），本次不做缓存兜底，维持现状（按原有退避策略持续重试）。

### 数据流小结

```
Agent.Start()
  ├─ syncRules() [HTTP GetRules]
  │    ├─ 成功 → 防抖持久化缓存（rules/clientToken/blockedProtocols）
  │    └─ 失败 → rulecache.Load()
  │         ├─ 有缓存 → startFromCache()：用缓存规则/端点拉起转发器
  │         └─ 无缓存 → 维持现状，Start() 报错、进程退出
  └─ 后台循环启动，主控机恢复后走既有 diff 逻辑自动纠偏，无需新代码

getExitEndpoint(agentID)  [替换所有直连 a.client.GetExitEndpoint 的地方]
  ├─ 调用主控机 API
  │    ├─ 成功 → 更新 endpointCache[agentID]，防抖持久化，返回
  │    └─ 失败 → 查 endpointCache[agentID]
  │         ├─ 命中 → 打 warning 日志，返回缓存端点（可能陈旧，直到主控机恢复）
  │         └─ 未命中 → 原样返回错误（行为不变）
```

## 错误处理与边界情况

- 缓存文件损坏（JSON 解析失败）：`Load()` 返回 error，等同于"无缓存"，不会导致 panic 或用半份数据启动。
- 缓存文件为空规则列表：视为不可用，不使用。
- 缓存中某条规则在 `startFromCache` 阶段起不来（如端口被占用）：记录该规则状态为 error，其余规则不受影响，`Start()` 整体仍返回成功。
- 缓存文件写入失败（磁盘满/权限问题）：只记录 warning 日志，不影响 Agent 正常运行（持久化是尽力而为，不是关键路径）。
- 缓存文件权限固定为 0600，且拒绝写入符号链接目标，与现有 `config.SaveServerURL` 的安全策略保持一致（缓存内容含 `clientToken`，敏感）。

## 测试计划（供实现计划阶段细化）

- `rulecache`：保存/读取往返；文件不存在；文件损坏；空规则集；符号链接目标拒绝写入；原子写（写入中途失败不破坏旧文件）。
- `Agent.startFromCache`：多条规则中一条失败不影响其它规则启动；成功恢复 `clientToken`/`blockedProtocols`/`endpointCache`。
- `Agent.getExitEndpoint`：主控机可用时正常透传并更新缓存；主控机不可用且有缓存时降级返回缓存值并打日志；主控机不可用且无缓存时原样返回错误。
- 集成/手测：模拟主控机不可达状态下重启进程，确认已同步过的规则能继续监听；模拟主控机恢复后确认能自动纠偏（新增/删除/变更的规则生效）。
