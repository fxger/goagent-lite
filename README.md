# goagent-lite

轻量级、多 Agent、并发可控的 **Agent 执行网关（Agent Execution Gateway）**。

goagent-lite 不是 Agent 框架，也不是 Agent Runtime。它不实现 ReAct、Plan & Execute、LangGraph，不调用 LLM，也不理解 Agent 的内部结构。它只做一层统一、可观测、可隔离的执行管理：

- 通过 HTTP API 接收任务；
- 按 `(agent_id, agent_version)` 路由到已注册的 Agent；
- 用固定 Worker Pool + per-agent 队列 + Agent 信号量做双层并发控制；
- 统一处理排队、超时、取消、panic 恢复与优雅关闭；
- 把 Agent 事件统一编号并通过 SSE 转发给任意数量的订阅者；
- 通过 `AgentAdapter` 接入进程内或远程 Agent（远程协议见 [docs/protocol.md](docs/protocol.md)）。

## 为什么需要 Agent Gateway

当多个 Agent 服务（不同语言、不同框架、不同团队）对外提供能力时，调用方会面对一堆重复问题：谁限制总并发？某个 Agent 慢会不会拖垮其他 Agent？任务排队、超时、取消语义是否一致？事件流格式是否统一？进程退出时正在跑的任务怎么办？

把这些横切关注点收敛到一个 Go 网关后：

- 调用方只面对一套任务 API；
- Agent 侧只实现一个很薄的执行协议；
- 并发、隔离、过载保护、生命周期由网关统一保证；
- 单个 Agent 的异常不会扩散到其他 Agent。

与 Agent Framework/Runtime 的区别：Framework 定义“Agent 怎么思考和行动”，Gateway 定义“任务怎么被提交、排队、执行、取消和观测”。两者正交，可以组合使用。

## 快速开始

要求 Go 1.23+（开发环境使用 Go 1.27）。

```bash
# 1. 启动两个远程 mock agent（可选，用于 HTTP 协议示例）
go run ./examples/mock-agent -addr :9101
go run ./examples/mock-agent -addr :9102 -fail-rate 0.1

# 2. 启动网关
go run ./cmd/server -config configs/agents.yaml
# 监听 :8080

# 3. 提交任务（local-mock 为进程内 Agent，无需启动 mock-agent）
curl -s -X POST http://127.0.0.1:8080/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{"agent_id":"local-mock","agent_version":"v1","input":{"query":"hello"}}'

# 4. 查询 / 结果 / 事件
curl -s http://127.0.0.1:8080/api/v1/tasks/<task_id>
curl -s http://127.0.0.1:8080/api/v1/tasks/<task_id>/result
curl -N http://127.0.0.1:8080/api/v1/tasks/<task_id>/events

# 5. 取消
curl -s -X POST http://127.0.0.1:8080/api/v1/tasks/<task_id>/cancel
```

也可以直接运行远程 mock：

```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{"agent_id":"remote-mock-a","agent_version":"v1","input":{"query":"hi"}}'
```

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/tasks` | 提交任务，返回 `201 {task_id,status}` |
| GET | `/api/v1/tasks/{id}` | 查询任务状态、耗时、错误 |
| GET | `/api/v1/tasks/{id}/result` | 最终结果（`completed` 的 payload）；无结果时 `null` |
| GET | `/api/v1/tasks/{id}/events` | SSE：先回放历史，再实时推送；终态后服务端关闭 |
| POST | `/api/v1/tasks/{id}/cancel` | 取消任务 |
| GET | `/health` | 健康检查（过载时仍可用） |
| GET | `/metrics` | 轻量 JSON 指标 |
| GET | `/debug/pprof/*` | 仅在 `enable_pprof: true` 时挂载 |

提交示例：

```json
{
  "agent_id": "research-agent",
  "agent_version": "v1",
  "input": { "query": "分析当前 RAG 技术的发展趋势" },
  "config": { "timeout_seconds": 120 },
  "metadata": { "user_id": "10001", "trace_id": "trace-001" }
}
```

- `agent_id`、`agent_version`、`input` 必填；`input` 必须是 JSON 对象。
- `config.timeout_seconds` 仅覆盖 `RUNNING` 阶段，排队时间不计入。
- `metadata` 为 `map[string]string` 透传。

统一错误结构：

```json
{ "error": { "code": "QUEUE_FULL", "message": "gateway queue is full" } }
```

| 错误码 | HTTP | 场景 |
|---|---|---|
| `INVALID_REQUEST` | 400 | JSON/字段/大小非法 |
| `AGENT_NOT_FOUND` | 404 | `(agent_id, version)` 未注册 |
| `AGENT_DISABLED` | 409 | Agent 被禁用 |
| `QUEUE_FULL` | 429 | 全局 QUEUED 达到 `queue_capacity` |
| `AGENT_QUOTA_EXCEEDED` | 429 | 该 Agent 的队列达到 `queue_capacity` |
| `TASK_NOT_FOUND` | 404 | 任务不存在或已过保留期 |
| `TASK_ALREADY_FINISHED` | 409 | 对终态任务再次取消 |
| `GATEWAY_SHUTTING_DOWN` | 429 | 优雅关闭期间提交 |
| `INTERNAL_ERROR` | 500 | 未分类内部错误 |

任务状态：`QUEUED → RUNNING → SUCCEEDED / FAILED / TIMEOUT / CANCELLED`，终态只能进入一次。取消 `QUEUED` 任务立即生效；取消 `RUNNING` 任务返回受理语义（无 `CANCELLING` 状态），实际终止通过任务 ctx 传播。

SSE 事件格式：

```json
{"task_id":"task-…","sequence":3,"type":"token","timestamp":"2026-11-07T10:00:00Z","payload":{"text":"分析"}}
```

`sequence` 由网关按到达顺序分配。SSE 缓冲满的慢消费者会被断开，不会反压任务执行。

## 配置

完整示例见 [`configs/agents.yaml`](configs/agents.yaml)。

```yaml
gateway:
  listen_addr: ":8080"
  max_concurrency: 500          # Worker 数 = 全局 RUNNING 上限
  queue_capacity: 1000          # 所有 Agent 子队列中 QUEUED 总数上限
  default_timeout_seconds: 120  # 仅 RUNNING 阶段
  shutdown_deadline_seconds: 30
  max_request_body_bytes: 1048576
  sse_heartbeat_seconds: 15
  sse_subscriber_buffer: 64
  event_retention_minutes: 10
  enable_pprof: false
  log_level: info

agents:
  - id: local-mock
    version: v1
    protocol: local
    max_concurrency: 100
    queue_capacity: 200
    timeout_seconds: 60
    mock:
      duration_ms: 250
      jitter_ms: 250
      fail_rate: 0.0
      panic_rate: 0.0
      tokens: 5
      token_interval_ms: 20

  - id: remote-mock-a
    version: v1
    protocol: http
    endpoint: http://127.0.0.1:9101
    max_concurrency: 200
    queue_capacity: 400
    timeout_seconds: 30
    enabled: true
```

配置在启动时严格校验（未知字段、重复 `(id,version)`、非法 URL、并发/队列/超时越界、Agent 并发超过网关上限），任一错误都会拒绝启动。

### 如何配置并发上限

1. `gateway.max_concurrency` 决定 Worker 数，也就是进程内同时 RUNNING 的任务上限。默认 500。
2. 每个 Agent 的 `max_concurrency` 是该 Agent 的独立配额，必须 ≤ 网关值。A 打满只会让自己的队列排队，B 仍可继续领取 Worker。
3. 全局 `queue_capacity` 限制所有 Agent 的 QUEUED 总数；Agent 级 `queue_capacity` 可选，用于隔离单 Agent 的排队长度。
4. 超出容量立即返回 429，不会阻塞请求。

## 接入一个新 Agent

远程 Agent 只需实现一个端点：`POST {endpoint}/v1/execute`，返回 `text/event-stream`，每个事件一行 `data: {...}`，最后一条必须是 `completed` 或 `failed`。完整规范见 [docs/protocol.md](docs/protocol.md)，可运行参考见 [`examples/mock-agent`](examples/mock-agent/main.go)。

接入步骤：

1. 在 `configs/agents.yaml` 的 `agents` 中增加一项，`protocol: http`，填 `endpoint`、并发/队列/超时；
2. 重启网关（配置在启动时加载）；
3. 用 `agent_id` / `agent_version` 提交任务。

### 如何添加新的 Adapter

实现 `internal/agent.AgentAdapter`：

```go
Start(ctx context.Context, req AgentRequest) (<-chan AgentEvent, error)
```

约束：

- Adapter 无状态，可被多个 Worker 并发调用；
- `Start` 返回 error 表示启动失败（连接失败、HTTP 非 200 等）；运行中失败通过流内 `failed` 事件或流关闭表达；
- Adapter 是事件 channel 的唯一关闭方；channel 必须有界（建议 ≥32），不得丢事件；
- 感知 `ctx.Done()`，取消时停止一切阻塞操作并关闭 channel；
- 只依赖 `internal/agent` 的协议类型，不得 import `task/scheduler/worker`。

然后在 `cmd/server/main.go` 的 registry 构造处按 `protocol` 选择新 Adapter。

## 运行与测试

```bash
go build ./...
go test ./...
go test -race ./...   # 需要 cgo 工具链；Windows 可先安装 MinGW-w64 或 WinLibs
go vet ./...
```

`tests/integration` 包含全链路与故障测试（goleak 检查）。`tests/benchmark` 是可选压测，默认跳过：

```bash
# 500 并发、500 个任务、mock 时长均匀分布 100ms–2s
GOAGENT_BENCH=1 BENCH_TASKS=500 BENCH_WORKERS=500 \
  go test ./tests/benchmark -run TestLoad -v
```

也可以使用 Makefile：

```bash
make fmt vet test race
make bench          # 500 任务
make run            # 启动网关
make mock           # 启动参考 mock agent
```

压测报告与硬件条件见 [docs/benchmark.md](docs/benchmark.md)。报告不会把“成功提交 500 个任务”等同于“500 并发执行”：以 `/metrics` 的 RUNNING、Worker 存活数、排队与执行时长为准。

## 项目结构

```text
cmd/server/             进程入口、依赖装配、优雅关闭
internal/api/           HTTP 路由、参数校验、SSE handler、错误码
internal/task/          任务模型、状态机、Manager facade
internal/scheduler/     per-agent 队列、信号量、round-robin 派发
internal/worker/        固定 Worker Pool
internal/executor/      执行、超时、取消、事件消费、终态 CAS
internal/agent/         协议类型、Registry、Router
internal/agent/adapters local.go（进程内 mock）、http.go（远程 SSE）
internal/event/         sequence、扇出、回放、订阅管理
internal/repository/    Store 接口 + 内存实现
internal/config/        YAML 加载与严格校验
internal/metrics/       atomic 指标
configs/agents.yaml     示例配置
examples/mock-agent/    §4.5 协议参考实现
tests/integration/      全链路与故障测试
tests/benchmark/        可选压测
docs/                   架构、协议、压测报告
```

依赖方向：`api → task → scheduler/worker → executor → repository / agent / event`。`executor` 不回调 `task.Manager`，`agent` 不依赖 `task/scheduler/worker`。

## 能力边界（第一阶段）

本版本有意保持轻量，以下事项**不做**，请勿按这些能力设计生产方案：

- **不持久化**：任务、结果、事件都在进程内存中，进程崩溃或重启后全部丢失。
- **不做幂等提交**：没有 idempotency key；重复 POST 会产生多个独立任务。
- **不做排队超时**：`QUEUED` 任务不会因等待过久而超时；背压由有界队列和 429 提供。
- **不做事件断线续传**：SSE 不支持 `Last-Event-ID`；重连会从 `sequence 0` 重新回放（仅在 `event_retention_minutes` 保留期内）。
- **不做分布式 exactly-once**：单进程、无跨节点协调。

同时不引入：Redis/Kafka/RabbitMQ/NATS、Kubernetes/Service Mesh、分布式任务调度、复杂 Agent Memory/RAG、LLM 推理、自研 Agent 框架、完整 MCP、Agent 自动进化/RL、复杂 Web 管理后台。

## 依赖

仅两个第三方依赖，其余全部标准库：

- `gopkg.in/yaml.v3`：YAML 配置解析；
- `go.uber.org/goleak`：仅测试使用的 goroutine 泄漏检测。
