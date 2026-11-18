# goagent-lite 架构设计

本文描述网关的内部分层、并发模型、任务生命周期与关键可靠性机制。协议细节见 [protocol.md](protocol.md)，压测数据见 [benchmark.md](benchmark.md)。

## 1. 定位

goagent-lite 是一个**单进程、内存态**的多 Agent 执行网关：

- 统一接收任务（HTTP API），按 Agent 维度做并发隔离；
- 统一调度、排队、超时、取消；
- 统一事件流（SSE）与最终结果；
- 通过 `AgentAdapter` 接入任意 Agent 实现（进程内或远程 HTTP/SSE）。

它**不是** Agent 框架/Runtime：不理解 ReAct、Plan & Execute、LangGraph、多 Agent 拓扑，也不调用 LLM。它只管理“任务生命周期”这一层。

## 2. 分层与依赖方向

```text
                 ┌─────────────────────────────┐
   HTTP/SSE ───▶ │ internal/api                │
                 └──────────────┬──────────────┘
                                ▼
                 ┌─────────────────────────────┐
                 │ internal/task (Manager)     │  状态机、提交/取消/查询门面
                 └───────┬─────────────┬───────┘
                         ▼             ▼
        ┌────────────────────┐   ┌─────────────────┐
        │ internal/scheduler │   │ internal/event  │
        └─────────┬──────────┘   └─────────────────┘
                  ▼
        ┌────────────────────┐
        │ internal/worker    │  固定 Worker Pool
        └─────────┬──────────┘
                  ▼
        ┌────────────────────┐
        │ internal/executor  │  超时、取消、事件消费、终态 CAS
        └───┬────────────┬───┘
            ▼            ▼
   ┌──────────────┐  ┌──────────────────────────┐
   │ repository   │  │ agent (+ adapters)       │
   │ (memory)     │  │ local.go / http.go       │
   └──────────────┘  └──────────────────────────┘
```

单向依赖：`api → task → scheduler/worker → executor → repository / agent / event`。

- `executor` 不回调 `task.Manager`（避免循环依赖）。
- `agent`（含 adapters）只依赖协议类型，不依赖 task/scheduler/worker。
- `scheduler` 不认识 `task.Task`：它调度 `scheduler.Job{ID, AgentKey, Value any}`，由 Manager 在 `Value` 中放入任务对象。
- `task.Manager` 通过 `task.Canceller` 接口（由 executor 实现）取消运行中任务。

可用的依赖方向检查：

```bash
go list -deps ./internal/agent/... | grep goagent-lite/internal/task   # 必须为空
go list -deps ./internal/executor | grep goagent-lite/internal/task     # 只允许 task（Store/Model）
```

## 3. 任务生命周期

状态机（终态只能进入一次，通过 Repository 的 CAS 保证）：

```text
            ┌──────────┐
 submit ───▶│  QUEUED  │──── cancel ───▶ CANCELLED (task_cancelled)
            └────┬─────┘
                 │ worker 出队 + CAS
                 ▼
            ┌──────────┐
            │ RUNNING  │── completed ──▶ SUCCEEDED
            └────┬─────┘── failed ─────▶ FAILED
                 │── ctx deadline ────▶ TIMEOUT
                 │── cancel ──────────▶ CANCELLED
                 └── 进程关闭 ────────▶ CANCELLED (gateway_shutdown)
```

- 提交即 `QUEUED`（无 `CREATED`）。
- 终态集合：`SUCCEEDED / FAILED / CANCELLED / TIMEOUT`。
- 取消与完成竞争：谁先成功 CAS 谁生效，另一个成为 no-op。
- `QUEUED` 任务取消是“惰性删除”：只把状态置为 `CANCELLED` 并在调度器登记，任务对象仍留在队列里，出队时直接丢弃且不占用 Agent 配额。
- `timeout_seconds` 只覆盖 `RUNNING` 阶段；排队时间不计入超时。

## 4. 并发模型

### 4.1 Worker = 并发槽

Worker 数在启动时固定为 `gateway.max_concurrency`。每个 Worker 同一时刻只执行一个任务，因此：

```text
进程内 RUNNING 任务数 ≤ Worker 数 = max_concurrency
```

没有额外的“任务 goroutine 池”。任务执行期间，Worker goroutine 被该任务占用；Executor 另起一个事件泵 goroutine 消费 Adapter 事件流，直到 Adapter 关闭 channel 才退出（panic 时自行 recover 并 drain）。因此每个 RUNNING 任务固定对应 3 个 goroutine：Worker、事件泵、Adapter 内部 goroutine。

### 4.2 双层调度

调度器维护：

- 全局 `queued` 计数（受 `queue_capacity` 限制）；
- 每个 Agent 一条 FIFO 队列；
- 每个 Agent 一个容量为 `max_concurrency` 的信号量（Agent 配额）；
- 一个 round-robin 游标。

`Next()` 的派发循环：

1. 从游标开始轮询各 Agent 队列；
2. 队列为空 → 跳过；
3. **先**非阻塞抢该 Agent 的信号量，抢不到（配额已满）→ 跳过，继续下一个 Agent（避免队头阻塞）；
4. 抢到信号量后出队；若该任务已被取消 → 归还信号量、丢弃任务，继续扫描；
5. 成功派发后移动游标，并在仍有排队任务时链式唤醒另一个 Worker。

这保证：

- 一个慢/高并发 Agent 不会阻塞其他 Agent；
- 不会出现“全局有空闲 Worker、却因为某个队列配额满而空转”；
- 配额只在真正执行时占用，取消/完成/超时/panic 通过 `defer release()` 归还。

### 4.3 过载保护

- 全局 `QUEUED` 达到 `queue_capacity` → `429 QUEUE_FULL`；
- 某 Agent 配置了 `queue_capacity` 且其队列已满 → `429 AGENT_QUOTA_EXCEEDED`；
- 拒绝在入队时立即判定，不等待、不阻塞请求，并计入 `/metrics.rejections`。

## 5. 执行与事件

`Executor.Execute` 对一个已出队任务：

1. CAS `QUEUED → RUNNING`（失败说明已被取消，直接释放配额）；
2. 计算超时：任务级 `config.timeout_seconds` > Agent 配置 `timeout_seconds` > 全局默认；
3. `context.WithTimeout` 派生任务 ctx，调用 `Adapter.Start`；
4. 逐条消费 Adapter 事件 → Event Manager 分配 `sequence` 并扇出；
5. 收到 `completed` → 结果写入任务并 CAS `SUCCEEDED`；收到 `failed` → 读取 `error_code/message` 并 CAS `FAILED`；
6. 流关闭但没有终态：
   - 用户取消 → `CANCELLED(task_cancelled)`
   - 超时 → `TIMEOUT(agent_timeout)`
   - 网关关闭 → `CANCELLED(gateway_shutdown)`
   - 其他 → `FAILED(agent_stream_interrupted)`
7. 只有 CAS 成功的路径计数指标，保证“终态只出现一次”；
8. 顶层 `recover` 兜底：任务 `FAILED(internal_panic)`，取消 ctx，排空并关闭事件流。

事件通道由 Adapter 唯一关闭；Executor 只读。事件扇出层的慢消费者会被断开（缓冲满），不影响任务本身。

## 6. HTTP 与 SSE

- `POST /api/v1/tasks` 校验 JSON、Agent 路由、队列容量，返回 `201 {task_id,status}`。
- `GET /api/v1/tasks/{id}`、`/result` 读取内存 Repository。
- `GET /api/v1/tasks/{id}/events`：先回放历史事件，再转入实时订阅；定期写 SSE 注释心跳；任务终态后服务端关闭连接。
  - 不支持 `Last-Event-ID` 从 `sequence 0` 之外断点续传；客户端重连会重新收到完整历史（在保留期内）。
- `POST /api/v1/tasks/{id}/cancel`：`QUEUED` 立即 `CANCELLED`；`RUNNING` 返回受理并异步取消；终态返回 `409 TASK_ALREADY_FINISHED`。
- `GET /health` 在过载时仍返回 200（`accepting` 表示是否还接收提交）。
- `GET /metrics` 为轻量 JSON，不加锁聚合。

## 7. 优雅关闭

信号触发后由 `main` 串行执行（单一关闭者）：

1. `Manager.BeginShutdown()`：关闭提交门禁，新任务 `429 GATEWAY_SHUTTING_DOWN`；
2. `api.BeginShutdown()`：通知所有 SSE handler 主动结束连接；
3. `Manager.Shutdown(deadline)`：
   - 所有 `QUEUED` 任务置 `CANCELLED(gateway_shutdown)` 并关闭其事件流；
   - 等待 `RUNNING` 任务自然结束，直到 `shutdown_deadline_seconds`；
   - 超时后 `CancelAll()` 取消剩余任务 ctx；
4. `worker.Pool.Stop()`：关闭调度器，Worker 从 `Next()` 返回并退出；
5. `http.Server.Shutdown()`：等待在途请求；
6. `Manager.Close()`：停止 janitor 与事件管理器。

所有 `close` 都有唯一所有者：事件 channel 由 Adapter 关，订阅 channel 由 Event Manager 关，调度器 `done` 由 Scheduler 关。

## 8. 内存与资源

- 任务、事件、指标全部在进程内存中；`event_retention_minutes` 到期后由 janitor 清理终态任务与其事件。
- 事件保留期内，SSE 可回放；不持久化、不跨进程共享。
- 每任务 goroutine 数：1 个 Worker + 1 个 Executor 事件泵 + 1 个 Adapter 内部 goroutine（Local/HTTP 各一）+ 可能的 HTTP 连接读写 goroutine；事件订阅者按连接计。
- `/debug/pprof` 默认关闭，`enable_pprof: true` 时挂载。

## 9. 设计取舍

| 选择 | 原因 |
|---|---|
| Worker 数 = 全局并发 | 并发上限可静态推理，goroutine 数量可预测，避免无界任务 goroutine |
| 调度器只持有不透明 Job | 避免 scheduler → task 反向依赖，保持单向分层 |
| 终态 CAS 在 Repository | 取消/完成/超时竞争只有一处裁决点，天然幂等 |
| 事件 `sequence` 由网关分配 | Agent 无状态、可重试；序号权威唯一 |
| 不做持久化/分布式 | 单进程轻量网关，能力边界明确写入 README |
