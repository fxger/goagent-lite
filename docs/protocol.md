# goagent-lite Agent 接入协议（HTTP/SSE）

版本：v1（对应任务书 v2 §4.5 定稿）
状态：稳定，Phase 3 起网关与 `examples/mock-agent` 均按本文实现。

goagent-lite 不感知 Agent 内部实现（ReAct、Plan & Execute、LangGraph、自研逻辑均可以）。网关只通过本文定义的 HTTP/SSE 契约管理任务的启动、事件流、终态、超时与取消。

---

## 1. 端点

```text
POST {endpoint}/v1/execute
Content-Type: application/json
Accept: text/event-stream
```

`endpoint` 在 `configs/agents.yaml` 的 Agent 配置中给出，例如 `http://127.0.0.1:9101`。网关会拼接固定路径 `/v1/execute`；Agent 不需要提供其他业务端点（健康检查自行决定，网关不调用）。

## 2. 请求

```json
{
  "task_id": "task-9f2c1d3e4b5a",
  "input": { "query": "分析当前 RAG 技术的发展趋势" },
  "config": { "timeout_seconds": 30 },
  "metadata": { "user_id": "10001", "trace_id": "trace-001" }
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `task_id` | string | 网关生成，全局唯一。Agent 不得自造或修改，应原样用于日志与最终结果。 |
| `input` | JSON 对象 | 业务输入。网关只校验 JSON 合法性与大小上限，不解释字段。 |
| `config` | JSON 对象 | 网关通用执行配置。当前仅透传 `timeout_seconds`，Agent 可忽略。 |
| `metadata` | map[string]string | 透传字段，可能为空对象或省略。 |

请求体上限由网关 `max_request_body_bytes`（默认 1 MiB）约束。

## 3. 成功响应

返回 `200`，`Content-Type: text/event-stream`，随后逐条输出 SSE 事件：

```text
data: {"type":"started","timestamp":"2026-11-07T10:00:00Z","payload":{}}

data: {"type":"token","timestamp":"2026-11-07T10:00:00.120Z","payload":{"text":"分析"}}

data: {"type":"completed","timestamp":"2026-11-07T10:00:01Z","payload":{"summary":"..."}}

```

Agent 在最后一条 `completed` 或 `failed` 之后关闭连接。

### 3.1 事件 JSON

每个 `data:` 行是一个独立 JSON 对象，只含三个字段：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `type` | string | 是 | 事件类型，见下表。 |
| `timestamp` | RFC 3339 string | 否 | 事件发生时间；缺失时网关用接收时间补全。 |
| `payload` | 任意 JSON | 否 | 事件负载；`completed` 时即任务最终结果。 |

事件中**不得**包含 `task_id` 与 `sequence`：二者由网关补全，`sequence` 是网关侧唯一序号权威，按事件到达顺序分配。

SSE 注释行（以 `:` 开头）与空行被忽略；其他非 `data:` 行被忽略。网关要求每个事件是单独一行 `data:`，不做多行 `data:` 拼接。

### 3.2 事件类型

| 类型 | 分类 | 说明 |
|---|---|---|
| `started` | 中间 | Agent 开始执行 |
| `message` | 中间 | 普通文本消息 |
| `token` | 中间 | 流式 Token |
| `tool_call` | 中间 | 工具调用 |
| `tool_result` | 中间 | 工具返回 |
| `progress` | 中间 | 执行进度 |
| `error` | 中间 | 可恢复错误（流继续，任务不终止） |
| `completed` | **终态** | 执行完成，`payload` 即最终输出 |
| `failed` | **终态** | 执行失败，`payload` 携带错误信息 |
| `custom` | 中间 | 自定义事件，`payload` 任意 |

不在表内的 `type` 会被网关映射为 `custom`，不会导致任务失败。

### 3.3 终态契约（硬性）

1. 事件流的**最后一条事件**必须是 `completed` 或 `failed`，随后关闭连接。
2. `completed` 的 `payload` 就是任务最终结果（`GET /api/v1/tasks/{id}/result` 返回该值）。
3. `failed` 的 `payload` 建议包含 `error_code` 与 `message`：

   ```json
   {"type":"failed","timestamp":"...","payload":{"error_code":"tool_timeout","message":"search tool timed out"}}
   ```

   网关原样保留 `error_code`/`message`（也兼容 `code` 字段），任务置 `FAILED`。
4. 收到终态事件后网关即结束消费并关闭连接，不再读取后续字节。
5. `error` 是中间事件：收到后流继续，任务不终止；Agent 若最终失败，必须在流末发送 `failed`。

### 3.4 网关补发的终态事件

网关自身终止任务时（用户取消、超时、启动失败、流中断、panic），会在关闭事件流前向**订阅者**补发一条 `failed` 事件，`payload.error_code` 给出真实原因：

| 场景 | `error_code` |
|---|---|
| 用户取消 RUNNING / QUEUED 任务 | `task_cancelled` |
| 网关优雅关闭 | `gateway_shutdown` |
| 超过 `timeout_seconds` | `agent_timeout` |
| 连接失败 / 拒绝、HTTP 非 200 | `agent_unavailable` / `agent_error` |
| 流中断、协议违规 | `agent_stream_interrupted` / `agent_protocol_violation` |

这条事件由网关生成，不是 Agent 发出的，所以 Agent 不需要、也不应该在取消时补发终态事件（见 §4）。它只表示事件流终结：任务真实状态以 `GET /api/v1/tasks/{id}` 为准，`CANCELLED`、`TIMEOUT` 不会因为事件类型叫 `failed` 而改变；`completed` 仍然只有 Agent 能发出。

## 4. 取消与超时

- **取消 = 网关断开连接**。没有显式 cancel 接口。Agent 必须监听请求的 `Context().Done()`（`r.Context()`），在客户端断开后立即停止计算、停止写流并释放资源。
- 网关在以下情况断开连接：用户取消 RUNNING 任务、`timeout_seconds` 到期、网关优雅关闭。
- Agent 无需在取消时补发终态事件；网关按自己的取消/超时语义决定任务状态，并向订阅者补发 §3.4 所述的 `failed` 事件。若流因取消关闭且没有终态，任务不会被误判为 `agent_stream_interrupted`。
- 连接建立后应尽快（亚秒级）返回 `200` 并开始输出；建连与首字节延迟受网关任务超时约束。

## 5. 错误映射

| Agent 侧现象 | 网关错误码 | 任务结果 | 分类 |
|---|---|---|---|
| 连接失败 / 拒绝 | `agent_unavailable` | FAILED | 启动失败（快速失败，不进入执行） |
| HTTP 非 200 | `agent_error` | FAILED | 启动失败 |
| `200` 但 `Content-Type` 不是 `text/event-stream` | `agent_protocol_violation` | FAILED | 运行失败 |
| 事件 JSON 非法或缺少 `type` | `agent_protocol_violation` | FAILED | 运行失败 |
| 流关闭且没有终态事件 | `agent_stream_interrupted` | FAILED | 运行失败 |
| `failed` 终态事件 | Agent 自述的 `error_code` | FAILED | 运行失败 |
| 超过 `timeout_seconds` | `agent_timeout` | TIMEOUT | 超时 |
| 网关主动取消 | `task_cancelled` / `gateway_shutdown` | CANCELLED | 取消 |

“启动失败”由 `AgentAdapter.Start` 直接返回 error；“运行失败”通过事件流中的 `failed` 事件表达。

## 6. 参考实现

`examples/mock-agent` 是本文的可执行参考实现，仅用标准库：

```bash
go run ./examples/mock-agent -addr :9101
go run ./examples/mock-agent -addr :9102 -fail-rate 0.1
go run ./examples/mock-agent -addr :9103 -mode abort      # 流中断
go run ./examples/mock-agent -addr :9104 -mode invalid    # 非法 JSON
go run ./examples/mock-agent -addr :9105 -mode http500    # HTTP 500
go run ./examples/mock-agent -addr :9106 -mode hang       # 超时
```

`-min-duration` / `-max-duration` 控制随机执行时长（默认 100ms–2s），`-tokens` / `-token-interval` 控制 Token 密度，`-fail-rate` 控制失败概率，`-mode` 注入故障。

## 7. 兼容性与演进

- 路径带 `/v1`；不兼容变更时新增 `/v2/execute`，网关按 Agent 配置选择。
- 新增事件类型对网关安全：未知类型降级为 `custom`。
- 新增请求字段也必须向后兼容；Agent 应忽略不认识的字段。
