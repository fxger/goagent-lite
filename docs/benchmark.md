# 压测报告

本报告验证 goagent-lite 在 100 / 300 / 500 / 800 / 1000 个任务规模下的吞吐、延迟、
排队行为、并发上限、goroutine、内存与 CPU 开销。重点不是“提交了多少任务”，而是
**同一时刻真正执行了多少任务**：以 `/metrics` 的 `running`、Worker 存活数和每个任务的
`execution_duration_ms` 为准。

## 1. 结论摘要

- 所有规模成功率 **100%**，无超时、无失败、无任务丢失。
- `running` 峰值严格等于配置的 Worker 数（100/300/500），`workers_alive` 与配置一致；
  超出并发的任务留在队列中，没有被“偷偷”执行。
- 500 并发以内，排队时间 p99 ≤ 0.6ms，端到端延迟几乎等于 Agent 执行时长；
  800/1000 任务时排队 p95 约 0.89s/1.32s，符合“500 个并发槽 + 有界队列”的预期。
- goroutine 峰值 = Worker 数 + 每个运行中任务的 Executor 事件泵 + Adapter goroutine + HTTP 服务少量固定开销，
  实测 500 并发时为 **1508–1509**（模型 `3×500`，另有约 9 个 HTTP/运行时固定开销），任务结束后回落到 714–765，无泄漏（goleak 通过）。
- 网关自身 CPU 开销很低：500 并发下进程 CPU 约 0.44s（2.22s 墙钟，约单核 20%），
  因为 mock Agent 的耗时是 `time.Sleep` 而非忙等；堆峰值 < 10MB。

## 2. 测试环境

| 项目 | 值 |
|---|---|
| 机器 | HP ZBook Power 15.6 inch G10 Mobile Workstation PC |
| CPU | 13th Gen Intel Core i5-13500H，12 核 / 16 线程，2.6GHz |
| 内存 | 31.6 GB |
| 操作系统 | Windows 11 家庭版 中文版，10.0.26200 |
| Go | go1.27.3 windows/amd64 |
| 磁盘 | D: NTFS |
| 测试形态 | 单进程内嵌网关 + 本地 mock Agent + HTTP 客户端（httptest） |

单机、共享笔记本环境，未做 CPU 隔离或容器资源限制；后台系统活动会影响 CPU 数值，
因此 CPU 只作为量级参考，延迟与并发上限是更稳定的指标。

## 3. Mock Agent 行为

`tests/benchmark/load_test.go` 使用 `LocalAdapter`，行为固定为：

- 执行时长：`100ms + uniform(0, 1900ms)`，即 **100ms–2s 均匀分布**，禁止零耗时；
- 每任务发送 3 个 token 事件，间隔 20ms；
- 失败率 0，panic 率 0，任务超时 60s；
- 事件通道与真实 Adapter 相同（有界 channel），经过完整的 Executor → Event Manager 路径；
- Agent 的 `max_concurrency` 等于 Worker 数，因此并发上限由 Worker 数决定。

## 4. 负载方法

1. 启动内嵌网关，Worker 数 = 配置值，全局队列容量 = `tasks + workers`，避免提交被 429；
2. 用同一个 `http.Client` **顺序**提交全部任务（提交速率受本机 HTTP 往返限制，不是网关瓶颈）；
3. `wall` 从第一个提交开始，到 `/metrics` 显示全部任务终态且 `running=queued=0` 为止；
4. 提交与等待期间每 10ms 采样 `runtime.NumGoroutine()`、`HeapAlloc` 和 `running` 峰值；
5. CPU 通过操作系统计数器读取（Windows `GetProcessTimes`，Linux `getrusage`），
   统计整个测试进程的 user+kernel 时间，因此**包含测试用 HTTP 客户端和 mock Agent 自身**，
   是偏保守的上界；
6. 结束后逐任务读取 `GET /api/v1/tasks/{id}`，从 `created_at/finished_at` 计算端到端延迟，
   直接使用网关返回的 `queue_duration_ms` / `execution_duration_ms` 统计分位值。

延迟分位值采用“排序后取 `floor((n-1)*p)`”的最近秩方法。

## 5. 结果

每档均多次运行以排除偶发系统负载；下表是最终代码下的一次代表性运行，
不同轮次之间延迟差异通常在 ±5% 内（提交速率受本机 HTTP 与后台活动影响更大）。
`CPU` 列为进程 user+kernel 时间；`单核%` = CPU / wall；`整机%` = CPU / wall / 16。

| 任务/Worker | 成功 | wall (s) | 提交/s | 完成/s | 延迟 p50 | p95 | p99 | 排队 p95 | 排队 max | 执行 p50 | 执行 p99 | RUNNING 峰值 | goroutine 峰值/结束 | CPU (s) | 单核% | 整机% | Heap 峰值 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 / 100 | 100/100 | 2.09 | 3400 | 48 | 1021 | 1858 | 1967 | 0.6 | 0.7 | 1021 | 1967 | 100 | 308 / 332 | 0.13 | 6.0% | 0.4% | 3.23 MB |
| 300 / 300 | 300/300 | 2.15 | 2249 | 140 | 1061 | 1984 | 2050 | 0.5 | 1.0 | 1061 | 2049 | 300 | 909 / 523 | 0.50 | 23.3% | 1.5% | 5.32 MB |
| 500 / 500 | 500/500 | 2.22 | 2988 | 226 | 1110 | 1948 | 2034 | 0.0 | 1.8 | 1110 | 2034 | 500 | 1508 / 714 | 0.44 | 19.7% | 1.2% | 7.94 MB |
| 800 / 500 | 800/800 | 3.16 | 4349 | 254 | 1319 | 2390 | 2795 | 891.0 | 992.8 | 1084 | 2042 | 500 | 1509 / 765 | 0.94 | 29.7% | 1.9% | 9.78 MB |
| 1000 / 500 | 1000/1000 | 3.68 | 3898 | 272 | 1534 | 2856 | 3206 | 1319.1 | 1405.7 | 1126 | 2045 | 500 | 1509 / 720 | 0.91 | 24.6% | 1.5% | 9.71 MB |

单位：延迟/排队/执行均为毫秒。

## 6. 解读

### 6.1 并发上限

800 与 1000 任务档的 `RUNNING` 峰值固定为 500，`workers_alive=500`；
其余任务在队列中等待，`queue_ms` 随压力上升（p95 891ms → 1319ms）。
没有出现 `RUNNING > 500`，也没有 Worker 空闲而队列非空的情况。

### 6.2 延迟构成

- 100–500 任务档：排队 p95 接近 0，延迟 p50 基本等于执行 p50（约 1.0–1.1s），
  p99 接近 mock 的 2s 上限，符合均匀分布；
- 800/1000 任务档：多出的任务排队约 0.9–1.4s，因此端到端 p95 上升到 2.4–2.9s，
  但执行时长分位值仍保持在 2s 左右，说明延迟增量来自排队而非执行变慢。

### 6.3 goroutine 账目

每个 RUNNING 任务固定占用 3 个 goroutine：Worker、Executor 事件泵、Adapter 内部 goroutine。
压测跑满时 RUNNING 数等于 Worker 数，因此模型就是 `3×workers = workers×2 + agentConcurrency`
（压测输出中的 `formula_expected_at_peak` 即此值）；实测比模型多出的约 9 个 goroutine 属于
HTTP 服务与运行时固定开销，不进入模型。

| Worker | 理论模型（3×workers） | 实测峰值（含 ~9 固定开销） | 结束后 |
|---:|---:|---:|---:|
| 100 | 300 | 308 | 332 |
| 300 | 900 | 909 | 523 |
| 500 | 1500 | 1508–1509 | 714–765 |

提交端与轮询端复用 keep-alive 连接，因此结束后 goroutine 数（523–765）高于 Worker 数，
但会随空闲连接回收而下降；`go.uber.org/goleak` 在所有测试结束时未发现泄漏。压测同时断言
`RUNNING 峰值 ≤ Worker 数` 且 `goroutine 峰值 ≤ workers×2 + agent 并发 + 100`。

### 6.4 内存与 CPU

- 堆峰值随任务对象、事件历史和测试端 JSON 缓冲增长，1000 任务时约 9.7MB；
  任务完成后由 janitor（`event_retention_minutes`）清理。
- CPU 值很小且受 Windows 15.6ms 计时粒度影响（100 任务档仅数个计时 tick），
  应视为“网关不是瓶颈”的量级证据，而非精确基准。mock Agent 的耗时是 sleep，
  所以进程主要开销在 HTTP/JSON 与调度，而不是业务计算。

### 6.5 四类状态说明

| 状态 | 本测试中的含义 | 观测值 |
|---|---|---|
| 已提交 | 收到 201 的任务数 | 100 / 300 / 500 / 800 / 1000 |
| 正在执行 | `RUNNING`，占用 Worker 与 Agent 配额 | 峰值 = min(任务数, Worker 数) |
| I/O 等待 | mock 的 `time.Sleep`，goroutine 可运行但不在 CPU 上 | 运行中任务的绝大部分时间 |
| CPU 密集 | 本测试无 CPU 密集任务；网关编解码/调度只占很小比例 | CPU 0.13–0.94s |

因此本报告不会把“创建了 500 个任务”当作“500 并发”：并发由 `RUNNING`、Worker 数和
执行时长共同证明。

## 7. 复现

```bash
GOAGENT_BENCH=1 BENCH_TASKS=500 BENCH_WORKERS=500 \
  go test ./tests/benchmark -run TestLoad -count=1 -v
```

输出中 `BENCHMARK_REPORT_BEGIN ... END` 之间是完整 JSON 报告，包含所有分位值、
`status_counts`、goroutine/内存/CPU 字段。原始输出保存在开发机的
`D:\Desktop\gateway\tools\bench{100,300,500,800,1000}e.txt`（不随仓库交付）。

## 8. 局限

- 单机、单进程、共享操作系统，未做 cgroup/容器隔离；
- 网关、mock Agent 与 HTTP 客户端在同一进程，CPU/内存包含测试脚手架，数值偏保守；
- 未覆盖慢客户端、SSE 长连接占满、磁盘/网络抖动等场景；这些由
  `tests/integration/faults_test.go` 的功能性故障用例覆盖，而非吞吐基准；
- 压测数据不构成容量承诺，生产容量需按真实 Agent 时长、事件速率和部署环境重新测量。
