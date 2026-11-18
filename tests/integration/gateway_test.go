package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"goagent-lite/internal/agent"
	"goagent-lite/internal/agent/adapters"
	"goagent-lite/internal/api"
	"goagent-lite/internal/event"
	"goagent-lite/internal/executor"
	"goagent-lite/internal/metrics"
	"goagent-lite/internal/repository"
	"goagent-lite/internal/scheduler"
	"goagent-lite/internal/task"
	"goagent-lite/internal/worker"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
	)
}

// blockingAdapter never completes on its own; it ends when its context is
// cancelled. It records how many tasks were started concurrently.
type blockingAdapter struct {
	running atomic.Int64
	peak    atomic.Int64
	started chan struct{}
	once    sync.Once
}

func newBlockingAdapter() *blockingAdapter {
	return &blockingAdapter{started: make(chan struct{})}
}

func (b *blockingAdapter) Start(ctx context.Context, req agent.AgentRequest) (<-chan agent.AgentEvent, error) {
	n := b.running.Add(1)
	for {
		p := b.peak.Load()
		if n <= p || b.peak.CompareAndSwap(p, n) {
			break
		}
	}
	ch := make(chan agent.AgentEvent, 1)
	ch <- agent.AgentEvent{Type: agent.EventStarted}
	b.once.Do(func() { close(b.started) })
	go func() {
		defer b.running.Add(-1)
		defer close(ch)
		<-ctx.Done()
	}()
	return ch, nil
}

type harness struct {
	t      *testing.T
	server *httptest.Server
	mgr    *task.Manager
	pool   *worker.Pool
	api    *api.Server
	exec   *executor.Executor
	met    *metrics.Metrics
}

type harnessOptions struct {
	globalWorkers int
	globalQueue   int
	defs          []agent.Definition
}

func newHarness(t *testing.T, opts harnessOptions) *harness {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg, err := agent.NewRegistry(opts.defs)
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewMemory()
	events := event.New(64)
	met := metrics.New()
	sched := scheduler.New(opts.globalQueue)
	exec := executor.New(repo, events, reg, met, executor.Options{DefaultTimeout: 5 * time.Second, Logger: logger})
	mgr, err := task.NewManager(task.ManagerOptions{
		QueueCapacity:         opts.globalQueue,
		DefaultTimeoutSeconds: 30,
		EventRetention:        time.Minute,
		Logger:                logger,
	}, repo, reg, sched, events, met, exec)
	if err != nil {
		t.Fatal(err)
	}
	pool := worker.New(opts.globalWorkers, sched, exec, met, logger)
	pool.Start()
	apiSrv := api.New(mgr, logger, api.Options{
		MaxRequestBodyBytes: 1 << 20,
		SSEHeartbeat:        time.Second,
		WorkersConfigured:   opts.globalWorkers,
	})
	ts := httptest.NewServer(apiSrv.Handler())
	h := &harness{t: t, server: ts, mgr: mgr, pool: pool, api: apiSrv, exec: exec, met: met}
	t.Cleanup(func() {
		apiSrv.BeginShutdown()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		mgr.Shutdown(ctx)
		pool.Stop()
		pool.Wait()
		mgr.Close()
		ts.Close()
	})
	return h
}

func localDef(id string, maxConcurrency int, duration time.Duration, queueCapacity int) agent.Definition {
	return agent.Definition{
		ID: id, Version: "v1", Protocol: "local", Enabled: true,
		MaxConcurrency: maxConcurrency, QueueCapacity: queueCapacity, TimeoutSeconds: 10,
		Adapter: newLocalAdapter(duration),
	}
}

func newLocalAdapter(d time.Duration) agent.AgentAdapter {
	return adapters.NewLocal(adapters.LocalOptions{
		Duration: d, Tokens: 2, TokenInterval: time.Millisecond,
	})
}

func (h *harness) post(path string, body any) (*http.Response, map[string]any) {
	h.t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		h.t.Fatal(err)
	}
	resp, err := http.Post(h.server.URL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	raw, _ := io.ReadAll(resp.Body)
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			h.t.Fatalf("decode response %q: %v", raw, err)
		}
	}
	return resp, out
}

func (h *harness) get(path string) (*http.Response, map[string]any) {
	h.t.Helper()
	resp, err := http.Get(h.server.URL + path)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	raw, _ := io.ReadAll(resp.Body)
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			h.t.Fatalf("decode response %q: %v", raw, err)
		}
	}
	return resp, out
}

func (h *harness) submit(agentID string, input string) (string, int) {
	h.t.Helper()
	resp, body := h.post("/api/v1/tasks", map[string]any{
		"agent_id": agentID, "agent_version": "v1", "input": json.RawMessage(input),
	})
	if resp.StatusCode != 201 {
		return "", resp.StatusCode
	}
	id, _ := body["task_id"].(string)
	if id == "" {
		h.t.Fatalf("missing task_id in %v", body)
	}
	return id, resp.StatusCode
}

func (h *harness) waitStatus(id string, want task.Status, timeout time.Duration) map[string]any {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, body := h.get("/api/v1/tasks/" + id)
		if body["status"] == string(want) {
			return body
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, body := h.get("/api/v1/tasks/" + id)
	h.t.Fatalf("task %s did not reach %s: %v", id, want, body)
	return nil
}

func TestSubmitAndComplete(t *testing.T) {
	h := newHarness(t, harnessOptions{globalWorkers: 4, globalQueue: 16, defs: []agent.Definition{
		localDef("echo", 2, 20*time.Millisecond, 0),
	}})
	id, code := h.submit("echo", `{"hello":"world"}`)
	if code != 201 {
		t.Fatalf("submit status = %d", code)
	}
	body := h.waitStatus(id, task.StatusSucceeded, 2*time.Second)
	if body["agent_id"] != "echo" || body["agent_version"] != "v1" {
		t.Fatalf("task view = %v", body)
	}
	resp, result := h.get("/api/v1/tasks/" + id + "/result")
	if resp.StatusCode != 200 || result["status"] != "SUCCEEDED" {
		t.Fatalf("result = %d %v", resp.StatusCode, result)
	}
	if result["result"] == nil {
		t.Fatalf("missing result payload: %v", result)
	}
}

func TestValidationErrors(t *testing.T) {
	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 4, defs: []agent.Definition{
		localDef("echo", 1, 10*time.Millisecond, 0),
		{ID: "off", Version: "v1", Protocol: "local", Enabled: false, MaxConcurrency: 1, TimeoutSeconds: 5,
			Adapter: newLocalAdapter(0)},
	}})
	cases := []struct {
		name string
		body map[string]any
		code string
		http int
	}{
		{"missing agent", map[string]any{"input": map[string]any{}}, "INVALID_REQUEST", 400},
		{"bad input", map[string]any{"agent_id": "echo", "agent_version": "v1", "input": []int{1}}, "INVALID_REQUEST", 400},
		{"unknown agent", map[string]any{"agent_id": "nope", "agent_version": "v1", "input": map[string]any{}}, "AGENT_NOT_FOUND", 404},
		{"disabled agent", map[string]any{"agent_id": "off", "agent_version": "v1", "input": map[string]any{}}, "AGENT_DISABLED", 409},
		{"negative timeout", map[string]any{"agent_id": "echo", "agent_version": "v1", "input": map[string]any{}, "config": map[string]any{"timeout_seconds": -5}}, "INVALID_REQUEST", 400},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := h.post("/api/v1/tasks", tc.body)
			if resp.StatusCode != tc.http {
				t.Fatalf("status = %d, want %d (%v)", resp.StatusCode, tc.http, body)
			}
			errObj, _ := body["error"].(map[string]any)
			if errObj["code"] != tc.code {
				t.Fatalf("code = %v, want %s", errObj["code"], tc.code)
			}
		})
	}

	resp, _ := h.get("/api/v1/tasks/does-not-exist")
	if resp.StatusCode != 404 {
		t.Fatalf("unknown task status = %d", resp.StatusCode)
	}
}

func TestEventStreamReplayAndClose(t *testing.T) {
	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 4, defs: []agent.Definition{
		localDef("echo", 1, 20*time.Millisecond, 0),
	}})
	id, _ := h.submit("echo", `{}`)
	h.waitStatus(id, task.StatusSucceeded, 2*time.Second)

	resp, err := http.Get(h.server.URL + "/api/v1/tasks/" + id + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	events := readSSE(t, resp.Body, 2*time.Second)
	if len(events) < 3 {
		t.Fatalf("events = %d", len(events))
	}
	if events[0]["type"] != agent.EventStarted {
		t.Fatalf("first = %v", events[0])
	}
	last := events[len(events)-1]
	if last["type"] != agent.EventCompleted {
		t.Fatalf("last = %v", last)
	}
	for i, ev := range events {
		if int(ev["sequence"].(float64)) != i+1 {
			t.Fatalf("sequence[%d] = %v", i, ev["sequence"])
		}
		if ev["task_id"] != id {
			t.Fatalf("task_id = %v", ev["task_id"])
		}
	}
}

func readSSE(t *testing.T, r io.Reader, timeout time.Duration) []map[string]any {
	t.Helper()
	type result struct {
		events []map[string]any
		err    error
	}
	done := make(chan result, 1)
	go func() {
		var out []map[string]any
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var ev map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
				done <- result{err: err}
				return
			}
			out = append(out, ev)
		}
		done <- result{events: out, err: sc.Err()}
	}()
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatal(res.err)
		}
		return res.events
	case <-time.After(timeout):
		t.Fatal("timed out reading SSE stream")
		return nil
	}
}

func TestCancelQueuedTask(t *testing.T) {
	blocker := newBlockingAdapter()
	defs := []agent.Definition{{
		ID: "block", Version: "v1", Protocol: "local", Enabled: true,
		MaxConcurrency: 1, TimeoutSeconds: 10, Adapter: blocker,
	}}
	h := newHarness(t, harnessOptions{globalWorkers: 4, globalQueue: 16, defs: defs})
	first, _ := h.submit("block", `{}`)
	select {
	case <-blocker.started:
	case <-time.After(time.Second):
		t.Fatal("first task never started")
	}
	second, _ := h.submit("block", `{}`)

	// Subscribing to the queued task must yield an open stream that ends when
	// the cancellation closes it.
	resp, err := http.Get(h.server.URL + "/api/v1/tasks/" + second + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	resp2, body := h.post("/api/v1/tasks/"+second+"/cancel", nil)
	if resp2.StatusCode != 200 || body["status"] != "CANCELLED" {
		t.Fatalf("cancel = %d %v", resp2.StatusCode, body)
	}
	if got := readSSE(t, resp.Body, 2*time.Second); len(got) != 0 {
		t.Fatalf("cancelled queued task produced events: %v", got)
	}
	view := h.waitStatus(second, task.StatusCancelled, time.Second)
	if view["error_code"] != agent.ErrCodeTaskCancelled {
		t.Fatalf("error_code = %v", view["error_code"])
	}

	// Cancelling a terminal task is a conflict.
	resp3, body3 := h.post("/api/v1/tasks/"+second+"/cancel", nil)
	if resp3.StatusCode != 409 {
		t.Fatalf("second cancel = %d %v", resp3.StatusCode, body3)
	}

	// Cancel the running task; it converges to CANCELLED asynchronously.
	if _, body := h.post("/api/v1/tasks/"+first+"/cancel", nil); body["status"] != "RUNNING" {
		t.Fatalf("running cancel response = %v", body)
	}
	h.waitStatus(first, task.StatusCancelled, 2*time.Second)
}

func TestAgentConcurrencyLimit(t *testing.T) {
	blocker := newBlockingAdapter()
	defs := []agent.Definition{{
		ID: "limited", Version: "v1", Protocol: "local", Enabled: true,
		MaxConcurrency: 2, TimeoutSeconds: 10, Adapter: blocker,
	}}
	h := newHarness(t, harnessOptions{globalWorkers: 8, globalQueue: 32, defs: defs})
	ids := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		id, code := h.submit("limited", fmt.Sprintf(`{"i":%d}`, i))
		if code != 201 {
			t.Fatalf("submit %d status = %d", i, code)
		}
		ids = append(ids, id)
	}
	deadline := time.Now().Add(2 * time.Second)
	for blocker.running.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if got := blocker.running.Load(); got != 2 {
		t.Fatalf("running = %d, want 2", got)
	}
	if peak := blocker.peak.Load(); peak != 2 {
		t.Fatalf("peak = %d, want 2", peak)
	}
	_, metricsBody := h.get("/metrics")
	queues, _ := metricsBody["queues"].(map[string]any)
	agents, _ := queues["agents"].([]any)
	if len(agents) != 1 {
		t.Fatalf("agent stats = %v", agents)
	}
	stat := agents[0].(map[string]any)
	if stat["running"].(float64) != 2 || stat["queued"].(float64) != 4 || stat["max_concurrency"].(float64) != 2 {
		t.Fatalf("agent stat = %v", stat)
	}
	for _, id := range ids {
		h.post("/api/v1/tasks/"+id+"/cancel", nil)
	}
	for _, id := range ids {
		h.waitStatus(id, task.StatusCancelled, 2*time.Second)
	}
}

func TestAgentQueueQuota(t *testing.T) {
	blocker := newBlockingAdapter()
	defs := []agent.Definition{{
		ID: "small", Version: "v1", Protocol: "local", Enabled: true,
		MaxConcurrency: 1, QueueCapacity: 1, TimeoutSeconds: 10, Adapter: blocker,
	}}
	h := newHarness(t, harnessOptions{globalWorkers: 4, globalQueue: 16, defs: defs})
	first, _ := h.submit("small", `{}`)
	select {
	case <-blocker.started:
	case <-time.After(time.Second):
		t.Fatal("first task never started")
	}
	if _, code := h.submit("small", `{}`); code != 201 {
		t.Fatalf("second submit = %d", code)
	}
	resp, body := h.post("/api/v1/tasks", map[string]any{"agent_id": "small", "agent_version": "v1", "input": map[string]any{}})
	if resp.StatusCode != 429 {
		t.Fatalf("third submit = %d %v", resp.StatusCode, body)
	}
	errObj, _ := body["error"].(map[string]any)
	if errObj["code"] != "AGENT_QUOTA_EXCEEDED" {
		t.Fatalf("code = %v", errObj["code"])
	}
	h.post("/api/v1/tasks/"+first+"/cancel", nil)
	h.waitStatus(first, task.StatusCancelled, 2*time.Second)
}

func TestShutdownRejectsAndCancels(t *testing.T) {
	blocker := newBlockingAdapter()
	defs := []agent.Definition{{
		ID: "block", Version: "v1", Protocol: "local", Enabled: true,
		MaxConcurrency: 1, TimeoutSeconds: 60, Adapter: blocker,
	}}
	h := newHarness(t, harnessOptions{globalWorkers: 4, globalQueue: 8, defs: defs})
	running, _ := h.submit("block", `{}`)
	select {
	case <-blocker.started:
	case <-time.After(time.Second):
		t.Fatal("task never started")
	}
	h.mgr.BeginShutdown()
	resp, body := h.post("/api/v1/tasks", map[string]any{"agent_id": "block", "agent_version": "v1", "input": map[string]any{}})
	if resp.StatusCode != 429 {
		t.Fatalf("submit during shutdown = %d %v", resp.StatusCode, body)
	}
	// Health must remain available while overloaded/shutting down.
	if resp, _ := h.get("/health"); resp.StatusCode != 200 {
		t.Fatalf("health during shutdown = %d", resp.StatusCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	h.mgr.Shutdown(ctx)
	h.pool.Stop()
	h.pool.Wait()
	view := h.waitStatus(running, task.StatusCancelled, 2*time.Second)
	if view["error_code"] != agent.ErrCodeGatewayShutdown {
		t.Fatalf("error_code = %v", view["error_code"])
	}
}

func TestMetricsAndHealth(t *testing.T) {
	h := newHarness(t, harnessOptions{globalWorkers: 3, globalQueue: 9, defs: []agent.Definition{
		localDef("echo", 2, 10*time.Millisecond, 0),
	}})
	id, _ := h.submit("echo", `{}`)
	h.waitStatus(id, task.StatusSucceeded, 2*time.Second)

	resp, health := h.get("/health")
	if resp.StatusCode != 200 || health["status"] != "ok" {
		t.Fatalf("health = %d %v", resp.StatusCode, health)
	}
	resp, m := h.get("/metrics")
	if resp.StatusCode != 200 {
		t.Fatalf("metrics status = %d", resp.StatusCode)
	}
	tasks, _ := m["tasks"].(map[string]any)
	if tasks["submitted"].(float64) < 1 || tasks["succeeded"].(float64) < 1 {
		t.Fatalf("tasks metrics = %v", tasks)
	}
	workers, _ := m["workers"].(map[string]any)
	if workers["configured"].(float64) != 3 || workers["alive"].(float64) != 3 {
		t.Fatalf("workers = %v", workers)
	}
	if m["goroutines"].(float64) < 3 {
		t.Fatalf("goroutines = %v", m["goroutines"])
	}
}
