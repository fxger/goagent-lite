package integration_test

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"goagent-lite/internal/agent"
	"goagent-lite/internal/agent/adapters"
	"goagent-lite/internal/task"
)

func remoteDef(id, endpoint string) agent.Definition {
	return agent.Definition{
		ID: id, Version: "v1", Protocol: "http", Enabled: true,
		MaxConcurrency: 4, TimeoutSeconds: 5,
		Adapter: adapters.NewHTTP(adapters.HTTPOptions{Endpoint: endpoint}),
	}
}

func (h *harness) submitTimeout(agentID string, timeoutSeconds int) (string, int) {
	h.t.Helper()
	resp, body := h.post("/api/v1/tasks", map[string]any{
		"agent_id": agentID, "agent_version": "v1",
		"input":  map[string]any{"x": 1},
		"config": map[string]any{"timeout_seconds": timeoutSeconds},
	})
	if resp.StatusCode != 201 {
		return "", resp.StatusCode
	}
	id, _ := body["task_id"].(string)
	return id, resp.StatusCode
}

func TestAgentUnavailableFastFail(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing is listening here any more

	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 8, defs: []agent.Definition{
		remoteDef("dead", "http://"+addr),
	}})
	start := time.Now()
	id, code := h.submit("dead", `{}`)
	if code != 201 {
		t.Fatalf("submit = %d", code)
	}
	body := h.waitStatus(id, task.StatusFailed, 2*time.Second)
	if body["error_code"] != "agent_unavailable" {
		t.Fatalf("error_code = %v, want agent_unavailable", body["error_code"])
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("fast failure took %v", d)
	}
}

func TestAgentHTTP500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 8, defs: []agent.Definition{
		remoteDef("err500", srv.URL),
	}})
	id, _ := h.submit("err500", `{}`)
	body := h.waitStatus(id, task.StatusFailed, 2*time.Second)
	if body["error_code"] != "agent_error" {
		t.Fatalf("error_code = %v", body["error_code"])
	}
	if msg, _ := body["error_message"].(string); !strings.Contains(msg, "500") {
		t.Fatalf("error_message = %q", msg)
	}
}

func TestAgentProtocolViolation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {this is not json}\n\n")
	}))
	defer srv.Close()
	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 8, defs: []agent.Definition{
		remoteDef("badjson", srv.URL),
	}})
	id, _ := h.submit("badjson", `{}`)
	body := h.waitStatus(id, task.StatusFailed, 2*time.Second)
	if body["error_code"] != "agent_protocol_violation" {
		t.Fatalf("error_code = %v", body["error_code"])
	}
}

func TestAgentStreamInterrupted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"type":"started","payload":{}}`+"\n\n")
		w.(http.Flusher).Flush()
		// Return without a terminal event: the stream is truncated.
	}))
	defer srv.Close()
	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 8, defs: []agent.Definition{
		remoteDef("abort", srv.URL),
	}})
	id, _ := h.submit("abort", `{}`)
	body := h.waitStatus(id, task.StatusFailed, 2*time.Second)
	if body["error_code"] != "agent_stream_interrupted" {
		t.Fatalf("error_code = %v", body["error_code"])
	}
}

func TestAgentTimeout(t *testing.T) {
	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 8, defs: []agent.Definition{{
		ID: "slow", Version: "v1", Protocol: "local", Enabled: true,
		MaxConcurrency: 2, TimeoutSeconds: 10,
		Adapter: adapters.NewLocal(adapters.LocalOptions{Duration: 5 * time.Second}),
	}}})
	id, _ := h.submitTimeout("slow", 1)
	start := time.Now()
	body := h.waitStatus(id, task.StatusTimeout, 3*time.Second)
	if body["error_code"] != "agent_timeout" {
		t.Fatalf("error_code = %v", body["error_code"])
	}
	// 上限放宽到 3.5s：-race 或慢速 CI 机器上调度延迟会拉长收敛时间，
	// 下界 800ms 仍足以排除“立即失败”，默认超时是配置里的 10s。
	if d := time.Since(start); d < 800*time.Millisecond || d > 3500*time.Millisecond {
		t.Fatalf("timeout after %v, want ~1s", d)
	}
}

func TestLocalPanicWorkerSurvives(t *testing.T) {
	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 8, defs: []agent.Definition{
		{ID: "panicky", Version: "v1", Protocol: "local", Enabled: true, MaxConcurrency: 2, TimeoutSeconds: 10,
			Adapter: adapters.NewLocal(adapters.LocalOptions{Duration: time.Millisecond, PanicRate: 1})},
		{ID: "healthy", Version: "v1", Protocol: "local", Enabled: true, MaxConcurrency: 2, TimeoutSeconds: 10,
			Adapter: adapters.NewLocal(adapters.LocalOptions{Duration: time.Millisecond})},
	}})
	id, _ := h.submit("panicky", `{}`)
	body := h.waitStatus(id, task.StatusFailed, 2*time.Second)
	if body["error_code"] != "internal_panic" {
		t.Fatalf("error_code = %v", body["error_code"])
	}
	// The worker must still be alive after the panic.
	okID, _ := h.submit("healthy", `{}`)
	h.waitStatus(okID, task.StatusSucceeded, 2*time.Second)
}

func (h *harness) openSSE(id string) (*http.Response, *bufio.Reader) {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodGet, h.server.URL+"/api/v1/tasks/"+id+"/events", nil)
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		h.t.Fatalf("sse status = %d", resp.StatusCode)
	}
	return resp, bufio.NewReader(resp.Body)
}

func readSSEEvent(rd *bufio.Reader) (map[string]any, error) {
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &ev); err != nil {
			return nil, err
		}
		return ev, nil
	}
}

func TestSSEClientDisconnectDoesNotCancelTask(t *testing.T) {
	blocker := newBlockingAdapter()
	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 8, defs: []agent.Definition{
		{ID: "blocker", Version: "v1", Protocol: "local", Enabled: true, MaxConcurrency: 2, TimeoutSeconds: 10, Adapter: blocker},
	}})
	id, _ := h.submit("blocker", `{}`)
	resp, rd := h.openSSE(id)
	if _, err := readSSEEvent(rd); err != nil {
		t.Fatalf("read first event: %v", err)
	}
	resp.Body.Close() // client walks away; the task must keep running

	time.Sleep(100 * time.Millisecond)
	_, body := h.get("/api/v1/tasks/" + id)
	if body["status"] != string(task.StatusRunning) {
		t.Fatalf("status = %v, want RUNNING after SSE disconnect", body["status"])
	}
	_, cancelBody := h.post("/api/v1/tasks/"+id+"/cancel", nil)
	if cancelBody["status"] != string(task.StatusCancelled) && cancelBody["status"] != string(task.StatusRunning) {
		t.Fatalf("cancel body = %v", cancelBody)
	}
	h.waitStatus(id, task.StatusCancelled, 2*time.Second)
}

func TestMultipleSSESubscribers(t *testing.T) {
	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 8, defs: []agent.Definition{
		localDef("multi", 2, 120*time.Millisecond, 0),
	}})
	id, _ := h.submit("multi", `{}`)

	var wg sync.WaitGroup
	seqs := make([][]float64, 2)
	terms := make([]string, 2)
	for i := 0; i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, rd := h.openSSE(id)
			defer resp.Body.Close()
			for {
				ev, err := readSSEEvent(rd)
				if err != nil {
					return
				}
				if s, ok := ev["sequence"].(float64); ok {
					seqs[i] = append(seqs[i], s)
				}
				if typ := ev["type"].(string); typ == "completed" || typ == "failed" {
					terms[i] = typ
					return
				}
			}
		}()
	}
	wg.Wait()
	for i := 0; i < 2; i++ {
		if terms[i] != "completed" {
			t.Fatalf("subscriber %d terminal = %q", i, terms[i])
		}
		if len(seqs[i]) < 2 {
			t.Fatalf("subscriber %d got %v", i, seqs[i])
		}
	}
	// Both subscribers must observe the same authoritative sequence numbers.
	if len(seqs[0]) != len(seqs[1]) {
		t.Fatalf("sequence lengths differ: %v vs %v", seqs[0], seqs[1])
	}
	for i := range seqs[0] {
		if seqs[0][i] != seqs[1][i] {
			t.Fatalf("sequences differ at %d: %v vs %v", i, seqs[0], seqs[1])
		}
	}
}

func TestCompletionStorm(t *testing.T) {
	const n = 100
	h := newHarness(t, harnessOptions{globalWorkers: 16, globalQueue: 200, defs: []agent.Definition{
		localDef("storm", 16, 5*time.Millisecond, 0),
	}})
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id, code := h.submit("storm", `{}`)
		if code != 201 {
			t.Fatalf("submit %d = %d", i, code)
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		h.waitStatus(id, task.StatusSucceeded, 10*time.Second)
	}
	// Let the last executors release their gauges.
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, body := h.get("/metrics")
		tasks := body["tasks"].(map[string]any)
		if tasks["running"].(float64) == 0 && tasks["queued"].(float64) == 0 {
			if tasks["succeeded"].(float64) < n {
				t.Fatalf("succeeded = %v < %d", tasks["succeeded"], n)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("gauges did not drain: %v", tasks)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCancelCompleteRace(t *testing.T) {
	h := newHarness(t, harnessOptions{globalWorkers: 8, globalQueue: 64, defs: []agent.Definition{
		localDef("race", 8, 2*time.Millisecond, 0),
	}})
	for i := 0; i < 40; i++ {
		id, code := h.submit("race", `{}`)
		if code != 201 {
			t.Fatalf("submit = %d", code)
		}
		_, cancelBody := h.post("/api/v1/tasks/"+id+"/cancel", nil)
		if st, _ := cancelBody["status"].(string); st != "" && st != string(task.StatusCancelled) && st != string(task.StatusRunning) {
			t.Fatalf("cancel body = %v", cancelBody)
		}
		deadline := time.Now().Add(2 * time.Second)
		for {
			_, body := h.get("/api/v1/tasks/" + id)
			st, _ := body["status"].(string)
			if st == string(task.StatusSucceeded) || st == string(task.StatusCancelled) {
				if st == string(task.StatusCancelled) {
					if ec, _ := body["error_code"].(string); ec != "task_cancelled" && ec != "gateway_shutdown" {
						t.Fatalf("cancelled error_code = %q", ec)
					}
				}
				break
			}
			if st == string(task.StatusFailed) || st == string(task.StatusTimeout) {
				t.Fatalf("unexpected terminal state %q", st)
			}
			if time.Now().After(deadline) {
				t.Fatalf("task %s stuck in %q", id, st)
			}
			time.Sleep(time.Millisecond)
		}
	}
	// Exactly one terminal state per task, and no quota left behind.
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, body := h.get("/metrics")
		tasks := body["tasks"].(map[string]any)
		if tasks["running"].(float64) == 0 && tasks["queued"].(float64) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("quota leaked: %v", tasks)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// readTerminal consumes events until a terminal one arrives.
func readTerminal(t *testing.T, rd *bufio.Reader, want string) map[string]any {
	t.Helper()
	for i := 0; i < 256; i++ {
		ev, err := readSSEEvent(rd)
		if err != nil {
			t.Fatalf("read event %d: %v", i, err)
		}
		typ, _ := ev["type"].(string)
		if typ != "completed" && typ != "failed" {
			continue
		}
		if typ != want {
			t.Fatalf("terminal event = %v, want %s", ev, want)
		}
		return ev
	}
	t.Fatal("no terminal event within 256 events")
	return nil
}

func requireErrorCode(t *testing.T, ev map[string]any, want string) {
	t.Helper()
	payload, _ := ev["payload"].(map[string]any)
	if payload["error_code"] != want {
		t.Fatalf("error_code = %v, want %s", payload["error_code"], want)
	}
}

// TestCancelRunningEmitsTerminalEvent covers a gap that used to force clients
// to poll /result: cancelling a RUNNING task closed the subscription without
// any terminal event.
func TestCancelRunningEmitsTerminalEvent(t *testing.T) {
	blocker := newBlockingAdapter()
	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 8, defs: []agent.Definition{
		{ID: "cancel-evt", Version: "v1", Protocol: "local", Enabled: true,
			MaxConcurrency: 2, TimeoutSeconds: 10, Adapter: blocker},
	}})
	id, _ := h.submit("cancel-evt", `{}`)
	resp, rd := h.openSSE(id)
	defer resp.Body.Close()

	select {
	case <-blocker.started:
	case <-time.After(time.Second):
		t.Fatal("task never started")
	}
	if _, body := h.post("/api/v1/tasks/"+id+"/cancel", nil); body["status"] != string(task.StatusRunning) {
		t.Fatalf("cancel response = %v", body)
	}

	ev := readTerminal(t, rd, "failed")
	requireErrorCode(t, ev, agent.ErrCodeTaskCancelled)

	if _, err := readSSEEvent(rd); err != io.EOF {
		t.Fatalf("stream should end after the terminal event, got %v", err)
	}
	h.waitStatus(id, task.StatusCancelled, 2*time.Second)
}

// TestTimeoutEmitsTerminalEvent: a deadline kill must announce agent_timeout.
func TestTimeoutEmitsTerminalEvent(t *testing.T) {
	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 8, defs: []agent.Definition{{
		ID: "slow-evt", Version: "v1", Protocol: "local", Enabled: true,
		MaxConcurrency: 2, TimeoutSeconds: 10,
		Adapter: adapters.NewLocal(adapters.LocalOptions{Duration: 5 * time.Second}),
	}}})
	id, _ := h.submitTimeout("slow-evt", 1)
	h.waitStatus(id, task.StatusTimeout, 3*time.Second)

	resp, rd := h.openSSE(id)
	defer resp.Body.Close()
	requireErrorCode(t, readTerminal(t, rd, "failed"), agent.ErrCodeTimeout)
}

// TestStartFailureEmitsTerminalEvent: a task that never reached the agent still
// ends its stream with a cause instead of closing silently.
func TestStartFailureEmitsTerminalEvent(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	h := newHarness(t, harnessOptions{globalWorkers: 2, globalQueue: 8, defs: []agent.Definition{
		remoteDef("dead-evt", "http://"+addr),
	}})
	id, _ := h.submit("dead-evt", `{}`)
	h.waitStatus(id, task.StatusFailed, 2*time.Second)

	resp, rd := h.openSSE(id)
	defer resp.Body.Close()
	requireErrorCode(t, readTerminal(t, rd, "failed"), agent.ErrCodeAgentUnavailable)
}
