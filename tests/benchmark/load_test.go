// Package benchmark contains an optional end-to-end load test. It is skipped
// unless GOAGENT_BENCH=1 so that `go test ./...` stays fast.
//
//	GOAGENT_BENCH=1 BENCH_TASKS=500 go test ./tests/benchmark -run TestLoad -v
//
// The test submits tasks over real HTTP to an in-process gateway (500 workers by
// default), waits for every task to reach a terminal state and prints a JSON
// report with throughput, latency percentiles, queue/execution durations,
// goroutine peaks, memory and CPU time.
package benchmark_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"strconv"
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

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

type taskView struct {
	TaskID              string     `json:"task_id"`
	Status              string     `json:"status"`
	CreatedAt           time.Time  `json:"created_at"`
	StartedAt           *time.Time `json:"started_at"`
	FinishedAt          *time.Time `json:"finished_at"`
	QueueDurationMS     *float64   `json:"queue_duration_ms"`
	ExecutionDurationMS *float64   `json:"execution_duration_ms"`
}

type report struct {
	Config       map[string]any `json:"config"`
	Runtime      map[string]any `json:"runtime"`
	Results      map[string]any `json:"results"`
	LatencyMS    map[string]any `json:"latency_ms"`
	QueueMS      map[string]any `json:"queue_ms"`
	ExecutionMS  map[string]any `json:"execution_ms"`
	Goroutines   map[string]any `json:"goroutines"`
	MemoryBytes  map[string]any `json:"memory_bytes"`
	CPU          map[string]any `json:"cpu"`
	StatusCounts map[string]int `json:"status_counts"`
}

func TestLoad(t *testing.T) {
	if os.Getenv("GOAGENT_BENCH") != "1" {
		t.Skip("load test disabled; set GOAGENT_BENCH=1 to run")
	}
	tasks := envInt("BENCH_TASKS", 500)
	workers := envInt("BENCH_WORKERS", 500)
	agentConcurrency := envInt("BENCH_AGENT_CONCURRENCY", workers)
	minDuration := 100 * time.Millisecond
	maxJitter := 1900 * time.Millisecond // uniform 100ms..2s, as required by 10.1

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	def := agent.Definition{
		ID: "bench-local", Version: "v1", Protocol: "local", Enabled: true,
		MaxConcurrency: agentConcurrency, TimeoutSeconds: 60,
		Adapter: adapters.NewLocal(adapters.LocalOptions{
			Duration: minDuration, Jitter: maxJitter, Tokens: 3, TokenInterval: 20 * time.Millisecond,
		}),
	}
	reg, err := agent.NewRegistry([]agent.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewMemory()
	events := event.New(64)
	met := metrics.New()
	sched := scheduler.New(tasks + workers)
	exec := executor.New(repo, events, reg, met, executor.Options{DefaultTimeout: 60 * time.Second, Logger: logger})
	mgr, err := task.NewManager(task.ManagerOptions{
		QueueCapacity:         tasks + workers,
		DefaultTimeoutSeconds: 60,
		EventRetention:        5 * time.Minute,
		Logger:                logger,
	}, repo, reg, sched, events, met, exec)
	if err != nil {
		t.Fatal(err)
	}
	pool := worker.New(workers, sched, exec, met, logger)
	pool.Start()
	apiSrv := api.New(mgr, logger, api.Options{MaxRequestBodyBytes: 1 << 20, SSEHeartbeat: 15 * time.Second, WorkersConfigured: workers})
	ts := httptest.NewServer(apiSrv.Handler())
	defer func() {
		apiSrv.BeginShutdown()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		mgr.Shutdown(ctx)
		pool.Stop()
		pool.Wait()
		mgr.Close()
		ts.Close()
	}()

	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        workers + 100,
		MaxIdleConnsPerHost: workers + 100,
		MaxConnsPerHost:     workers + 100,
	}}

	// Sample goroutines, heap and RUNNING gauge while the load runs.
	stopSampling := make(chan struct{})
	var wg sync.WaitGroup
	var peakGoroutines, peakHeap, peakRunning atomic.Int64
	sample := func() {
		g := int64(runtime.NumGoroutine())
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		h := int64(ms.HeapAlloc)
		r := int64(met.Snapshot().Running)
		for {
			old := peakGoroutines.Load()
			if g <= old || peakGoroutines.CompareAndSwap(old, g) {
				break
			}
		}
		for {
			old := peakHeap.Load()
			if h <= old || peakHeap.CompareAndSwap(old, h) {
				break
			}
		}
		for {
			old := peakRunning.Load()
			if r <= old || peakRunning.CompareAndSwap(old, r) {
				break
			}
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				sample()
			}
		}
	}()

	cpuStart := processCPUSeconds()
	wallStart := time.Now()
	ids := make([]string, 0, tasks)
	for i := 0; i < tasks; i++ {
		body, _ := json.Marshal(map[string]any{
			"agent_id": "bench-local", "agent_version": "v1",
			"input": map[string]any{"i": i},
		})
		resp, err := client.Post(ts.URL+"/api/v1/tasks", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("submit %d: HTTP %d: %s", i, resp.StatusCode, raw)
		}
		var out struct {
			TaskID string `json:"task_id"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, out.TaskID)
	}
	submitElapsed := time.Since(wallStart)
	sample()

	// Wait for completion using the gateway's own metrics.
	deadline := time.Now().Add(5 * time.Minute)
	for {
		resp, err := client.Get(ts.URL + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		var m struct {
			Tasks struct {
				Submitted int64 `json:"submitted"`
				Succeeded int64 `json:"succeeded"`
				Failed    int64 `json:"failed"`
				TimedOut  int64 `json:"timed_out"`
				Cancelled int64 `json:"cancelled"`
				Running   int64 `json:"running"`
				Queued    int64 `json:"queued"`
			} `json:"tasks"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&m)
		resp.Body.Close()
		done := m.Tasks.Succeeded + m.Tasks.Failed + m.Tasks.TimedOut + m.Tasks.Cancelled
		if done >= int64(tasks) && m.Tasks.Running == 0 && m.Tasks.Queued == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for completion: %+v", m.Tasks)
		}
		sample()
		time.Sleep(20 * time.Millisecond)
	}
	wall := time.Since(wallStart)
	cpuUsed := processCPUSeconds() - cpuStart
	close(stopSampling)
	wg.Wait()

	if got := peakRunning.Load(); got > int64(workers) {
		t.Fatalf("RUNNING peak %d exceeds worker count %d", got, workers)
	}
	// Workers + event pumps + adapter goroutines + fixed HTTP overhead.
	if maxG := int64(workers*2 + agentConcurrency + 100); peakGoroutines.Load() > maxG {
		t.Errorf("goroutine peak %d exceeds the expected bound %d", peakGoroutines.Load(), maxG)
	}

	// Collect every task's timings.
	views := make([]taskView, tasks)
	var fetchWG sync.WaitGroup
	errCh := make(chan error, tasks)
	sem := make(chan struct{}, 64)
	for i, id := range ids {
		i, id := i, id
		fetchWG.Add(1)
		sem <- struct{}{}
		go func() {
			defer fetchWG.Done()
			defer func() { <-sem }()
			resp, err := client.Get(ts.URL + "/api/v1/tasks/" + id)
			if err != nil {
				errCh <- err
				return
			}
			defer resp.Body.Close()
			if err := json.NewDecoder(resp.Body).Decode(&views[i]); err != nil {
				errCh <- err
			}
		}()
	}
	fetchWG.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	var latencies, queues, executions []float64
	statuses := map[string]int{}
	for _, v := range views {
		statuses[v.Status]++
		if v.FinishedAt != nil {
			latencies = append(latencies, float64(v.FinishedAt.Sub(v.CreatedAt).Microseconds())/1000.0)
		}
		if v.QueueDurationMS != nil {
			queues = append(queues, *v.QueueDurationMS)
		}
		if v.ExecutionDurationMS != nil {
			executions = append(executions, *v.ExecutionDurationMS)
		}
	}
	succeeded := statuses[string(task.StatusSucceeded)]
	rep := report{
		Config: map[string]any{
			"tasks":                      tasks,
			"workers":                    workers,
			"agent_max_concurrency":      agentConcurrency,
			"mock_duration_distribution": "uniform",
			"mock_min_duration_ms":       minDuration.Milliseconds(),
			"mock_max_duration_ms":       (minDuration + maxJitter).Milliseconds(),
			"mock_failure_rate":          0,
			"mock_tokens":                3,
			"task_timeout_seconds":       60,
		},
		Runtime: map[string]any{
			"go_version": runtime.Version(),
			"goos":       runtime.GOOS,
			"goarch":     runtime.GOARCH,
			"num_cpu":    runtime.NumCPU(),
		},
		Results: map[string]any{
			"submitted":         tasks,
			"succeeded":         succeeded,
			"success_rate":      float64(succeeded) / float64(tasks),
			"wall_seconds":      wall.Seconds(),
			"submit_seconds":    submitElapsed.Seconds(),
			"submit_rate_per_s": float64(tasks) / submitElapsed.Seconds(),
			"completion_rate":   float64(succeeded) / wall.Seconds(),
			"peak_running":      peakRunning.Load(),
			"workers_alive":     met.Snapshot().Workers,
			"peak_goroutines":   peakGoroutines.Load(),
			"final_goroutines":  runtime.NumGoroutine(),
		},
		LatencyMS:   percentileMap(latencies),
		QueueMS:     percentileMap(queues),
		ExecutionMS: percentileMap(executions),
		Goroutines: map[string]any{
			"peak":  peakGoroutines.Load(),
			"final": runtime.NumGoroutine(),
			// One worker goroutine per task, one executor event pump and one
			// adapter goroutine per running task.
			"formula_expected_at_peak": workers + workers + agentConcurrency + 50,
		},
		MemoryBytes: map[string]any{"peak_heap_alloc": peakHeap.Load()},
		CPU: map[string]any{
			"cpu_seconds":          cpuUsed,
			"cpu_percent_one_core": cpuUsed / wall.Seconds() * 100,
			"cpu_percent_machine":  cpuUsed / wall.Seconds() / float64(runtime.NumCPU()) * 100,
		},
		StatusCounts: statuses,
	}
	out, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Printf("\nBENCHMARK_REPORT_BEGIN\n%s\nBENCHMARK_REPORT_END\n", out)
}

func percentileMap(v []float64) map[string]any {
	sort.Float64s(v)
	return map[string]any{
		"count": len(v),
		"p50":   percentile(v, 0.50),
		"p95":   percentile(v, 0.95),
		"p99":   percentile(v, 0.99),
		"max":   percentile(v, 1.0),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}
