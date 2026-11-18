package executor

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"goagent-lite/internal/agent"
	"goagent-lite/internal/event"
	"goagent-lite/internal/metrics"
	"goagent-lite/internal/repository"
	"goagent-lite/internal/task"
)

type scriptedAdapter struct {
	events   []agent.AgentEvent
	startErr error
}

func (s scriptedAdapter) Start(ctx context.Context, req agent.AgentRequest) (<-chan agent.AgentEvent, error) {
	if s.startErr != nil {
		return nil, s.startErr
	}
	ch := make(chan agent.AgentEvent, len(s.events))
	for _, ev := range s.events {
		ch <- ev
	}
	close(ch)
	return ch, nil
}

type blockingAdapter struct {
	started chan struct{}
	once    sync.Once
}

func (b *blockingAdapter) Start(ctx context.Context, req agent.AgentRequest) (<-chan agent.AgentEvent, error) {
	ch := make(chan agent.AgentEvent, 1)
	ch <- agent.AgentEvent{Type: agent.EventStarted}
	b.once.Do(func() { close(b.started) })
	go func() {
		defer close(ch)
		<-ctx.Done()
	}()
	return ch, nil
}

type nilAdapter struct{}

func (nilAdapter) Start(ctx context.Context, req agent.AgentRequest) (<-chan agent.AgentEvent, error) {
	return nil, nil
}

type fixture struct {
	repo   *repository.Memory
	events *event.Manager
	met    *metrics.Metrics
	exec   *Executor
	reg    *agent.Registry
}

func newFixture(t *testing.T, defaultTimeout time.Duration, adapter agent.AgentAdapter) *fixture {
	t.Helper()
	reg, err := agent.NewRegistry([]agent.Definition{{
		ID: "a", Version: "v1", Protocol: "local", Enabled: true,
		MaxConcurrency: 4, Adapter: adapter,
	}})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{
		repo:   repository.NewMemory(),
		events: event.New(32),
		met:    metrics.New(),
		reg:    reg,
	}
	f.exec = New(f.repo, f.events, f.reg, f.met, Options{DefaultTimeout: defaultTimeout})
	return f
}

func (f *fixture) newTask(t *testing.T, timeoutSeconds int) *task.Task {
	t.Helper()
	tk := &task.Task{
		ID: "task-1", AgentID: "a", AgentVersion: "v1", Status: task.StatusQueued,
		Input: json.RawMessage(`{"in":true}`), TimeoutSeconds: timeoutSeconds,
		CreatedAt: time.Now().UTC(),
	}
	if err := f.repo.Create(tk); err != nil {
		t.Fatal(err)
	}
	return tk
}

func TestExecuteSuccess(t *testing.T) {
	f := newFixture(t, time.Second, scriptedAdapter{events: []agent.AgentEvent{
		{Type: agent.EventStarted, Payload: json.RawMessage(`{}`)},
		{Type: agent.EventToken, Payload: json.RawMessage(`{"text":"hi"}`)},
		{Type: agent.EventCompleted, Payload: json.RawMessage(`{"answer":42}`)},
	}})
	tk := f.newTask(t, 0)
	f.exec.Execute(context.Background(), tk)

	got, _ := f.repo.Get("task-1")
	if got.Status != task.StatusSucceeded {
		t.Fatalf("status = %s", got.Status)
	}
	if string(got.Result) != `{"answer":42}` {
		t.Fatalf("result = %s", got.Result)
	}
	if got.StartedAt == nil || got.FinishedAt == nil {
		t.Fatal("timestamps missing")
	}
	if f.met.Succeeded.Load() != 1 || f.met.Failed.Load() != 0 {
		t.Fatalf("metrics = %+v", f.met.Snapshot())
	}
	hist, ch, cancel := f.events.Open("task-1")
	defer cancel()
	if len(hist) != 3 {
		t.Fatalf("history = %d", len(hist))
	}
	if _, open := <-ch; open {
		t.Fatal("event stream must be closed after execution")
	}
}

func TestExecuteCompletedWithoutPayload(t *testing.T) {
	f := newFixture(t, time.Second, scriptedAdapter{events: []agent.AgentEvent{{Type: agent.EventCompleted}}})
	f.exec.Execute(context.Background(), f.newTask(t, 0))
	got, _ := f.repo.Get("task-1")
	if got.Status != task.StatusSucceeded || string(got.Result) != `{}` {
		t.Fatalf("got %+v", got)
	}
}

func TestExecuteAgentFailure(t *testing.T) {
	f := newFixture(t, time.Second, scriptedAdapter{events: []agent.AgentEvent{
		{Type: agent.EventStarted},
		{Type: agent.EventFailed, Payload: json.RawMessage(`{"code":"boom","message":"bad"}`)},
	}})
	f.exec.Execute(context.Background(), f.newTask(t, 0))
	got, _ := f.repo.Get("task-1")
	if got.Status != task.StatusFailed || got.ErrorCode != "boom" || got.ErrorMessage != "bad" {
		t.Fatalf("got %+v", got)
	}
	if f.met.Failed.Load() != 1 {
		t.Fatalf("failed = %d", f.met.Failed.Load())
	}
}

func TestExecuteStreamInterrupted(t *testing.T) {
	f := newFixture(t, time.Second, scriptedAdapter{events: []agent.AgentEvent{{Type: agent.EventStarted}}})
	f.exec.Execute(context.Background(), f.newTask(t, 0))
	got, _ := f.repo.Get("task-1")
	if got.Status != task.StatusFailed || got.ErrorCode != agent.ErrCodeStreamInterrupted {
		t.Fatalf("got %+v", got)
	}
	if f.met.StreamInterrupted.Load() != 1 {
		t.Fatal("stream interruption not counted")
	}
}

func TestExecuteStartError(t *testing.T) {
	f := newFixture(t, time.Second, scriptedAdapter{startErr: agent.NewError(agent.ErrCodeAgentError, "nope")})
	f.exec.Execute(context.Background(), f.newTask(t, 0))
	got, _ := f.repo.Get("task-1")
	if got.Status != task.StatusFailed || got.ErrorCode != agent.ErrCodeAgentError {
		t.Fatalf("got %+v", got)
	}
	if f.met.StartFailures.Load() != 1 {
		t.Fatal("start failure not counted")
	}
	_, ch, cancel := f.events.Open("task-1")
	defer cancel()
	if _, open := <-ch; open {
		t.Fatal("stream must be closed after a start failure")
	}
}

func TestExecuteNilChannel(t *testing.T) {
	f := newFixture(t, time.Second, nilAdapter{})
	f.exec.Execute(context.Background(), f.newTask(t, 0))
	got, _ := f.repo.Get("task-1")
	if got.Status != task.StatusFailed || got.ErrorCode != agent.ErrCodeProtocolViolation {
		t.Fatalf("got %+v", got)
	}
}

func TestExecuteTimeout(t *testing.T) {
	f := newFixture(t, 100*time.Millisecond, &blockingAdapter{started: make(chan struct{})})
	start := time.Now()
	f.exec.Execute(context.Background(), f.newTask(t, 0))
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("timeout took %v", elapsed)
	}
	got, _ := f.repo.Get("task-1")
	if got.Status != task.StatusTimeout || got.ErrorCode != agent.ErrCodeTimeout {
		t.Fatalf("got %+v", got)
	}
	if f.met.TimedOut.Load() != 1 {
		t.Fatal("timeout not counted")
	}
}

func TestExecuteClientCancel(t *testing.T) {
	adapter := &blockingAdapter{started: make(chan struct{})}
	f := newFixture(t, time.Second, adapter)
	tk := f.newTask(t, 0)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.exec.Execute(context.Background(), tk)
	}()
	<-adapter.started
	if !f.exec.Cancel(tk.ID) {
		t.Fatal("cancel should be accepted")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("execution did not stop after cancellation")
	}
	got, _ := f.repo.Get(tk.ID)
	if got.Status != task.StatusCancelled || got.ErrorCode != agent.ErrCodeTaskCancelled {
		t.Fatalf("got %+v", got)
	}
	if f.met.Cancelled.Load() != 1 {
		t.Fatal("cancel not counted")
	}
}

func TestExecuteShutdownCancel(t *testing.T) {
	adapter := &blockingAdapter{started: make(chan struct{})}
	f := newFixture(t, time.Second, adapter)
	tk := f.newTask(t, 0)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.exec.Execute(context.Background(), tk)
	}()
	<-adapter.started
	f.exec.CancelAll()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("execution did not stop after CancelAll")
	}
	got, _ := f.repo.Get(tk.ID)
	if got.Status != task.StatusCancelled || got.ErrorCode != agent.ErrCodeGatewayShutdown {
		t.Fatalf("got %+v", got)
	}
}

func TestCancelBeforeRegister(t *testing.T) {
	adapter := &blockingAdapter{started: make(chan struct{})}
	f := newFixture(t, time.Second, adapter)
	tk := f.newTask(t, 0)
	// Cancel before execution begins: the request is remembered.
	f.exec.Cancel(tk.ID)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.exec.Execute(context.Background(), tk)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pending cancellation was not applied")
	}
	got, _ := f.repo.Get(tk.ID)
	if got.Status != task.StatusCancelled || got.ErrorCode != agent.ErrCodeTaskCancelled {
		t.Fatalf("got %+v", got)
	}
}

func TestExecuteMissingAgent(t *testing.T) {
	f := newFixture(t, time.Second, scriptedAdapter{})
	tk := f.newTask(t, 0)
	tk.AgentVersion = "v9"
	// Recreate under a different id so the store holds the bad version.
	tk.ID = "task-missing"
	if err := f.repo.Create(tk); err != nil {
		t.Fatal(err)
	}
	f.exec.Execute(context.Background(), tk)
	got, _ := f.repo.Get(tk.ID)
	if got.Status != task.StatusFailed {
		t.Fatalf("got %+v", got)
	}
}

func TestFinishOnlyOnce(t *testing.T) {
	f := newFixture(t, time.Second, scriptedAdapter{events: []agent.AgentEvent{{Type: agent.EventCompleted, Payload: json.RawMessage(`{}`)}}})
	f.exec.Execute(context.Background(), f.newTask(t, 0))
	_ = f.repo.Transition("task-1", task.StatusRunning, nil)
	_ = f.exec.finish("task-1", task.StatusFailed, "x", "y", nil)
	if f.met.Failed.Load() != 0 {
		t.Fatal("second terminal transition must not count")
	}
}
