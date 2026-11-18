package task

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"goagent-lite/internal/agent"
	"goagent-lite/internal/apierr"
	"goagent-lite/internal/event"
	"goagent-lite/internal/metrics"
	"goagent-lite/internal/scheduler"
)

type testStore struct {
	mu sync.Mutex
	m  map[string]*Task
}

func newMemoryStore() *testStore { return &testStore{m: make(map[string]*Task)} }

func (s *testStore) Create(t *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[t.ID]; ok {
		return errors.New("exists")
	}
	s.m[t.ID] = t.Clone()
	return nil
}

func (s *testStore) Get(id string) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.m[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return t.Clone(), nil
}

func (s *testStore) Transition(id string, to Status, apply func(*Task)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.m[id]
	if !ok {
		return ErrTaskNotFound
	}
	if !CanTransition(t.Status, to) {
		return &TransitionError{From: t.Status, To: to}
	}
	if apply != nil {
		apply(t)
	}
	t.Status = to
	if to.IsTerminal() {
		now := time.Now().UTC()
		t.FinishedAt = &now
	}
	return nil
}

func (s *testStore) List() []*Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Task, 0, len(s.m))
	for _, t := range s.m {
		out = append(out, t.Clone())
	}
	return out
}

func (s *testStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, id)
	return nil
}

type stubAdapter struct{}

func (stubAdapter) Start(ctx context.Context, req agent.AgentRequest) (<-chan agent.AgentEvent, error) {
	ch := make(chan agent.AgentEvent)
	close(ch)
	return ch, nil
}

type fakeCanceller struct {
	mu      sync.Mutex
	tasks   []string
	allCall int
}

func (f *fakeCanceller) Cancel(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks = append(f.tasks, id)
	return true
}

func (f *fakeCanceller) CancelAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allCall++
}

func newTestManager(t *testing.T, schedCap, agentQueue, agentMax int) (*Manager, *scheduler.Scheduler, *metrics.Metrics, *event.Manager, *fakeCanceller) {
	t.Helper()
	reg, err := agent.NewRegistry([]agent.Definition{
		{ID: "a", Version: "v1", Protocol: "local", Enabled: true, MaxConcurrency: agentMax, QueueCapacity: agentQueue, TimeoutSeconds: 30, Adapter: stubAdapter{}},
		{ID: "off", Version: "v1", Protocol: "local", Enabled: false, MaxConcurrency: 1, TimeoutSeconds: 30, Adapter: stubAdapter{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryStore()
	events := event.New(8)
	met := metrics.New()
	sched := scheduler.New(schedCap)
	canceller := &fakeCanceller{}
	mgr, err := NewManager(ManagerOptions{
		QueueCapacity:         schedCap,
		DefaultTimeoutSeconds: 120,
		EventRetention:        time.Minute,
	}, store, reg, sched, events, met, canceller)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mgr.Close)
	return mgr, sched, met, events, canceller
}

func TestCanTransition(t *testing.T) {
	cases := []struct {
		from, to Status
		ok       bool
	}{
		{StatusQueued, StatusRunning, true},
		{StatusQueued, StatusCancelled, true},
		{StatusQueued, StatusSucceeded, false},
		{StatusRunning, StatusSucceeded, true},
		{StatusRunning, StatusFailed, true},
		{StatusRunning, StatusTimeout, true},
		{StatusRunning, StatusCancelled, true},
		{StatusRunning, StatusRunning, false},
		{StatusSucceeded, StatusFailed, false},
		{StatusFailed, StatusCancelled, false},
		{StatusCancelled, StatusRunning, false},
		{StatusTimeout, StatusSucceeded, false},
	}
	for _, tc := range cases {
		if got := CanTransition(tc.from, tc.to); got != tc.ok {
			t.Errorf("CanTransition(%s,%s) = %v, want %v", tc.from, tc.to, got, tc.ok)
		}
	}
	for _, s := range []Status{StatusSucceeded, StatusFailed, StatusCancelled, StatusTimeout} {
		if !s.IsTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	for _, s := range []Status{StatusQueued, StatusRunning} {
		if s.IsTerminal() {
			t.Errorf("%s should not be terminal", s)
		}
	}
}

func TestCloneIsDeep(t *testing.T) {
	now := time.Now().UTC()
	in := &Task{ID: "t", Input: json.RawMessage(`{"a":1}`), Metadata: map[string]string{"k": "v"}, StartedAt: &now}
	c := in.Clone()
	c.Input[0] = '['
	c.Metadata["k"] = "x"
	*c.StartedAt = now.Add(time.Hour)
	if string(in.Input) != `{"a":1}` || in.Metadata["k"] != "v" || !in.StartedAt.Equal(now) {
		t.Fatal("Clone is not deep")
	}
}

func TestSubmitAndGet(t *testing.T) {
	mgr, _, met, _, _ := newTestManager(t, 10, 0, 5)
	task, err := mgr.Submit(SubmitRequest{
		AgentID: "a", AgentVersion: "v1", Input: json.RawMessage(`{"x":1}`),
		Metadata: map[string]string{"trace": "abc"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != StatusQueued || task.TimeoutSeconds != 30 || task.ID == "" {
		t.Fatalf("unexpected task: %+v", task)
	}
	got, err := mgr.Get(task.ID)
	if err != nil || got.ID != task.ID {
		t.Fatalf("get: %v %+v", err, got)
	}
	if met.Submitted.Load() != 1 {
		t.Fatalf("submitted = %d", met.Submitted.Load())
	}
	stats := mgr.Stats()
	if stats.QueueTotal != 1 || stats.QueueCapacity != 10 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestSubmitUnknownAndDisabled(t *testing.T) {
	mgr, _, _, _, _ := newTestManager(t, 10, 0, 5)
	_, err := mgr.Submit(SubmitRequest{AgentID: "nope", AgentVersion: "v1", Input: json.RawMessage(`{}`)})
	if e := apierr.From(err); e.Code != apierr.CodeAgentNotFound {
		t.Fatalf("code = %s", e.Code)
	}
	_, err = mgr.Submit(SubmitRequest{AgentID: "off", AgentVersion: "v1", Input: json.RawMessage(`{}`)})
	if e := apierr.From(err); e.Code != apierr.CodeAgentDisabled {
		t.Fatalf("code = %s", e.Code)
	}
}

func TestCancelQueuedClosesEventStream(t *testing.T) {
	mgr, _, met, _, _ := newTestManager(t, 10, 0, 5)
	task, _ := mgr.Submit(SubmitRequest{AgentID: "a", AgentVersion: "v1", Input: json.RawMessage(`{}`)})
	_, ch, cancel, err := mgr.Subscribe(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	got, err := mgr.Cancel(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusCancelled || got.ErrorCode != agent.ErrCodeTaskCancelled {
		t.Fatalf("cancelled task = %+v", got)
	}
	select {
	case _, open := <-ch:
		if open {
			t.Fatal("event stream should be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("event stream was not closed")
	}
	// The cancelled job is still sitting in the queue (lazy deletion).
	if total := mgr.sched.Queued(); total != 1 {
		t.Fatalf("scheduler queued = %d", total)
	}
	if met.Cancelled.Load() != 0 {
		t.Fatal("executor counter must not move for a QUEUED cancellation")
	}

	// Cancelling again reports the terminal state.
	_, err = mgr.Cancel(task.ID)
	if e := apierr.From(err); e.Code != apierr.CodeTaskAlreadyFinished {
		t.Fatalf("code = %s", e.Code)
	}
}

func TestCancelRunningIsAccepted(t *testing.T) {
	mgr, _, _, _, canceller := newTestManager(t, 10, 0, 5)
	task, _ := mgr.Submit(SubmitRequest{AgentID: "a", AgentVersion: "v1", Input: json.RawMessage(`{}`)})
	if err := mgr.store.Transition(task.ID, StatusRunning, nil); err != nil {
		t.Fatal(err)
	}
	got, err := mgr.Cancel(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusRunning {
		t.Fatalf("status = %s, want RUNNING (accepted)", got.Status)
	}
	if len(canceller.tasks) != 1 || canceller.tasks[0] != task.ID {
		t.Fatalf("canceller calls = %v", canceller.tasks)
	}
}

func TestCancelTerminalRejected(t *testing.T) {
	mgr, _, _, _, _ := newTestManager(t, 10, 0, 5)
	task, _ := mgr.Submit(SubmitRequest{AgentID: "a", AgentVersion: "v1", Input: json.RawMessage(`{}`)})
	if err := mgr.store.Transition(task.ID, StatusRunning, nil); err != nil {
		t.Fatal(err)
	}
	if err := mgr.store.Transition(task.ID, StatusSucceeded, nil); err != nil {
		t.Fatal(err)
	}
	_, err := mgr.Cancel(task.ID)
	if e := apierr.From(err); e.Code != apierr.CodeTaskAlreadyFinished {
		t.Fatalf("code = %s", e.Code)
	}
}

func TestGlobalQueueFullMapping(t *testing.T) {
	mgr, _, met, _, _ := newTestManager(t, 1, 0, 5)
	if _, err := mgr.Submit(SubmitRequest{AgentID: "a", AgentVersion: "v1", Input: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	_, err := mgr.Submit(SubmitRequest{AgentID: "a", AgentVersion: "v1", Input: json.RawMessage(`{}`)})
	if e := apierr.From(err); e.Code != apierr.CodeQueueFull || e.Status != 429 {
		t.Fatalf("code = %s status = %d", e.Code, e.Status)
	}
	if met.QueueFullReject.Load() != 1 || met.Rejected.Load() != 1 {
		t.Fatalf("rejection counters = %d/%d", met.QueueFullReject.Load(), met.Rejected.Load())
	}
}

func TestAgentQueueFullMapping(t *testing.T) {
	mgr, _, met, _, _ := newTestManager(t, 10, 1, 5)
	if _, err := mgr.Submit(SubmitRequest{AgentID: "a", AgentVersion: "v1", Input: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	_, err := mgr.Submit(SubmitRequest{AgentID: "a", AgentVersion: "v1", Input: json.RawMessage(`{}`)})
	if e := apierr.From(err); e.Code != apierr.CodeAgentQuotaExceeded || e.Status != 429 {
		t.Fatalf("code = %s status = %d", e.Code, e.Status)
	}
	if met.AgentQuotaReject.Load() != 1 {
		t.Fatal("agent quota rejection not counted")
	}
}

func TestShutdownCancelsQueuedAndClosesGate(t *testing.T) {
	mgr, _, _, _, _ := newTestManager(t, 10, 0, 5)
	t1, _ := mgr.Submit(SubmitRequest{AgentID: "a", AgentVersion: "v1", Input: json.RawMessage(`{}`)})
	t2, _ := mgr.Submit(SubmitRequest{AgentID: "a", AgentVersion: "v1", Input: json.RawMessage(`{}`)})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	mgr.Shutdown(ctx)

	for _, id := range []string{t1.ID, t2.ID} {
		got, _ := mgr.Get(id)
		if got.Status != StatusCancelled || got.ErrorCode != agent.ErrCodeGatewayShutdown {
			t.Fatalf("task %s = %+v", id, got)
		}
	}
	_, err := mgr.Submit(SubmitRequest{AgentID: "a", AgentVersion: "v1", Input: json.RawMessage(`{}`)})
	if err == nil || apierr.From(err).Code != apierr.CodeGatewayShuttingDown {
		t.Fatalf("submit after shutdown: %v", err)
	}
	if mgr.Accepting() {
		t.Fatal("manager must stop accepting submissions")
	}
}

func TestWaitRunning(t *testing.T) {
	mgr, _, _, _, _ := newTestManager(t, 10, 0, 5)
	task, _ := mgr.Submit(SubmitRequest{AgentID: "a", AgentVersion: "v1", Input: json.RawMessage(`{}`)})
	_ = mgr.store.Transition(task.ID, StatusRunning, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := mgr.WaitRunning(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
	_ = mgr.store.Transition(task.ID, StatusSucceeded, nil)
	if err := mgr.WaitRunning(context.Background()); err != nil {
		t.Fatal(err)
	}
}
