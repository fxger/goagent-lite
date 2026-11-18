package task

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"goagent-lite/internal/agent"
	"goagent-lite/internal/apierr"
	"goagent-lite/internal/event"
	"goagent-lite/internal/metrics"
	"goagent-lite/internal/scheduler"
)

// Canceller is implemented by the executor. The manager never imports the
// executor package, keeping the dependency direction acyclic.
type Canceller interface {
	Cancel(taskID string) bool
	CancelAll()
}

// ManagerOptions configures the facade.
type ManagerOptions struct {
	QueueCapacity         int
	DefaultTimeoutSeconds int
	EventRetention        time.Duration
	Logger                *slog.Logger
}

// Manager is the task lifecycle facade: submission, lookup, cancellation,
// subscriptions, shutdown and housekeeping.
type Manager struct {
	store     Store
	registry  *agent.Registry
	router    *agent.Router
	sched     *scheduler.Scheduler
	events    *event.Manager
	metrics   *metrics.Metrics
	canceller Canceller
	log       *slog.Logger

	retention      time.Duration
	defaultTimeout int

	submitMu sync.Mutex
	closed   bool

	stopJanitor chan struct{}
	janitorWG   sync.WaitGroup
}

// NewManager wires the facade and registers every configured agent with the
// scheduler.
func NewManager(opts ManagerOptions, store Store, registry *agent.Registry,
	sched *scheduler.Scheduler, events *event.Manager, m *metrics.Metrics, canceller Canceller) (*Manager, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.EventRetention <= 0 {
		opts.EventRetention = 10 * time.Minute
	}
	if opts.DefaultTimeoutSeconds <= 0 {
		opts.DefaultTimeoutSeconds = 120
	}
	if opts.QueueCapacity <= 0 {
		opts.QueueCapacity = 1000
	}
	mgr := &Manager{
		store:          store,
		registry:       registry,
		router:         agent.NewRouter(registry),
		sched:          sched,
		events:         events,
		metrics:        m,
		canceller:      canceller,
		log:            opts.Logger,
		retention:      opts.EventRetention,
		defaultTimeout: opts.DefaultTimeoutSeconds,
		stopJanitor:    make(chan struct{}),
	}
	for _, reg := range registry.All() {
		if err := sched.RegisterAgent(scheduler.AgentConfig{
			ID:             reg.ID,
			Version:        reg.Version,
			MaxConcurrency: reg.MaxConcurrency,
			QueueCapacity:  reg.QueueCapacity,
		}); err != nil {
			return nil, fmt.Errorf("register agent %s: %w", reg.Key(), err)
		}
	}
	mgr.janitorWG.Add(1)
	go mgr.janitor()
	return mgr, nil
}

// Scheduler exposes the scheduler for the worker pool.
func (m *Manager) Scheduler() *scheduler.Scheduler { return m.sched }

// Events exposes the event manager for adapters/tests.
func (m *Manager) Events() *event.Manager { return m.events }

// Metrics exposes metrics.
func (m *Manager) Metrics() *metrics.Metrics { return m.metrics }

// QueueCapacity returns the global QUEUED capacity.
func (m *Manager) QueueCapacity() int { return m.sched.QueueCapacity() }

// SubmitRequest is the validated payload of POST /api/v1/tasks.
type SubmitRequest struct {
	AgentID        string
	AgentVersion   string
	Input          json.RawMessage
	TimeoutSeconds int
	Metadata       map[string]string
}

// Submit accepts a task, persists it as QUEUED and enqueues it.
func (m *Manager) Submit(req SubmitRequest) (*Task, error) {
	reg, err := m.router.Resolve(req.AgentID, req.AgentVersion)
	if err != nil {
		switch {
		case errors.Is(err, agent.ErrAgentNotFound):
			return nil, apierr.New(apierr.CodeAgentNotFound, err.Error())
		case errors.Is(err, agent.ErrAgentDisabled):
			return nil, apierr.New(apierr.CodeAgentDisabled, err.Error())
		default:
			return nil, apierr.New(apierr.CodeInternalError, err.Error())
		}
	}

	timeout := req.TimeoutSeconds
	if timeout <= 0 {
		timeout = reg.TimeoutSeconds
	}
	if timeout <= 0 {
		timeout = m.defaultTimeout
	}

	id, err := newTaskID()
	if err != nil {
		return nil, apierr.New(apierr.CodeInternalError, "generate task id: "+err.Error())
	}

	m.submitMu.Lock()
	defer m.submitMu.Unlock()
	if m.closed {
		return nil, apierr.New(apierr.CodeGatewayShuttingDown, "gateway is shutting down")
	}
	t := &Task{
		ID:             id,
		AgentID:        reg.ID,
		AgentVersion:   reg.Version,
		Status:         StatusQueued,
		Input:          append(json.RawMessage(nil), req.Input...),
		TimeoutSeconds: timeout,
		Metadata:       req.Metadata,
		CreatedAt:      time.Now().UTC(),
	}
	if err := m.store.Create(t); err != nil {
		return nil, apierr.New(apierr.CodeInternalError, "persist task: "+err.Error())
	}
	job := &scheduler.Job{ID: t.ID, AgentKey: reg.Key(), Value: t}
	if err := m.sched.Enqueue(job); err != nil {
		_ = m.store.Delete(t.ID)
		switch {
		case errors.Is(err, scheduler.ErrQueueFull):
			m.metrics.Rejected.Add(1)
			m.metrics.QueueFullReject.Add(1)
			return nil, apierr.New(apierr.CodeQueueFull, fmt.Sprintf("task queue is full (%d)", m.sched.QueueCapacity()))
		case errors.Is(err, scheduler.ErrAgentQueueFull):
			m.metrics.Rejected.Add(1)
			m.metrics.AgentQuotaReject.Add(1)
			return nil, apierr.New(apierr.CodeAgentQuotaExceeded, fmt.Sprintf("agent queue is full for %s", reg.Key()))
		case errors.Is(err, scheduler.ErrClosed):
			return nil, apierr.New(apierr.CodeGatewayShuttingDown, "gateway is shutting down")
		default:
			return nil, apierr.New(apierr.CodeInternalError, "enqueue task: "+err.Error())
		}
	}
	m.metrics.Submitted.Add(1)
	return t.Clone(), nil
}

// Get returns a task snapshot.
func (m *Manager) Get(id string) (*Task, error) {
	t, err := m.store.Get(id)
	if err != nil {
		return nil, apierr.New(apierr.CodeTaskNotFound, "task not found")
	}
	return t, nil
}

// Cancel requests cancellation. QUEUED tasks are marked CANCELLED (lazy
// removal); RUNNING tasks are cancelled through their context. The read-check-
// transition loop tolerates the QUEUED->RUNNING race: losing the race to
// execution means the cancellation is redirected to the running context.
func (m *Manager) Cancel(id string) (*Task, error) {
	for attempt := 0; attempt < 3; attempt++ {
		t, err := m.store.Get(id)
		if err != nil {
			return nil, apierr.New(apierr.CodeTaskNotFound, "task not found")
		}
		if t.Status.IsTerminal() {
			return nil, apierr.New(apierr.CodeTaskAlreadyFinished,
				fmt.Sprintf("task already finished with status %s", t.Status))
		}
		switch t.Status {
		case StatusQueued:
			err := m.store.Transition(id, StatusCancelled, func(x *Task) {
				x.ErrorCode = agent.ErrCodeTaskCancelled
				x.ErrorMessage = "task cancelled while queued"
			})
			if err != nil {
				if IsTransitionError(err) {
					continue // lost the race against the worker: re-read
				}
				return nil, apierr.From(err)
			}
			m.sched.MarkCancelled(id)
			// A QUEUED task never reaches the executor, so nobody else will
			// close its event stream; do it here so SSE subscribers end.
			m.events.Close(id)
		case StatusRunning:
			m.canceller.Cancel(id) // accepted; terminal state converges asynchronously
		}
		out, err := m.store.Get(id)
		if err != nil {
			return nil, apierr.From(err)
		}
		return out, nil
	}
	return nil, apierr.New(apierr.CodeTaskAlreadyFinished, "task finished while cancelling")
}

// Subscribe opens an SSE subscription: history first, then live events.
func (m *Manager) Subscribe(id string) ([]event.Event, <-chan event.Event, func(), error) {
	if _, err := m.store.Get(id); err != nil {
		return nil, nil, nil, apierr.New(apierr.CodeTaskNotFound, "task not found")
	}
	history, ch, cancel := m.events.Open(id)
	return history, ch, cancel, nil
}

// AgentStat is the queue state of one agent.
type AgentStat struct {
	Key            string `json:"key"`
	Queued         int    `json:"queued"`
	Running        int    `json:"running"`
	MaxConcurrency int    `json:"max_concurrency"`
	QueueCapacity  int    `json:"queue_capacity"`
}

// Stats aggregates counters and queue state for /metrics.
type Stats struct {
	Metrics       metrics.Snapshot
	QueueTotal    int
	QueueCapacity int
	Agents        []AgentStat
}

// Stats returns a point-in-time snapshot.
func (m *Manager) Stats() Stats {
	total, agents := m.sched.Snapshot()
	out := make([]AgentStat, 0, len(agents))
	for _, a := range agents {
		out = append(out, AgentStat{
			Key:            a.Key,
			Queued:         a.Queued,
			Running:        a.Running,
			MaxConcurrency: a.MaxConcurrency,
			QueueCapacity:  a.QueueCapacity,
		})
	}
	return Stats{
		Metrics:       m.metrics.Snapshot(),
		QueueTotal:    total,
		QueueCapacity: m.sched.QueueCapacity(),
		Agents:        out,
	}
}

// Accepting reports whether new submissions are accepted.
func (m *Manager) Accepting() bool {
	m.submitMu.Lock()
	defer m.submitMu.Unlock()
	return !m.closed
}

// BeginShutdown closes the submission gate immediately.
func (m *Manager) BeginShutdown() {
	m.submitMu.Lock()
	m.closed = true
	m.submitMu.Unlock()
}

// Shutdown cancels all QUEUED tasks, then waits for RUNNING tasks to finish
// until ctx expires; on expiry every remaining task context is cancelled.
func (m *Manager) Shutdown(ctx context.Context) {
	m.BeginShutdown()

	for _, t := range m.store.List() {
		if t.Status != StatusQueued {
			continue
		}
		if err := m.store.Transition(t.ID, StatusCancelled, func(x *Task) {
			x.ErrorCode = agent.ErrCodeGatewayShutdown
			x.ErrorMessage = "gateway shut down before the task started"
		}); err == nil {
			m.sched.MarkCancelled(t.ID)
			m.events.Close(t.ID)
		}
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if m.runningCount() == 0 {
			return
		}
		select {
		case <-ctx.Done():
			m.log.Warn("shutdown deadline reached, cancelling running tasks", "running", m.runningCount())
			m.canceller.CancelAll()
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) runningCount() int {
	n := 0
	for _, t := range m.store.List() {
		if t.Status == StatusRunning {
			n++
		}
	}
	return n
}

// WaitRunning blocks until no task is RUNNING.
func (m *Manager) WaitRunning(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if m.runningCount() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Close stops housekeeping. It does not stop the scheduler or workers.
func (m *Manager) Close() {
	select {
	case <-m.stopJanitor:
	default:
		close(m.stopJanitor)
	}
	m.janitorWG.Wait()
}

func (m *Manager) janitor() {
	defer m.janitorWG.Done()
	interval := 30 * time.Second
	if half := m.retention / 2; half > 0 && half < interval {
		interval = half
	}
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopJanitor:
			return
		case <-ticker.C:
			m.reap()
		}
	}
}

func (m *Manager) reap() {
	cutoff := time.Now().UTC().Add(-m.retention)
	for _, t := range m.store.List() {
		if !t.Status.IsTerminal() || t.FinishedAt == nil {
			continue
		}
		if t.FinishedAt.Before(cutoff) {
			m.events.Purge(t.ID)
			_ = m.store.Delete(t.ID)
		}
	}
}

func newTaskID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "task-" + hex.EncodeToString(b[:]), nil
}
