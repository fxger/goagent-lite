// Package executor runs one task through an adapter: it owns the running
// context, timeout, cancellation, event consumption and terminal-state CAS.
package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"goagent-lite/internal/agent"
	"goagent-lite/internal/event"
	"goagent-lite/internal/metrics"
	"goagent-lite/internal/task"
)

// Registry resolves a task's agent definition.
type Registry interface {
	Lookup(id, version string) (*agent.Registered, bool)
}

// Options configures the executor.
type Options struct {
	DefaultTimeout time.Duration
	Logger         *slog.Logger
}

// Executor is stateless apart from the cancellation registry.
type Executor struct {
	repo           task.Store
	events         *event.Manager
	registry       Registry
	metrics        *metrics.Metrics
	log            *slog.Logger
	defaultTimeout time.Duration

	mu              sync.Mutex
	cancels         map[string]context.CancelFunc
	cancelRequested map[string]struct{}
	userCancelled   map[string]struct{}
}

// New creates an executor.
func New(repo task.Store, events *event.Manager, registry Registry, m *metrics.Metrics, opts Options) *Executor {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.DefaultTimeout <= 0 {
		opts.DefaultTimeout = 120 * time.Second
	}
	return &Executor{
		repo:            repo,
		events:          events,
		registry:        registry,
		metrics:         m,
		log:             opts.Logger,
		defaultTimeout:  opts.DefaultTimeout,
		cancels:         make(map[string]context.CancelFunc),
		cancelRequested: make(map[string]struct{}),
		userCancelled:   make(map[string]struct{}),
	}
}

// Execute drives a task from QUEUED to a terminal state. It never returns an
// error: every failure is recorded in the repository and in metrics.
func (e *Executor) Execute(parent context.Context, t *task.Task) {
	started := time.Now().UTC()
	if err := e.repo.Transition(t.ID, task.StatusRunning, func(x *task.Task) {
		x.StartedAt = &started
	}); err != nil {
		// Most likely cancelled while queued (lazy removal) or already terminal.
		e.log.Debug("task not started", "task_id", t.ID, "err", err)
		return
	}
	e.metrics.Running.Add(1)
	agentKey := agent.Key(t.AgentID, t.AgentVersion)
	e.metrics.IncAgentRunning(agentKey)
	defer func() {
		e.metrics.Running.Add(-1)
		e.metrics.DecAgentRunning(agentKey)
	}()

	timeout := e.defaultTimeout
	reg, ok := e.registry.Lookup(t.AgentID, t.AgentVersion)
	if !ok {
		e.settle(t.ID, task.StatusFailed, "agent_not_found", "agent disappeared from the registry")
		e.events.Close(t.ID)
		return
	}
	if t.TimeoutSeconds > 0 {
		timeout = time.Duration(t.TimeoutSeconds) * time.Second
	} else if reg.TimeoutSeconds > 0 {
		timeout = time.Duration(reg.TimeoutSeconds) * time.Second
	}

	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	e.register(t.ID, cancel)
	defer e.unregister(t.ID)

	var ch <-chan agent.AgentEvent
	defer func() {
		if r := recover(); r != nil {
			e.log.Error("executor panic", "task_id", t.ID, "panic", r, "stack", string(debug.Stack()))
			e.metrics.Panics.Add(1)
			e.settle(t.ID, task.StatusFailed, agent.ErrCodeInternalPanic, fmt.Sprintf("executor panic: %v", r))
			cancel()
			if ch != nil {
				drain(ch, 5*time.Second)
			}
		}
		e.events.Close(t.ID)
	}()

	var dispatchConfig json.RawMessage
	if t.TimeoutSeconds > 0 {
		dispatchConfig, _ = json.Marshal(map[string]any{"timeout_seconds": t.TimeoutSeconds})
	}
	req := agent.AgentRequest{
		TaskID:   t.ID,
		Input:    t.Input,
		Config:   dispatchConfig,
		Metadata: t.Metadata,
	}
	ch, err := reg.Adapter.Start(ctx, req)
	if err != nil {
		e.handleStartError(ctx, t, err)
		return
	}
	if ch == nil {
		e.settle(t.ID, task.StatusFailed, agent.ErrCodeProtocolViolation, "adapter returned a nil event channel")
		return
	}
	e.runEventPump(ctx, t, ch, cancel)
}

// runEventPump consumes the adapter stream on its own goroutine. Keeping the
// pump separate from the worker is what §9.2 requires: the pump drains until
// the adapter closes the channel, and it recovers its own panics so neither a
// broken event handler nor a misbehaving adapter can take the worker down.
func (e *Executor) runEventPump(ctx context.Context, t *task.Task, ch <-chan agent.AgentEvent, cancel context.CancelFunc) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				e.log.Error("executor event pump panic", "task_id", t.ID, "panic", r, "stack", string(debug.Stack()))
				e.metrics.Panics.Add(1)
				e.finish(t.ID, task.StatusFailed, agent.ErrCodeInternalPanic, fmt.Sprintf("event pump panic: %v", r), nil)
				cancel()
				drain(ch, 5*time.Second)
			}
		}()
		e.consume(ctx, t, ch)
	}()
	<-done
}

func (e *Executor) handleStartError(ctx context.Context, t *task.Task, err error) {
	switch {
	case e.wasUserCancelled(t.ID):
		e.settle(t.ID, task.StatusCancelled, agent.ErrCodeTaskCancelled, "task cancelled by client")
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		e.settle(t.ID, task.StatusTimeout, agent.ErrCodeTimeout, "task timed out before the agent started")
	case errors.Is(ctx.Err(), context.Canceled):
		e.settle(t.ID, task.StatusCancelled, agent.ErrCodeGatewayShutdown, "task cancelled")
	default:
		code, msg := agent.ErrCodeAgentUnavailable, err.Error()
		if ae, ok := agent.AsError(err); ok {
			code, msg = ae.Code, ae.Message
		}
		e.metrics.StartFailures.Add(1)
		e.settle(t.ID, task.StatusFailed, code, msg)
	}
}

func (e *Executor) consume(ctx context.Context, t *task.Task, ch <-chan agent.AgentEvent) {
	settled := false
	for ev := range ch {
		if ev.Timestamp.IsZero() {
			ev.Timestamp = time.Now().UTC()
		}
		e.events.Publish(t.ID, ev)
		e.metrics.EventsPublished.Add(1)
		if settled {
			continue
		}
		switch ev.Type {
		case agent.EventCompleted:
			if e.finish(t.ID, task.StatusSucceeded, "", "", func(x *task.Task) {
				if ev.Payload != nil {
					x.Result = append(json.RawMessage(nil), ev.Payload...)
				} else {
					x.Result = json.RawMessage(`{}`)
				}
			}) {
				settled = true
			}
		case agent.EventFailed:
			code, msg := parseFailure(ev.Payload)
			if e.finish(t.ID, task.StatusFailed, code, msg, nil) {
				settled = true
			}
		}
	}
	if settled {
		return
	}

	// The stream closed without a terminal event: decide why, then make sure
	// subscribers still get a terminal signal.
	switch {
	case e.wasUserCancelled(t.ID):
		e.settle(t.ID, task.StatusCancelled, agent.ErrCodeTaskCancelled, "task cancelled by client")
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		e.settle(t.ID, task.StatusTimeout, agent.ErrCodeTimeout, "task exceeded its execution timeout")
	case errors.Is(ctx.Err(), context.Canceled):
		e.settle(t.ID, task.StatusCancelled, agent.ErrCodeGatewayShutdown, "task context cancelled")
	default:
		e.settle(t.ID, task.StatusFailed, agent.ErrCodeStreamInterrupted, "agent closed the stream without a terminal event")
	}
}

// finish applies a terminal transition and updates counters exactly once.
func (e *Executor) finish(taskID string, status task.Status, code, message string, apply func(*task.Task)) bool {
	err := e.repo.Transition(taskID, status, func(x *task.Task) {
		if code != "" {
			x.ErrorCode = code
		}
		if message != "" {
			x.ErrorMessage = message
		}
		if apply != nil {
			apply(x)
		}
	})
	if err != nil {
		if !task.IsTransitionError(err) && !errors.Is(err, task.ErrTaskNotFound) {
			e.log.Warn("terminal transition failed", "task_id", taskID, "to", status, "err", err)
		} else {
			e.log.Debug("terminal state already set", "task_id", taskID, "to", status, "err", err)
		}
		return false
	}
	switch status {
	case task.StatusSucceeded:
		e.metrics.Succeeded.Add(1)
	case task.StatusFailed:
		e.metrics.Failed.Add(1)
		if code == agent.ErrCodeStreamInterrupted {
			e.metrics.StreamInterrupted.Add(1)
		}
	case task.StatusTimeout:
		e.metrics.TimedOut.Add(1)
	case task.StatusCancelled:
		e.metrics.Cancelled.Add(1)
	}
	return true
}

// settle moves a task to a terminal state and, when this call wins the CAS,
// publishes a failed terminal event for subscribers.
//
// It is the counterpart of the agent-provided terminal event: cancel, timeout,
// start failure, truncated stream and panic all end a task without the agent
// ever sending completed/failed. Without this, subscribers of those tasks saw a
// stream that simply closed and had to poll /result to learn the outcome.
func (e *Executor) settle(taskID string, status task.Status, code, message string) bool {
	if !e.finish(taskID, status, code, message, nil) {
		return false
	}
	e.events.Publish(taskID, agent.TerminalFailure(code, message))
	e.metrics.EventsPublished.Add(1)
	return true
}

// Cancel requests cancellation of a running task. It returns true because
// cancellation is accepted asynchronously even if the task is not registered
// yet (the request is remembered and applied when it starts).
func (e *Executor) Cancel(taskID string) bool {
	e.mu.Lock()
	e.userCancelled[taskID] = struct{}{}
	if fn, ok := e.cancels[taskID]; ok {
		e.mu.Unlock()
		fn()
		return true
	}
	e.cancelRequested[taskID] = struct{}{}
	e.mu.Unlock()
	return true
}

// CancelAll cancels every running task (used at shutdown deadline). It is not
// a client cancellation: tasks terminate as CANCELLED with error_code
// gateway_shutdown because the parent context is the cancellation cause.
func (e *Executor) CancelAll() {
	e.mu.Lock()
	fns := make([]context.CancelFunc, 0, len(e.cancels))
	for _, fn := range e.cancels {
		fns = append(fns, fn)
	}
	e.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

// Fail force-fails a task (used by worker panic recovery).
func (e *Executor) Fail(taskID, code, message string) {
	e.settle(taskID, task.StatusFailed, code, message)
	e.events.Close(taskID)
}

func (e *Executor) register(taskID string, cancel context.CancelFunc) {
	e.mu.Lock()
	e.cancels[taskID] = cancel
	_, pending := e.cancelRequested[taskID]
	delete(e.cancelRequested, taskID)
	e.mu.Unlock()
	if pending {
		cancel()
	}
}

func (e *Executor) unregister(taskID string) {
	e.mu.Lock()
	delete(e.cancels, taskID)
	delete(e.cancelRequested, taskID)
	delete(e.userCancelled, taskID)
	e.mu.Unlock()
}

func (e *Executor) wasUserCancelled(taskID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.userCancelled[taskID]
	return ok
}

func parseFailure(payload json.RawMessage) (string, string) {
	code, msg := "agent_failed", "agent reported failure"
	if len(payload) == 0 {
		return code, msg
	}
	var p struct {
		ErrorCode string `json:"error_code"`
		Code      string `json:"code"`
		Message   string `json:"message"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return code, string(payload)
	}
	switch {
	case p.ErrorCode != "":
		code = p.ErrorCode
	case p.Code != "":
		code = p.Code
	}
	switch {
	case p.Message != "":
		msg = p.Message
	case p.Error != "":
		msg = p.Error
	}
	return code, msg
}

// drain consumes a channel until it closes or the timeout expires. It is used
// after a panic so the adapter goroutine can finish without leaking.
func drain(ch <-chan agent.AgentEvent, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-timer.C:
			return
		}
	}
}
