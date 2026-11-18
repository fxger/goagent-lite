// Package adapters contains the concrete agent adapters.
package adapters

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	"goagent-lite/internal/agent"
)

// LocalOptions configures the in-process mock agent.
type LocalOptions struct {
	Duration       time.Duration
	Jitter         time.Duration
	FailRate       float64
	PanicRate      float64
	Tokens         int
	TokenInterval  time.Duration
	FailureCode    string
	FailureMessage string
	Buffer         int
}

// LocalAdapter is a stateless, configurable mock agent used for load and
// reliability testing without any network involvement.
type LocalAdapter struct {
	opts LocalOptions
}

// NewLocal builds a local mock adapter.
func NewLocal(opts LocalOptions) *LocalAdapter {
	if opts.Buffer <= 0 {
		opts.Buffer = 64
	}
	if opts.FailureCode == "" {
		opts.FailureCode = "mock_agent_failure"
	}
	if opts.FailureMessage == "" {
		opts.FailureMessage = "mock agent simulated a failure"
	}
	if opts.TokenInterval <= 0 {
		opts.TokenInterval = 20 * time.Millisecond
	}
	return &LocalAdapter{opts: opts}
}

func (a *LocalAdapter) send(ctx context.Context, ch chan<- agent.AgentEvent, ev agent.AgentEvent) bool {
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	select {
	case ch <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

// Start launches the mock execution. It supports fixed/jittered duration,
// configurable failure, panic, token streaming and context cancellation.
func (a *LocalAdapter) Start(ctx context.Context, req agent.AgentRequest) (<-chan agent.AgentEvent, error) {
	ch := make(chan agent.AgentEvent, a.opts.Buffer)
	go func() {
		defer close(ch)
		defer func() {
			if r := recover(); r != nil {
				// The adapter owns the channel; a simulated panic is reported as
				// a terminal failed event so the executor sees a well-formed
				// stream. The worker itself stays alive.
				payload, _ := json.Marshal(map[string]string{
					"error_code": agent.ErrCodeInternalPanic,
					"message":    "mock agent panic",
				})
				a.send(ctx, ch, agent.AgentEvent{Type: agent.EventFailed, Payload: payload})
			}
		}()

		if !a.send(ctx, ch, agent.AgentEvent{Type: agent.EventStarted, Payload: json.RawMessage(`{}`)}) {
			return
		}

		duration := a.opts.Duration
		if a.opts.Jitter > 0 {
			duration += time.Duration(rand.Int63n(int64(a.opts.Jitter)))
		}

		if a.opts.PanicRate > 0 && rand.Float64() < a.opts.PanicRate {
			sleepCtx(ctx, duration)
			panic("mock agent simulated panic")
		}

		if a.opts.Tokens > 0 {
			interval := a.opts.TokenInterval
			if duration > 0 && time.Duration(a.opts.Tokens)*interval > duration {
				interval = duration / time.Duration(a.opts.Tokens)
			}
			for i := 0; i < a.opts.Tokens; i++ {
				if !sleepCtx(ctx, interval) {
					return
				}
				payload, _ := json.Marshal(map[string]any{"text": "token", "index": i})
				if !a.send(ctx, ch, agent.AgentEvent{Type: agent.EventToken, Payload: payload}) {
					return
				}
			}
		}
		if !sleepCtx(ctx, duration) {
			return
		}

		if a.opts.FailRate > 0 && rand.Float64() < a.opts.FailRate {
			payload, _ := json.Marshal(map[string]string{
				"error_code": a.opts.FailureCode,
				"message":    a.opts.FailureMessage,
			})
			a.send(ctx, ch, agent.AgentEvent{Type: agent.EventFailed, Payload: payload})
			return
		}

		payload, _ := json.Marshal(map[string]any{
			"summary":     "mock local agent completed",
			"task_id":     req.TaskID,
			"duration_ms": duration.Milliseconds(),
		})
		a.send(ctx, ch, agent.AgentEvent{Type: agent.EventCompleted, Payload: payload})
	}()
	return ch, nil
}

// sleepCtx sleeps for d unless ctx is cancelled. It returns false on cancel.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
