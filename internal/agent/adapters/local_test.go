package adapters

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"goagent-lite/internal/agent"
)

func collect(t *testing.T, ch <-chan agent.AgentEvent) []agent.AgentEvent {
	t.Helper()
	var out []agent.AgentEvent
	timeout := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-timeout:
			t.Fatal("timed out collecting events")
		}
	}
}

func TestLocalSuccessStream(t *testing.T) {
	a := NewLocal(LocalOptions{Duration: 20 * time.Millisecond, Tokens: 3, TokenInterval: time.Millisecond})
	ch, err := a.Start(context.Background(), agent.AgentRequest{TaskID: "t1", Input: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	events := collect(t, ch)
	if len(events) < 5 {
		t.Fatalf("events = %d, want started+3 tokens+completed", len(events))
	}
	if events[0].Type != agent.EventStarted {
		t.Fatalf("first event = %s", events[0].Type)
	}
	tokens := 0
	for _, ev := range events[1 : len(events)-1] {
		if ev.Type == agent.EventToken {
			tokens++
		}
	}
	if tokens != 3 {
		t.Fatalf("tokens = %d", tokens)
	}
	last := events[len(events)-1]
	if last.Type != agent.EventCompleted {
		t.Fatalf("last event = %s", last.Type)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["task_id"] != "t1" {
		t.Fatalf("payload = %v", payload)
	}
	for _, ev := range events {
		if ev.Timestamp.IsZero() {
			t.Fatal("event without timestamp")
		}
	}
}

func TestLocalFailure(t *testing.T) {
	a := NewLocal(LocalOptions{FailRate: 1, FailureCode: "boom", FailureMessage: "kaboom"})
	ch, _ := a.Start(context.Background(), agent.AgentRequest{TaskID: "t"})
	events := collect(t, ch)
	if len(events) != 2 || events[1].Type != agent.EventFailed {
		t.Fatalf("events = %+v", events)
	}
	var p map[string]string
	_ = json.Unmarshal(events[1].Payload, &p)
	if p["error_code"] != "boom" || p["message"] != "kaboom" {
		t.Fatalf("payload = %v", p)
	}
}

func TestLocalPanicIsRecovered(t *testing.T) {
	a := NewLocal(LocalOptions{PanicRate: 1})
	ch, _ := a.Start(context.Background(), agent.AgentRequest{TaskID: "t"})
	events := collect(t, ch)
	last := events[len(events)-1]
	if last.Type != agent.EventFailed {
		t.Fatalf("last = %s", last.Type)
	}
	var p map[string]string
	_ = json.Unmarshal(last.Payload, &p)
	if p["error_code"] != agent.ErrCodeInternalPanic {
		t.Fatalf("error_code = %q", p["error_code"])
	}
}

func TestLocalCancellationClosesStream(t *testing.T) {
	a := NewLocal(LocalOptions{Duration: 5 * time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	ch, _ := a.Start(ctx, agent.AgentRequest{TaskID: "t"})
	if ev := <-ch; ev.Type != agent.EventStarted {
		t.Fatalf("first = %s", ev.Type)
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected event after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("adapter did not close the stream after cancellation")
	}
}
