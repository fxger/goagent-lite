package agent

import (
	"context"
	"errors"
	"testing"
	"time"
)

type nopAdapter struct{}

func (nopAdapter) Start(ctx context.Context, req AgentRequest) (<-chan AgentEvent, error) {
	ch := make(chan AgentEvent)
	close(ch)
	return ch, nil
}

func TestKeyAndNormalize(t *testing.T) {
	if got := Key("a", "v1"); got != "a@v1" {
		t.Fatalf("key = %q", got)
	}
	if got := NormalizeEventType("bogus"); got != EventCustom {
		t.Fatalf("normalize = %q", got)
	}
	for _, typ := range []string{EventStarted, EventMessage, EventToken, EventToolCall, EventToolResult,
		EventProgress, EventError, EventCompleted, EventFailed, EventCustom} {
		if got := NormalizeEventType(typ); got != typ {
			t.Errorf("normalize(%q) = %q", typ, got)
		}
	}
	if !IsTerminalEvent(EventCompleted) || !IsTerminalEvent(EventFailed) {
		t.Fatal("completed/failed must be terminal")
	}
	if IsTerminalEvent(EventStarted) || IsTerminalEvent(EventError) {
		t.Fatal("started/error must not be terminal")
	}
}

func TestRegistryValidation(t *testing.T) {
	_, err := NewRegistry([]Definition{
		{ID: "a", Version: "v1", Adapter: nopAdapter{}},
		{ID: "a", Version: "v1", Adapter: nopAdapter{}},
	})
	if err == nil {
		t.Fatal("duplicate agent must be rejected")
	}
	_, err = NewRegistry([]Definition{{ID: "", Version: "v1"}})
	if err == nil {
		t.Fatal("empty id must be rejected")
	}
	_, err = NewRegistry([]Definition{{ID: "a", Version: ""}})
	if err == nil {
		t.Fatal("empty version must be rejected")
	}
}

func TestRegistryLookupAllAndRouter(t *testing.T) {
	reg, err := NewRegistry([]Definition{
		{ID: "a", Version: "v1", Enabled: true, Adapter: nopAdapter{}},
		{ID: "a", Version: "v2", Enabled: false, Adapter: nopAdapter{}},
		{ID: "b", Version: "v1", Enabled: true, Adapter: nopAdapter{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Lookup("a", "v1"); !ok {
		t.Fatal("a@v1 missing")
	}
	if _, ok := reg.Lookup("a", "v9"); ok {
		t.Fatal("unexpected a@v9")
	}
	if len(reg.All()) != 3 {
		t.Fatalf("all = %d", len(reg.All()))
	}
	router := NewRouter(reg)
	if _, err := router.Resolve("a", "v2"); !errors.Is(err, ErrAgentDisabled) {
		t.Fatalf("disabled agent err = %v", err)
	}
	if _, err := router.Resolve("zzz", "v1"); !errors.Is(err, ErrAgentNotFound) {
		t.Fatalf("unknown agent err = %v", err)
	}
	def, err := router.Resolve("b", "v1")
	if err != nil || def.ID != "b" {
		t.Fatalf("resolve b: %v", err)
	}
}

func TestAgentError(t *testing.T) {
	err := &Error{Code: ErrCodeAgentUnavailable, Message: "boom"}
	if err.Error() != "agent_unavailable: boom" {
		t.Fatalf("error string = %q", err.Error())
	}
	ae, ok := AsError(err)
	if !ok || ae.Code != ErrCodeAgentUnavailable {
		t.Fatal("AsError failed")
	}
	if _, ok := AsError(context.DeadlineExceeded); ok {
		t.Fatal("plain error must not convert")
	}
}

func TestEventTimestampHelpers(t *testing.T) {
	ch := make(chan AgentEvent, 1)
	ch <- AgentEvent{Type: EventStarted, Timestamp: time.Now().UTC()}
	close(ch)
	ev := <-ch
	if ev.Timestamp.IsZero() {
		t.Fatal("timestamp missing")
	}
}
