package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"goagent-lite/internal/agent"
)

func sseServer(t *testing.T, lines []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/execute" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Accept"); got != "text/event-stream" {
			t.Errorf("Accept = %q", got)
		}
		var body struct {
			TaskID   string            `json:"task_id"`
			Input    json.RawMessage   `json:"input"`
			Config   json.RawMessage   `json:"config"`
			Metadata map[string]string `json:"metadata"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if body.TaskID != "t1" || string(body.Input) != `{"x":1}` || body.Metadata["k"] != "v" {
			t.Errorf("bad request body: %+v", body)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		for _, line := range lines {
			_, _ = io.WriteString(w, line+"\n\n")
			fl.Flush()
		}
	}))
}

func TestHTTPSuccessStream(t *testing.T) {
	srv := sseServer(t, []string{
		`data: {"type":"started","timestamp":"2026-01-01T00:00:00Z","payload":{}}`,
		`data: {"type":"token","payload":{"text":"he"}}`,
		`: comment line ignored`,
		`data: {"type":"completed","payload":{"ok":true}}`,
	})
	defer srv.Close()
	a := NewHTTP(HTTPOptions{Endpoint: srv.URL})
	ch, err := a.Start(context.Background(), agent.AgentRequest{
		TaskID: "t1", Input: json.RawMessage(`{"x":1}`), Metadata: map[string]string{"k": "v"},
	})
	if err != nil {
		t.Fatal(err)
	}
	events := collect(t, ch)
	if len(events) != 3 {
		t.Fatalf("events = %+v", events)
	}
	if events[0].Type != agent.EventStarted || !events[0].Timestamp.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("started = %+v", events[0])
	}
	if events[1].Type != agent.EventToken || string(events[1].Payload) != `{"text":"he"}` {
		t.Fatalf("token = %+v", events[1])
	}
	if events[2].Type != agent.EventCompleted || string(events[2].Payload) != `{"ok":true}` {
		t.Fatalf("completed = %+v", events[2])
	}
}

func TestHTTPUnknownTypeBecomesCustom(t *testing.T) {
	srv := sseServer(t, []string{
		`data: {"type":"something_new","payload":{}}`,
		`data: {"type":"completed","payload":{}}`,
	})
	defer srv.Close()
	ch, err := NewHTTP(HTTPOptions{Endpoint: srv.URL}).Start(context.Background(), agent.AgentRequest{TaskID: "t1", Input: json.RawMessage(`{"x":1}`), Metadata: map[string]string{"k": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	events := collect(t, ch)
	if len(events) != 2 || events[0].Type != agent.EventCustom {
		t.Fatalf("events = %+v", events)
	}
}

func TestHTTPNon200IsStartError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "agent overloaded", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	_, err := NewHTTP(HTTPOptions{Endpoint: srv.URL}).Start(context.Background(), agent.AgentRequest{TaskID: "t"})
	if err == nil {
		t.Fatal("expected error")
	}
	ae, ok := agent.AsError(err)
	if !ok || ae.Code != agent.ErrCodeAgentError || !strings.Contains(ae.Message, "503") {
		t.Fatalf("err = %v", err)
	}
}

func TestHTTPConnectionRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()
	_, err := NewHTTP(HTTPOptions{Endpoint: url}).Start(context.Background(), agent.AgentRequest{TaskID: "t"})
	ae, ok := agent.AsError(err)
	if !ok || ae.Code != agent.ErrCodeAgentUnavailable {
		t.Fatalf("err = %v", err)
	}
}

func TestHTTPWrongContentType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()
	ch, err := NewHTTP(HTTPOptions{Endpoint: srv.URL}).Start(context.Background(), agent.AgentRequest{TaskID: "t"})
	if err != nil {
		t.Fatal(err)
	}
	events := collect(t, ch)
	if len(events) != 1 || events[0].Type != agent.EventFailed {
		t.Fatalf("events = %+v", events)
	}
	var p map[string]string
	_ = json.Unmarshal(events[0].Payload, &p)
	if p["error_code"] != agent.ErrCodeProtocolViolation {
		t.Fatalf("code = %q", p["error_code"])
	}
}

func TestHTTPInvalidEventJSON(t *testing.T) {
	srv := sseServer(t, []string{`data: {not json}`})
	defer srv.Close()
	ch, err := NewHTTP(HTTPOptions{Endpoint: srv.URL}).Start(context.Background(), agent.AgentRequest{TaskID: "t1", Input: json.RawMessage(`{"x":1}`), Metadata: map[string]string{"k": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	events := collect(t, ch)
	if len(events) != 1 || events[0].Type != agent.EventFailed {
		t.Fatalf("events = %+v", events)
	}
	var p map[string]string
	_ = json.Unmarshal(events[0].Payload, &p)
	if p["error_code"] != agent.ErrCodeProtocolViolation {
		t.Fatalf("code = %q", p["error_code"])
	}
}

func TestHTTPMissingType(t *testing.T) {
	srv := sseServer(t, []string{`data: {"payload":{}}`})
	defer srv.Close()
	ch, _ := NewHTTP(HTTPOptions{Endpoint: srv.URL}).Start(context.Background(), agent.AgentRequest{TaskID: "t1", Input: json.RawMessage(`{"x":1}`), Metadata: map[string]string{"k": "v"}})
	events := collect(t, ch)
	if len(events) != 1 || events[0].Type != agent.EventFailed {
		t.Fatalf("events = %+v", events)
	}
}

func TestHTTPStreamWithoutTerminal(t *testing.T) {
	srv := sseServer(t, []string{`data: {"type":"started","payload":{}}`})
	defer srv.Close()
	ch, _ := NewHTTP(HTTPOptions{Endpoint: srv.URL}).Start(context.Background(), agent.AgentRequest{TaskID: "t1", Input: json.RawMessage(`{"x":1}`), Metadata: map[string]string{"k": "v"}})
	events := collect(t, ch)
	last := events[len(events)-1]
	if last.Type != agent.EventFailed {
		t.Fatalf("last = %s", last.Type)
	}
	var p map[string]string
	_ = json.Unmarshal(last.Payload, &p)
	if p["error_code"] != agent.ErrCodeStreamInterrupted {
		t.Fatalf("code = %q", p["error_code"])
	}
}

func TestHTTPContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		fmt.Fprintln(w, `data: {"type":"started","payload":{}}`)
		fl.Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := NewHTTP(HTTPOptions{Endpoint: srv.URL}).Start(ctx, agent.AgentRequest{TaskID: "t1", Input: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if ev := <-ch; ev.Type != agent.EventStarted {
		t.Fatalf("first = %s", ev.Type)
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel after cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("adapter did not close the stream")
	}
}
