package adapters

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"goagent-lite/internal/agent"
)

// HTTPOptions configures the remote agent adapter.
type HTTPOptions struct {
	Endpoint string
	Client   *http.Client
	Buffer   int
}

// HTTPAdapter implements the Agent HTTP/SSE wire protocol (docs/protocol.md).
// It is stateless and safe for concurrent use.
type HTTPAdapter struct {
	endpoint string
	client   *http.Client
	buffer   int
}

// NewHTTP builds a remote HTTP adapter.
func NewHTTP(opts HTTPOptions) *HTTPAdapter {
	if opts.Buffer <= 0 {
		opts.Buffer = 64
	}
	client := opts.Client
	if client == nil {
		client = &http.Client{Transport: http.DefaultTransport}
	}
	return &HTTPAdapter{
		endpoint: strings.TrimRight(opts.Endpoint, "/"),
		client:   client,
		buffer:   opts.Buffer,
	}
}

type executeRequest struct {
	TaskID   string            `json:"task_id"`
	Input    json.RawMessage   `json:"input"`
	Config   json.RawMessage   `json:"config,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Start posts the task to the remote agent and consumes its SSE stream.
func (a *HTTPAdapter) Start(ctx context.Context, req agent.AgentRequest) (<-chan agent.AgentEvent, error) {
	body, err := json.Marshal(executeRequest{
		TaskID:   req.TaskID,
		Input:    req.Input,
		Config:   req.Config,
		Metadata: req.Metadata,
	})
	if err != nil {
		return nil, agent.NewError(agent.ErrCodeAgentError, fmt.Sprintf("encode request: %v", err))
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint+"/v1/execute", bytes.NewReader(body))
	if err != nil {
		return nil, agent.NewError(agent.ErrCodeAgentError, fmt.Sprintf("build request: %v", err))
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, agent.NewError(agent.ErrCodeAgentUnavailable, fmt.Sprintf("connect to agent: %v", err))
	}
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, agent.NewError(agent.ErrCodeAgentError,
			fmt.Sprintf("agent returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet))))
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		resp.Body.Close()
		ch := make(chan agent.AgentEvent, 1)
		payload, _ := json.Marshal(map[string]string{
			"error_code": agent.ErrCodeProtocolViolation,
			"message":    "expected Content-Type text/event-stream, got " + ct,
		})
		ch <- agent.AgentEvent{Type: agent.EventFailed, Timestamp: time.Now().UTC(), Payload: payload}
		close(ch)
		return ch, nil
	}

	ch := make(chan agent.AgentEvent, a.buffer)
	go a.consume(ctx, resp.Body, ch)
	return ch, nil
}

func (a *HTTPAdapter) consume(ctx context.Context, body io.ReadCloser, ch chan<- agent.AgentEvent) {
	defer close(ch)
	defer body.Close()

	emit := func(ev agent.AgentEvent) bool {
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
	emitFailure := func(code, message string) {
		payload, _ := json.Marshal(map[string]string{"error_code": code, "message": message})
		emit(agent.AgentEvent{Type: agent.EventFailed, Payload: payload})
	}

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	terminal := false
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" {
			continue
		}
		var raw struct {
			Type      string          `json:"type"`
			Timestamp time.Time       `json:"timestamp"`
			Payload   json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal([]byte(data), &raw); err != nil {
			emitFailure(agent.ErrCodeProtocolViolation, "invalid event JSON: "+err.Error())
			return
		}
		if raw.Type == "" {
			emitFailure(agent.ErrCodeProtocolViolation, "event is missing type")
			return
		}
		ev := agent.AgentEvent{
			Type:      agent.NormalizeEventType(raw.Type),
			Timestamp: raw.Timestamp,
			Payload:   raw.Payload,
		}
		if !emit(ev) {
			return
		}
		if agent.IsTerminalEvent(ev.Type) {
			terminal = true
			return
		}
	}
	if ctx.Err() != nil {
		return
	}
	if err := scanner.Err(); err != nil {
		emitFailure(agent.ErrCodeStreamInterrupted, "read event stream: "+err.Error())
		return
	}
	if !terminal {
		emitFailure(agent.ErrCodeStreamInterrupted, "agent closed the stream without a terminal event")
	}
}
