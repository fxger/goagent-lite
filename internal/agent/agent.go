// Package agent defines the language-neutral agent protocol types, the static
// agent registry and the router that resolves a task to an adapter.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Protocol identifiers accepted in the configuration.
const (
	ProtocolHTTP  = "http"
	ProtocolLocal = "local"
)

// AgentRequest is what the gateway hands to an adapter. It never contains a
// sequence number: the gateway's Event Manager is the only sequence authority.
type AgentRequest struct {
	TaskID   string            `json:"task_id"`
	Input    json.RawMessage   `json:"input"`
	Config   json.RawMessage   `json:"config,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// AgentEvent is an adapter-level event. task_id and sequence are added by the
// gateway before the event is exposed to subscribers.
type AgentEvent struct {
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// Event type definitions (v2 protocol).
const (
	EventStarted    = "started"
	EventMessage    = "message"
	EventToken      = "token"
	EventToolCall   = "tool_call"
	EventToolResult = "tool_result"
	EventProgress   = "progress"
	EventError      = "error"
	EventCompleted  = "completed"
	EventFailed     = "failed"
	EventCustom     = "custom"
)

var knownEventTypes = map[string]struct{}{
	EventStarted:    {},
	EventMessage:    {},
	EventToken:      {},
	EventToolCall:   {},
	EventToolResult: {},
	EventProgress:   {},
	EventError:      {},
	EventCompleted:  {},
	EventFailed:     {},
	EventCustom:     {},
}

// NormalizeEventType maps unknown event types to "custom", as required by the
// wire protocol.
func NormalizeEventType(t string) string {
	if _, ok := knownEventTypes[t]; ok {
		return t
	}
	return EventCustom
}

// IsTerminalEvent reports whether an event type terminates the stream.
func IsTerminalEvent(t string) bool {
	return t == EventCompleted || t == EventFailed
}

// FailurePayload renders the payload of a failed terminal event.
func FailurePayload(code, message string) json.RawMessage {
	payload := map[string]string{"error_code": code}
	if message != "" {
		payload["message"] = message
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return json.RawMessage(`{"error_code":"internal_panic"}`)
	}
	return b
}

// TerminalFailure builds a failed terminal event.
//
// The gateway uses it when it ends a task on its own — cancellation, timeout,
// start failure, truncated stream, panic — so SSE subscribers always observe a
// terminal signal instead of a stream that simply stops. The task's real state
// remains authoritative through GET /api/v1/tasks/{id}; the event type is only
// the stream terminator, and error_code carries the actual cause.
func TerminalFailure(code, message string) AgentEvent {
	return AgentEvent{Type: EventFailed, Payload: FailurePayload(code, message)}
}

// AgentAdapter is the single extension point for agent integrations.
//
// Contract:
//   - Implementations must be stateless and safe for concurrent use.
//   - Start returning an error means "failed to start" (unavailable, rejected,
//     protocol setup failure); failures during execution are reported as a
//     failed event or by closing the stream without a terminal event.
//   - The adapter is the only closer of the returned channel. It must send a
//     terminal event on the happy path and must close the channel on every
//     path, including context cancellation.
//   - The channel is bounded; the adapter applies backpressure instead of
//     dropping events. Dropping happens only at the subscriber fan-out layer.
type AgentAdapter interface {
	Start(ctx context.Context, req AgentRequest) (<-chan AgentEvent, error)
}

// Error codes understood by the executor and reported to clients.
const (
	ErrCodeAgentUnavailable  = "agent_unavailable"
	ErrCodeAgentError        = "agent_error"
	ErrCodeStreamInterrupted = "agent_stream_interrupted"
	ErrCodeProtocolViolation = "agent_protocol_violation"
	ErrCodeTimeout           = "agent_timeout"
	ErrCodeInternalPanic     = "internal_panic"
	ErrCodeTaskCancelled     = "task_cancelled"
	ErrCodeGatewayShutdown   = "gateway_shutdown"
)

// Error is an adapter error carrying a gateway error code.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

// NewError builds an adapter error.
func NewError(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

// AsError extracts an *Error from err, if present.
func AsError(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// Definition is the validated static configuration of one agent.
type Definition struct {
	ID             string
	Version        string
	Protocol       string
	Endpoint       string
	Enabled        bool
	MaxConcurrency int
	QueueCapacity  int
	TimeoutSeconds int
	Adapter        AgentAdapter
}

// Key builds the unique registry key.
func Key(id, version string) string {
	return id + "@" + version
}

// Registered is an immutable registry entry.
type Registered struct {
	Definition
}

// Key returns the registry key of the agent.
func (r *Registered) Key() string { return Key(r.ID, r.Version) }

// Registry is a read-only agent registry built at startup.
type Registry struct {
	byKey map[string]*Registered
	order []string
}

// NewRegistry validates and freezes a set of agent definitions.
func NewRegistry(defs []Definition) (*Registry, error) {
	r := &Registry{byKey: make(map[string]*Registered, len(defs))}
	for _, d := range defs {
		if d.ID == "" || d.Version == "" {
			return nil, fmt.Errorf("agent: id and version are required")
		}
		if d.Adapter == nil {
			return nil, fmt.Errorf("agent %s@%s: adapter is required", d.ID, d.Version)
		}
		key := Key(d.ID, d.Version)
		if _, exists := r.byKey[key]; exists {
			return nil, fmt.Errorf("agent: duplicate agent %s", key)
		}
		cp := d
		r.byKey[key] = &Registered{Definition: cp}
		r.order = append(r.order, key)
	}
	return r, nil
}

// Lookup returns the registered agent for id/version.
func (r *Registry) Lookup(id, version string) (*Registered, bool) {
	reg, ok := r.byKey[Key(id, version)]
	return reg, ok
}

// All returns all registered agents in configuration order.
func (r *Registry) All() []*Registered {
	out := make([]*Registered, 0, len(r.order))
	for _, k := range r.order {
		out = append(out, r.byKey[k])
	}
	return out
}

// ErrAgentNotFound is returned by Router for an unknown agent.
var ErrAgentNotFound = errors.New("agent not found")

// ErrAgentDisabled is returned by Router for a disabled agent.
var ErrAgentDisabled = errors.New("agent disabled")

// Router performs the static checks that happen before a task is accepted.
// Dynamic concurrency accounting is owned by the scheduler.
type Router struct {
	registry *Registry
}

// NewRouter wraps a registry.
func NewRouter(r *Registry) *Router {
	return &Router{registry: r}
}

// Resolve validates existence and enabled state.
func (r *Router) Resolve(id, version string) (*Registered, error) {
	reg, ok := r.registry.Lookup(id, version)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAgentNotFound, Key(id, version))
	}
	if !reg.Enabled {
		return nil, fmt.Errorf("%w: %s", ErrAgentDisabled, Key(id, version))
	}
	return reg, nil
}
