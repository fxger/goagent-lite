// Package event owns event sequencing, fan-out, replay and per-subscriber
// bounded buffers. The gateway is the only sequence authority.
package event

import (
	"encoding/json"
	"sync"
	"time"

	"goagent-lite/internal/agent"
)

// Event is the externally visible event: the agent event plus gateway-owned
// task_id and sequence.
type Event struct {
	TaskID    string          `json:"task_id"`
	Sequence  int64           `json:"sequence"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type subscriber struct {
	ch chan Event
}

type stream struct {
	events []Event
	subs   map[uint64]*subscriber
	closed bool
}

// Manager stores per-task event streams and fans them out.
type Manager struct {
	mu      sync.Mutex
	streams map[string]*stream
	buffer  int
	nextSub uint64

	// OnSlowConsumer is invoked when a subscriber's bounded buffer is full and
	// the subscription is dropped. It must not block.
	OnSlowConsumer func()
}

// New creates an event manager with the given per-subscriber buffer size.
func New(subscriberBuffer int) *Manager {
	if subscriberBuffer < 1 {
		subscriberBuffer = 1
	}
	return &Manager{streams: make(map[string]*stream), buffer: subscriberBuffer}
}

// Publish assigns the next sequence number and fans the event out.
// A full subscriber buffer disconnects that subscriber only.
func (m *Manager) Publish(taskID string, ev agent.AgentEvent) Event {
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.streams[taskID]
	if !ok {
		st = &stream{subs: make(map[uint64]*subscriber)}
		m.streams[taskID] = st
	}
	e := Event{
		TaskID:    taskID,
		Sequence:  int64(len(st.events) + 1),
		Type:      agent.NormalizeEventType(ev.Type),
		Timestamp: ev.Timestamp,
		Payload:   ev.Payload,
	}
	st.events = append(st.events, e)
	if st.closed {
		return e
	}
	for id, sub := range st.subs {
		select {
		case sub.ch <- e:
		default:
			close(sub.ch)
			delete(st.subs, id)
			if m.OnSlowConsumer != nil {
				m.OnSlowConsumer()
			}
		}
	}
	return e
}

// Open subscribes to a task stream and returns the historical events plus the
// live channel. If the stream is already closed, the returned channel is
// closed after the history. cancel releases the subscription; it is idempotent.
func (m *Manager) Open(taskID string) ([]Event, <-chan Event, func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.streams[taskID]
	if !ok {
		st = &stream{subs: make(map[uint64]*subscriber)}
		m.streams[taskID] = st
	}
	history := make([]Event, len(st.events))
	copy(history, st.events)
	if st.closed {
		ch := make(chan Event)
		close(ch)
		return history, ch, func() {}
	}
	m.nextSub++
	id := m.nextSub
	sub := &subscriber{ch: make(chan Event, m.buffer)}
	st.subs[id] = sub
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			if _, exists := st.subs[id]; exists {
				delete(st.subs, id)
				close(sub.ch)
			}
		})
	}
	return history, sub.ch, cancel
}

// Close ends the live stream of a task after its terminal event has been
// published: all subscribers are disconnected cleanly. History is retained
// until Purge.
func (m *Manager) Close(taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.streams[taskID]
	if !ok {
		// The task reached a terminal state without ever publishing an event
		// (e.g. rejected before start or cancelled while QUEUED). Record the
		// closed stream so future subscribers terminate immediately.
		st = &stream{subs: make(map[uint64]*subscriber)}
		m.streams[taskID] = st
	}
	if st.closed {
		return
	}
	st.closed = true
	for id, sub := range st.subs {
		close(sub.ch)
		delete(st.subs, id)
	}
}

// Purge drops all retained events and subscriptions for a task.
func (m *Manager) Purge(taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.streams[taskID]
	if !ok {
		return
	}
	for id, sub := range st.subs {
		close(sub.ch)
		delete(st.subs, id)
	}
	delete(m.streams, taskID)
}

// History returns a copy of the retained events for a task.
func (m *Manager) History(taskID string) []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.streams[taskID]
	if !ok {
		return nil
	}
	out := make([]Event, len(st.events))
	copy(out, st.events)
	return out
}
