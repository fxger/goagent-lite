// Package repository provides the in-memory task repository.
package repository

import (
	"errors"
	"sync"
	"time"

	"goagent-lite/internal/task"
)

// ErrAlreadyExists is returned when a task ID collides.
var ErrAlreadyExists = errors.New("task already exists")

// Memory is a concurrency-safe, in-memory task repository.
//
// State transitions are applied under the write lock: the current state is
// checked, the transition is validated, and only then is the task mutated.
// This gives us compare-and-set semantics for terminal states: the first
// writer wins and every later writer observes an invalid transition.
type Memory struct {
	mu    sync.RWMutex
	tasks map[string]*task.Task
	now   func() time.Time
}

// NewMemory builds an empty repository.
func NewMemory() *Memory {
	return &Memory{tasks: make(map[string]*task.Task), now: func() time.Time { return time.Now().UTC() }}
}

// Create stores a copy of t. It fails if the ID already exists.
func (m *Memory) Create(t *task.Task) error {
	if t == nil || t.ID == "" {
		return errors.New("repository: task and task id are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tasks[t.ID]; ok {
		return ErrAlreadyExists
	}
	m.tasks[t.ID] = t.Clone()
	return nil
}

// Get returns a deep copy of the task with the given ID.
func (m *Memory) Get(id string) (*task.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, task.ErrTaskNotFound
	}
	return t.Clone(), nil
}

// Transition atomically moves a task to state to. apply may mutate the task
// within the critical section; if apply returns an error the transition is
// aborted. Terminal timestamps are filled in automatically.
func (m *Memory) Transition(id string, to task.Status, apply func(*task.Task)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return task.ErrTaskNotFound
	}
	if !task.CanTransition(t.Status, to) {
		return &task.TransitionError{From: t.Status, To: to}
	}
	if apply != nil {
		apply(t)
	}
	t.Status = to
	if to.IsTerminal() && t.FinishedAt == nil {
		now := m.now()
		t.FinishedAt = &now
	}
	return nil
}

// List returns deep copies of every task currently retained.
func (m *Memory) List() []*task.Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*task.Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t.Clone())
	}
	return out
}

// Delete removes a task.
func (m *Memory) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, id)
	return nil
}

// Len returns the number of retained tasks.
func (m *Memory) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.tasks)
}
