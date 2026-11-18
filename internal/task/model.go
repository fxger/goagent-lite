// Package task contains the task domain model, the task state machine and the
// task manager facade used by the HTTP layer.
package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Status is the lifecycle state of a task.
type Status string

const (
	StatusQueued    Status = "QUEUED"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusCancelled Status = "CANCELLED"
	StatusTimeout   Status = "TIMEOUT"
)

// IsTerminal reports whether s is a final state. Terminal states can never be
// changed again (CAS / single-set semantics).
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled, StatusTimeout:
		return true
	default:
		return false
	}
}

// Valid reports whether s is one of the defined states.
func (s Status) Valid() bool {
	switch s {
	case StatusQueued, StatusRunning, StatusSucceeded, StatusFailed, StatusCancelled, StatusTimeout:
		return true
	default:
		return false
	}
}

// transitionTable is the single source of truth for legal state transitions.
// QUEUED -> RUNNING | CANCELLED
// RUNNING -> SUCCEEDED | FAILED | CANCELLED | TIMEOUT
// terminal -> nothing
var transitionTable = map[Status]map[Status]bool{
	StatusQueued: {
		StatusRunning:   true,
		StatusCancelled: true,
	},
	StatusRunning: {
		StatusSucceeded: true,
		StatusFailed:    true,
		StatusCancelled: true,
		StatusTimeout:   true,
	},
}

// CanTransition reports whether from -> to is legal.
func CanTransition(from, to Status) bool {
	return transitionTable[from][to]
}

// TransitionError describes an illegal state transition.
type TransitionError struct {
	From Status
	To   Status
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("invalid task state transition: %s -> %s", e.From, e.To)
}

// IsTransitionError reports whether err is a *TransitionError.
func IsTransitionError(err error) bool {
	var te *TransitionError
	return errors.As(err, &te)
}

// ErrTaskNotFound is returned when a task does not exist (or has been purged).
var ErrTaskNotFound = errors.New("task not found")

// Task is the in-memory representation of a submitted task. All exported
// methods that hand a Task out of a store must return a deep copy.
type Task struct {
	ID             string            `json:"task_id"`
	AgentID        string            `json:"agent_id"`
	AgentVersion   string            `json:"agent_version"`
	Status         Status            `json:"status"`
	Input          json.RawMessage   `json:"input,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	StartedAt      *time.Time        `json:"started_at"`
	FinishedAt     *time.Time        `json:"finished_at"`
	ErrorCode      string            `json:"error_code,omitempty"`
	ErrorMessage   string            `json:"error_message,omitempty"`
	Result         json.RawMessage   `json:"result,omitempty"`
}

// Clone returns a deep copy of t.
func (t *Task) Clone() *Task {
	if t == nil {
		return nil
	}
	c := *t
	if t.Input != nil {
		c.Input = append(json.RawMessage(nil), t.Input...)
	}
	if t.Result != nil {
		c.Result = append(json.RawMessage(nil), t.Result...)
	}
	if t.Metadata != nil {
		c.Metadata = make(map[string]string, len(t.Metadata))
		for k, v := range t.Metadata {
			c.Metadata[k] = v
		}
	}
	if t.StartedAt != nil {
		v := *t.StartedAt
		c.StartedAt = &v
	}
	if t.FinishedAt != nil {
		v := *t.FinishedAt
		c.FinishedAt = &v
	}
	return &c
}

// QueueDuration returns the time spent QUEUED (zero if not started yet).
func (t *Task) QueueDuration() time.Duration {
	if t.StartedAt == nil {
		return 0
	}
	return t.StartedAt.Sub(t.CreatedAt)
}

// ExecutionDuration returns the RUNNING duration (zero if not finished yet).
func (t *Task) ExecutionDuration() time.Duration {
	if t.StartedAt == nil || t.FinishedAt == nil {
		return 0
	}
	return t.FinishedAt.Sub(*t.StartedAt)
}

// Store is the persistence contract consumed by the task manager. The memory
// implementation lives in internal/repository; defining the interface here
// (consumer-side) keeps the dependency direction one-way.
type Store interface {
	Create(t *Task) error
	Get(id string) (*Task, error)
	// Transition performs an atomic state change. apply is invoked while the
	// store lock is held and may mutate the task; returning an error aborts the
	// transition.
	Transition(id string, to Status, apply func(*Task)) error
	List() []*Task
	Delete(id string) error
}
