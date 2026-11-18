// Package scheduler owns the per-agent bounded queues, the agent-level
// semaphores and round-robin dispatch. It deliberately knows nothing about the
// task domain: jobs carry an opaque Value that the worker casts back.
package scheduler

import (
	"context"
	"errors"
	"sync"

	"goagent-lite/internal/agent"
)

// Errors returned by the scheduler.
var (
	ErrClosed          = errors.New("scheduler: closed")
	ErrQueueFull       = errors.New("scheduler: global queue is full")
	ErrAgentQueueFull  = errors.New("scheduler: agent queue is full")
	ErrUnknownAgent    = errors.New("scheduler: agent is not registered")
	ErrAgentRegistered = errors.New("scheduler: agent is already registered")
)

// Job is one queued unit of work.
type Job struct {
	ID       string
	AgentKey string
	Value    any
}

// AgentConfig configures one agent's queue and concurrency quota.
type AgentConfig struct {
	ID             string
	Version        string
	MaxConcurrency int
	QueueCapacity  int // 0 means the queue only shares the global capacity
}

type queue struct {
	key   string
	sem   chan struct{}
	items []*Job
	max   int
}

// AgentStat is a point-in-time view of one agent's queue.
type AgentStat struct {
	Key            string `json:"key"`
	Queued         int    `json:"queued"`
	Running        int    `json:"running"`
	MaxConcurrency int    `json:"max_concurrency"`
	QueueCapacity  int    `json:"queue_capacity"`
}

// Scheduler is the shared, concurrency-safe scheduler.
type Scheduler struct {
	mu       sync.Mutex
	queues   map[string]*queue
	order    []string
	cursor   int
	queued   int
	capacity int
	closed   bool
	notify   chan struct{}
	done     chan struct{}
	// cancelled records QUEUED task IDs cancelled by the manager. Removal is
	// lazy: the task stays in the queue and is discarded when dequeued.
	cancelled map[string]struct{}
}

// New creates a scheduler with a global QUEUED capacity.
func New(queueCapacity int) *Scheduler {
	return &Scheduler{
		queues:    make(map[string]*queue),
		capacity:  queueCapacity,
		notify:    make(chan struct{}, 1),
		done:      make(chan struct{}),
		cancelled: make(map[string]struct{}),
	}
}

// RegisterAgent adds an agent queue. It must be called before workers start.
func (s *Scheduler) RegisterAgent(cfg AgentConfig) error {
	if cfg.MaxConcurrency < 1 {
		return errors.New("scheduler: MaxConcurrency must be >= 1")
	}
	key := agent.Key(cfg.ID, cfg.Version)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.queues[key]; exists {
		return ErrAgentRegistered
	}
	s.queues[key] = &queue{
		key: key,
		sem: make(chan struct{}, cfg.MaxConcurrency),
		max: cfg.QueueCapacity,
	}
	s.order = append(s.order, key)
	return nil
}

// QueueCapacity returns the global QUEUED capacity.
func (s *Scheduler) QueueCapacity() int { return s.capacity }

// Enqueue places a job on its agent queue, enforcing both the per-agent queue
// bound (when configured) and the global QUEUED bound.
func (s *Scheduler) Enqueue(job *Job) error {
	if job == nil || job.ID == "" || job.AgentKey == "" {
		return errors.New("scheduler: invalid job")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	q, ok := s.queues[job.AgentKey]
	if !ok {
		return ErrUnknownAgent
	}
	if q.max > 0 && len(q.items) >= q.max {
		return ErrAgentQueueFull
	}
	if s.queued >= s.capacity {
		return ErrQueueFull
	}
	q.items = append(q.items, job)
	s.queued++
	s.signalLocked()
	return nil
}

// MarkCancelled records that a QUEUED task has been cancelled. It is an
// optimisation only: the executor re-checks the repository state (CAS) before
// running a dequeued task, so a missed mark cannot cause execution.
func (s *Scheduler) MarkCancelled(taskID string) {
	s.mu.Lock()
	s.cancelled[taskID] = struct{}{}
	s.mu.Unlock()
	s.signal()
}

// Next blocks until one job can be dispatched.
//
// Dispatch scans the queues round-robin starting at the cursor. For each
// non-empty queue it first tries to acquire the agent's semaphore without
// blocking; only on success does it dequeue. Queues whose quota is exhausted
// are skipped (no head-of-line blocking). Cancelled queued tasks are discarded
// without ever taking a worker slot and without consuming quota.
func (s *Scheduler) Next(ctx context.Context) (*Job, func(), error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, nil, ErrClosed
		}
		job, release, ok := s.takeLocked()
		pending := s.queued
		s.mu.Unlock()
		if ok {
			// Chain-wake one more worker so a burst of queued jobs does not
			// serialize on the single notify slot.
			if pending > 0 {
				s.signal()
			}
			return job, release, nil
		}
		select {
		case <-s.notify:
		case <-s.done:
			return nil, nil, ErrClosed
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

// takeLocked performs one dispatch attempt. Caller holds s.mu.
func (s *Scheduler) takeLocked() (*Job, func(), bool) {
	n := len(s.order)
	if n == 0 {
		return nil, nil, false
	}
	for {
		progressed := false
		for i := 0; i < n; i++ {
			idx := (s.cursor + i) % n
			q := s.queues[s.order[idx]]
			if len(q.items) == 0 {
				continue
			}
			select {
			case q.sem <- struct{}{}:
			default:
				continue // quota exhausted: skip this queue
			}
			job := q.items[0]
			q.items = q.items[1:]
			s.queued--
			s.cursor = (idx + 1) % n
			if _, dead := s.cancelled[job.ID]; dead {
				delete(s.cancelled, job.ID)
				<-q.sem // lazy cancellation must not consume quota
				progressed = true
				break
			}
			release := func() {
				<-q.sem
				s.signal()
			}
			return job, release, true
		}
		if !progressed {
			return nil, nil, false
		}
	}
}

// Close stops dispatch. It is safe to call once; later calls are no-ops.
func (s *Scheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.done)
	s.mu.Unlock()
	s.signal()
}

// Queued returns the current total number of QUEUED jobs.
func (s *Scheduler) Queued() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queued
}

// Snapshot returns the total and per-agent queue state.
func (s *Scheduler) Snapshot() (int, []AgentStat) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := make([]AgentStat, 0, len(s.order))
	for _, key := range s.order {
		q := s.queues[key]
		stats = append(stats, AgentStat{
			Key:            key,
			Queued:         len(q.items),
			Running:        len(q.sem),
			MaxConcurrency: cap(q.sem),
			QueueCapacity:  q.max,
		})
	}
	return s.queued, stats
}

func (s *Scheduler) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *Scheduler) signalLocked() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}
