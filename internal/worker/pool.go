// Package worker implements the fixed worker pool. There are exactly
// max_concurrency workers, each executing exactly one task at a time, so the
// global concurrency limit holds structurally.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"

	"goagent-lite/internal/agent"
	"goagent-lite/internal/executor"
	"goagent-lite/internal/metrics"
	"goagent-lite/internal/scheduler"
	"goagent-lite/internal/task"
)

// Pool is a fixed-size worker pool.
type Pool struct {
	size    int
	sched   *scheduler.Scheduler
	exec    *executor.Executor
	metrics *metrics.Metrics
	log     *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a pool of size workers.
func New(size int, sched *scheduler.Scheduler, exec *executor.Executor, m *metrics.Metrics, log *slog.Logger) *Pool {
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		size:    size,
		sched:   sched,
		exec:    exec,
		metrics: m,
		log:     log,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start launches all workers.
func (p *Pool) Start() {
	p.wg.Add(p.size)
	for i := 0; i < p.size; i++ {
		go p.run(i)
	}
}

// Context returns the pool context (parent of every task context).
func (p *Pool) Context() context.Context { return p.ctx }

func (p *Pool) run(id int) {
	defer p.wg.Done()
	p.metrics.Workers.Add(1)
	defer p.metrics.Workers.Add(-1)
	for {
		job, release, err := p.sched.Next(p.ctx)
		if err != nil {
			return
		}
		p.handle(job, release)
	}
}

func (p *Pool) handle(job *scheduler.Job, release func()) {
	defer release()
	t, ok := job.Value.(*task.Task)
	if !ok || t == nil {
		p.log.Error("worker: job payload is not a task", "job_id", job.ID)
		return
	}
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("worker panic", "task_id", t.ID, "panic", r, "stack", string(debug.Stack()))
			p.metrics.Panics.Add(1)
			p.exec.Fail(t.ID, agent.ErrCodeInternalPanic, fmt.Sprintf("worker panic: %v", r))
		}
	}()
	p.exec.Execute(p.ctx, t)
}

// Stop cancels the pool context and asks the scheduler to stop.
func (p *Pool) Stop() {
	p.cancel()
	p.sched.Close()
}

// Wait blocks until every worker has exited.
func (p *Pool) Wait() { p.wg.Wait() }
