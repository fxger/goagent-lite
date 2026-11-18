// Package metrics provides lock-free (atomic) counters for the hot paths.
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics holds all gateway counters and gauges. Hot paths only use atomics.
type Metrics struct {
	start time.Time

	Submitted         atomic.Int64
	Succeeded         atomic.Int64
	Failed            atomic.Int64
	TimedOut          atomic.Int64
	Cancelled         atomic.Int64
	Rejected          atomic.Int64
	QueueFullReject   atomic.Int64
	AgentQuotaReject  atomic.Int64
	Panics            atomic.Int64
	SlowConsumers     atomic.Int64
	Running           atomic.Int64
	Workers           atomic.Int64
	EventsPublished   atomic.Int64
	StartFailures     atomic.Int64
	StreamInterrupted atomic.Int64

	// agentRunning maps "agentID@version" to a *atomic.Int64 gauge. sync.Map is
	// read-mostly; updates remain atomic, no mutex map on the hot path.
	agentRunning sync.Map
}

// New creates a Metrics registry with the start time set to now.
func New() *Metrics {
	return &Metrics{start: time.Now()}
}

// StartTime returns the process start time used for uptime.
func (m *Metrics) StartTime() time.Time { return m.start }

// IncAgentRunning increments the per-agent RUNNING gauge.
func (m *Metrics) IncAgentRunning(key string) {
	m.agentGauge(key).Add(1)
}

// DecAgentRunning decrements the per-agent RUNNING gauge.
func (m *Metrics) DecAgentRunning(key string) {
	m.agentGauge(key).Add(-1)
}

func (m *Metrics) agentGauge(key string) *atomic.Int64 {
	if v, ok := m.agentRunning.Load(key); ok {
		return v.(*atomic.Int64)
	}
	v, _ := m.agentRunning.LoadOrStore(key, new(atomic.Int64))
	return v.(*atomic.Int64)
}

// AgentRunningSnapshot copies per-agent RUNNING gauges.
func (m *Metrics) AgentRunningSnapshot() map[string]int64 {
	out := make(map[string]int64)
	m.agentRunning.Range(func(k, v any) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}

// RateSnapshot contains derived ratios in [0,1].
type RateSnapshot struct {
	Success float64 `json:"success"`
	Failure float64 `json:"failure"`
	Timeout float64 `json:"timeout"`
	Cancel  float64 `json:"cancel"`
}

// Snapshot is an immutable view of the counters.
type Snapshot struct {
	UptimeSeconds float64          `json:"uptime_seconds"`
	Submitted     int64            `json:"submitted"`
	Succeeded     int64            `json:"succeeded"`
	Failed        int64            `json:"failed"`
	TimedOut      int64            `json:"timed_out"`
	Cancelled     int64            `json:"cancelled"`
	Rejected      int64            `json:"rejected"`
	Running       int64            `json:"running"`
	Workers       int64            `json:"workers"`
	Rate          RateSnapshot     `json:"completion_rate"`
	AgentRunning  map[string]int64 `json:"agent_running"`
	Extras        map[string]int64 `json:"extras"`
}

// Snapshot captures the current metric values.
func (m *Metrics) Snapshot() Snapshot {
	s := Snapshot{
		UptimeSeconds: time.Since(m.start).Seconds(),
		Submitted:     m.Submitted.Load(),
		Succeeded:     m.Succeeded.Load(),
		Failed:        m.Failed.Load(),
		TimedOut:      m.TimedOut.Load(),
		Cancelled:     m.Cancelled.Load(),
		Rejected:      m.Rejected.Load(),
		Running:       m.Running.Load(),
		Workers:       m.Workers.Load(),
		AgentRunning:  m.AgentRunningSnapshot(),
		Extras: map[string]int64{
			"queue_full_rejections":      m.QueueFullReject.Load(),
			"agent_quota_rejections":     m.AgentQuotaReject.Load(),
			"panics":                     m.Panics.Load(),
			"slow_consumers":             m.SlowConsumers.Load(),
			"events_published":           m.EventsPublished.Load(),
			"agent_start_failures":       m.StartFailures.Load(),
			"agent_stream_interruptions": m.StreamInterrupted.Load(),
		},
	}
	finished := s.Succeeded + s.Failed + s.TimedOut + s.Cancelled
	if finished > 0 {
		s.Rate.Success = float64(s.Succeeded) / float64(finished)
		s.Rate.Failure = float64(s.Failed) / float64(finished)
		s.Rate.Timeout = float64(s.TimedOut) / float64(finished)
		s.Rate.Cancel = float64(s.Cancelled) / float64(finished)
	}
	return s
}
