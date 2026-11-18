// Package api exposes the HTTP/SSE surface. It only talks to the task manager.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strings"
	"sync"
	"time"

	"goagent-lite/internal/apierr"
	"goagent-lite/internal/event"
	"goagent-lite/internal/task"
)

// Options configures the HTTP server.
type Options struct {
	MaxRequestBodyBytes int64
	SSEHeartbeat        time.Duration
	EnablePprof         bool
	WorkersConfigured   int
}

// Server is the HTTP API server.
type Server struct {
	mgr  *task.Manager
	log  *slog.Logger
	opts Options

	shutdownCh chan struct{}
	shutdownOn sync.Once
}

// New builds the API server.
func New(mgr *task.Manager, log *slog.Logger, opts Options) *Server {
	if log == nil {
		log = slog.Default()
	}
	if opts.MaxRequestBodyBytes <= 0 {
		opts.MaxRequestBodyBytes = 1 << 20
	}
	if opts.SSEHeartbeat <= 0 {
		opts.SSEHeartbeat = 15 * time.Second
	}
	return &Server{mgr: mgr, log: log, opts: opts, shutdownCh: make(chan struct{})}
}

// BeginShutdown asks every SSE handler to end its stream. It is idempotent and
// safe to call concurrently; the channel is closed exactly once.
func (s *Server) BeginShutdown() {
	s.shutdownOn.Do(func() { close(s.shutdownCh) })
}

// Handler returns the fully wired HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/tasks", s.handleSubmit)
	mux.HandleFunc("GET /api/v1/tasks/{id}", s.handleGet)
	mux.HandleFunc("GET /api/v1/tasks/{id}/result", s.handleResult)
	mux.HandleFunc("GET /api/v1/tasks/{id}/events", s.handleEvents)
	mux.HandleFunc("POST /api/v1/tasks/{id}/cancel", s.handleCancel)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	if s.opts.EnablePprof {
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}
	return s.recoverer(mux)
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("http handler panic", "method", r.Method, "path", r.URL.Path, "panic", rec)
				writeError(w, apierr.New(apierr.CodeInternalError, "internal server error"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type submitPayload struct {
	AgentID      string            `json:"agent_id"`
	AgentVersion string            `json:"agent_version"`
	Input        json.RawMessage   `json:"input"`
	Config       *submitConfig     `json:"config"`
	Metadata     map[string]string `json:"metadata"`
}

type submitConfig struct {
	TimeoutSeconds int `json:"timeout_seconds"`
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.opts.MaxRequestBodyBytes)
	dec := json.NewDecoder(r.Body)
	var req submitPayload
	if err := dec.Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, apierr.New(apierr.CodeInvalidRequest,
				fmt.Sprintf("request body exceeds %d bytes", s.opts.MaxRequestBodyBytes)))
			return
		}
		writeError(w, apierr.New(apierr.CodeInvalidRequest, "invalid JSON body: "+err.Error()))
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeError(w, apierr.New(apierr.CodeInvalidRequest, "request body must contain exactly one JSON object"))
		return
	}
	req.AgentID = strings.TrimSpace(req.AgentID)
	req.AgentVersion = strings.TrimSpace(req.AgentVersion)
	if req.AgentID == "" {
		writeError(w, apierr.New(apierr.CodeInvalidRequest, "agent_id is required"))
		return
	}
	if req.AgentVersion == "" {
		writeError(w, apierr.New(apierr.CodeInvalidRequest, "agent_version is required"))
		return
	}
	if len(req.Input) == 0 || !json.Valid(req.Input) || !strings.HasPrefix(strings.TrimSpace(string(req.Input)), "{") {
		writeError(w, apierr.New(apierr.CodeInvalidRequest, "input must be a valid JSON object"))
		return
	}
	timeout := 0
	if req.Config != nil {
		timeout = req.Config.TimeoutSeconds
		if timeout < 0 {
			writeError(w, apierr.New(apierr.CodeInvalidRequest, "config.timeout_seconds must be > 0"))
			return
		}
	}
	for k := range req.Metadata {
		if strings.TrimSpace(k) == "" {
			writeError(w, apierr.New(apierr.CodeInvalidRequest, "metadata keys must not be empty"))
			return
		}
	}

	t, err := s.mgr.Submit(task.SubmitRequest{
		AgentID:        req.AgentID,
		AgentVersion:   req.AgentVersion,
		Input:          req.Input,
		TimeoutSeconds: timeout,
		Metadata:       req.Metadata,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"task_id": t.ID,
		"status":  t.Status,
	})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	t, err := s.mgr.Get(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, newTaskView(t, false))
}

func (s *Server) handleResult(w http.ResponseWriter, r *http.Request) {
	t, err := s.mgr.Get(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, newTaskView(t, true))
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	t, err := s.mgr.Cancel(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"task_id": t.ID,
		"status":  t.Status,
	})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	history, ch, cancel, err := s.mgr.Subscribe(id)
	if err != nil {
		writeError(w, err)
		return
	}
	defer cancel()

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, apierr.New(apierr.CodeInternalError, "streaming is not supported by this connection"))
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	writeEvent := func(e event.Event) bool {
		b, err := json.Marshal(e)
		if err != nil {
			return false
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	for _, e := range history {
		if !writeEvent(e) {
			return
		}
	}

	heartbeat := time.NewTicker(s.opts.SSEHeartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case e, open := <-ch:
			if !open {
				return
			}
			if !writeEvent(e) {
				return
			}
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		case <-s.shutdownCh:
			_, _ = io.WriteString(w, ": server shutting down\n\n")
			flusher.Flush()
			return
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	stats := s.mgr.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"accepting":      s.mgr.Accepting(),
		"uptime_seconds": stats.Metrics.UptimeSeconds,
		"queued":         stats.QueueTotal,
		"running":        stats.Metrics.Running,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	stats := s.mgr.Stats()
	m := stats.Metrics
	writeJSON(w, http.StatusOK, map[string]any{
		"uptime_seconds": m.UptimeSeconds,
		"tasks": map[string]any{
			"submitted": m.Submitted,
			"succeeded": m.Succeeded,
			"failed":    m.Failed,
			"timed_out": m.TimedOut,
			"cancelled": m.Cancelled,
			"rejected":  m.Rejected,
			"running":   m.Running,
			"queued":    stats.QueueTotal,
		},
		"rates": m.Rate,
		"queues": map[string]any{
			"total":    stats.QueueTotal,
			"capacity": stats.QueueCapacity,
			"agents":   stats.Agents,
		},
		"workers": map[string]any{
			"configured": s.opts.WorkersConfigured,
			"alive":      m.Workers,
		},
		"rejections": map[string]any{
			"total":       m.Rejected,
			"queue_full":  m.Extras["queue_full_rejections"],
			"agent_quota": m.Extras["agent_quota_rejections"],
		},
		"events": map[string]any{
			"published":      m.Extras["events_published"],
			"slow_consumers": m.Extras["slow_consumers"],
		},
		"panics":     m.Extras["panics"],
		"goroutines": runtime.NumGoroutine(),
	})
}

type taskView struct {
	TaskID              string          `json:"task_id"`
	AgentID             string          `json:"agent_id"`
	AgentVersion        string          `json:"agent_version"`
	Status              task.Status     `json:"status"`
	CreatedAt           time.Time       `json:"created_at"`
	StartedAt           *time.Time      `json:"started_at"`
	FinishedAt          *time.Time      `json:"finished_at"`
	QueueDurationMS     *float64        `json:"queue_duration_ms"`
	ExecutionDurationMS *float64        `json:"execution_duration_ms"`
	ErrorCode           *string         `json:"error_code"`
	ErrorMessage        *string         `json:"error_message"`
	Result              json.RawMessage `json:"result,omitempty"`
}

func newTaskView(t *task.Task, withResult bool) taskView {
	v := taskView{
		TaskID:       t.ID,
		AgentID:      t.AgentID,
		AgentVersion: t.AgentVersion,
		Status:       t.Status,
		CreatedAt:    t.CreatedAt,
		StartedAt:    t.StartedAt,
		FinishedAt:   t.FinishedAt,
	}
	if t.StartedAt != nil {
		ms := float64(t.QueueDuration().Microseconds()) / 1000.0
		v.QueueDurationMS = &ms
	}
	if t.StartedAt != nil && t.FinishedAt != nil {
		ms := float64(t.ExecutionDuration().Microseconds()) / 1000.0
		v.ExecutionDurationMS = &ms
	}
	if t.ErrorCode != "" {
		code := t.ErrorCode
		v.ErrorCode = &code
	}
	if t.ErrorMessage != "" {
		msg := t.ErrorMessage
		v.ErrorMessage = &msg
	}
	if withResult {
		if t.Result != nil {
			v.Result = t.Result
		} else {
			v.Result = json.RawMessage("null")
		}
	}
	return v
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	e := apierr.From(err)
	writeJSON(w, e.Status, map[string]any{
		"error": map[string]string{
			"code":    string(e.Code),
			"message": e.Message,
		},
	})
}
