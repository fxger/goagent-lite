// Command mock-agent is a reference implementation of the remote agent
// HTTP/SSE wire protocol (docs/protocol.md). It is a test double: it performs
// no real work, only configurable latency, event streaming and failure modes.
//
// Run one or more instances and point configs/agents.yaml at them:
//
//	go run ./examples/mock-agent -addr :9101 -min-duration 100ms -max-duration 2s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type options struct {
	addr          string
	minDuration   time.Duration
	maxDuration   time.Duration
	tokenCount    int
	tokenInterval time.Duration
	failRate      float64
	mode          string
	seed          int64

	// rng is guarded by mu: math/rand.Rand is not safe for concurrent use and
	// handlers run on many connections at once.
	mu  sync.Mutex
	rng *rand.Rand
}

// jitter returns a pseudo-random offset in [0, max-min). It is deterministic
// for a given -seed, which is what makes failure injection reproducible.
func (o *options) jitter() time.Duration {
	if o.maxDuration <= o.minDuration {
		return 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return time.Duration(o.rng.Int63n(int64(o.maxDuration - o.minDuration)))
}

// shouldFail reports whether this run must end with a failed terminal event.
func (o *options) shouldFail() bool {
	if o.failRate <= 0 {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.rng.Float64() < o.failRate
}

func main() {
	var o options
	flag.StringVar(&o.addr, "addr", ":9101", "listen address")
	flag.DurationVar(&o.minDuration, "min-duration", 100*time.Millisecond, "minimum mock execution time")
	flag.DurationVar(&o.maxDuration, "max-duration", 2*time.Second, "maximum mock execution time")
	flag.IntVar(&o.tokenCount, "tokens", 0, "number of token events to emit")
	flag.DurationVar(&o.tokenInterval, "token-interval", 20*time.Millisecond, "delay between token events")
	flag.Float64Var(&o.failRate, "fail-rate", 0, "probability of a failed terminal event, [0,1]")
	flag.StringVar(&o.mode, "mode", "normal", "failure injection mode: normal|http500|abort|invalid|panic|hang")
	flag.Int64Var(&o.seed, "seed", time.Now().UnixNano(), "random seed")
	flag.Parse()

	if o.minDuration < 0 || o.maxDuration < o.minDuration {
		log.Fatalf("invalid duration range: %v..%v", o.minDuration, o.maxDuration)
	}
	if o.failRate < 0 || o.failRate > 1 {
		log.Fatalf("fail-rate must be in [0,1]")
	}
	o.rng = rand.New(rand.NewSource(o.seed))

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/execute", func(w http.ResponseWriter, r *http.Request) {
		handleExecute(w, r, &o)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"status":"ok","mode":%q}`+"\n", o.mode)
	})

	srv := &http.Server{Addr: o.addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("mock-agent listening on %s (mode=%s duration=%v..%v fail_rate=%.2f)",
			o.addr, o.mode, o.minDuration, o.maxDuration, o.failRate)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	log.Println("mock-agent stopped")
}

type executeRequest struct {
	TaskID   string            `json:"task_id"`
	Input    json.RawMessage   `json:"input"`
	Config   json.RawMessage   `json:"config"`
	Metadata map[string]string `json:"metadata"`
}

type wireEvent struct {
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

func handleExecute(w http.ResponseWriter, r *http.Request, o *options) {
	var req executeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.TaskID == "" {
		http.Error(w, "task_id is required", http.StatusBadRequest)
		return
	}
	log.Printf("execute task=%s mode=%s metadata=%v", req.TaskID, o.mode, req.Metadata)

	switch o.mode {
	case "http500":
		http.Error(w, "mock agent internal error", http.StatusInternalServerError)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	send := func(ev wireEvent) bool {
		if ev.Timestamp.IsZero() {
			ev.Timestamp = time.Now().UTC()
		}
		b, err := json.Marshal(ev)
		if err != nil {
			return false
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	if o.mode == "invalid" {
		_, _ = fmt.Fprint(w, "data: {\"type\":\"started\"\n\n") // deliberately malformed
		flusher.Flush()
		return
	}

	if !send(wireEvent{Type: "started", Payload: json.RawMessage(`{}`)}) {
		return
	}
	if o.mode == "abort" {
		// Close the stream without a terminal event.
		return
	}
	if o.mode == "panic" {
		panic("mock agent simulated panic")
	}

	duration := o.minDuration + o.jitter()
	if o.mode == "hang" {
		duration = time.Hour
	}

	ctx := r.Context()
	tokens := o.tokenCount
	for i := 0; i < tokens; i++ {
		if !sleepCtx(ctx, o.tokenInterval) {
			return
		}
		payload, _ := json.Marshal(map[string]any{"text": strings.Repeat("x", 1), "index": i})
		if !send(wireEvent{Type: "token", Payload: payload}) {
			return
		}
	}
	if !sleepCtx(ctx, duration) {
		// Gateway disconnected (cancel or timeout): stop working immediately.
		return
	}
	if o.shouldFail() {
		payload, _ := json.Marshal(map[string]string{
			"error_code": "mock_agent_failure",
			"message":    "mock agent simulated a failure",
		})
		send(wireEvent{Type: "failed", Payload: payload})
		return
	}
	payload, _ := json.Marshal(map[string]any{
		"summary":     "mock agent completed",
		"task_id":     req.TaskID,
		"duration_ms": duration.Milliseconds(),
	})
	send(wireEvent{Type: "completed", Payload: payload})
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
