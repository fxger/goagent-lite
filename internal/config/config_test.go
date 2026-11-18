package config

import (
	"strings"
	"testing"
)

func TestDefaultsAreValid(t *testing.T) {
	cfg := Defaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults invalid: %v", err)
	}
	if cfg.Gateway.ListenAddr != ":8080" || cfg.Gateway.MaxConcurrency != 500 ||
		cfg.Gateway.QueueCapacity != 1000 || cfg.Gateway.DefaultTimeoutSeconds != 120 ||
		cfg.Gateway.ShutdownDeadlineSecond != 30 || cfg.Gateway.MaxRequestBodyBytes != 1048576 ||
		cfg.Gateway.SSEHeartbeatSeconds != 15 || cfg.Gateway.SSESubscriberBuffer != 64 ||
		cfg.Gateway.EventRetentionMinutes != 10 {
		t.Fatalf("unexpected defaults: %+v", cfg.Gateway)
	}
}

func TestDecodeAppliesDefaults(t *testing.T) {
	cfg, err := Decode(strings.NewReader("gateway:\n  listen_addr: \":9000\"\nagents: []\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Gateway.ListenAddr != ":9000" || cfg.Gateway.MaxConcurrency != 500 {
		t.Fatalf("defaults not applied: %+v", cfg.Gateway)
	}
}

func TestDecodeValidAgents(t *testing.T) {
	cfg, err := Decode(strings.NewReader(`
gateway:
  listen_addr: ":8080"
  max_concurrency: 64
agents:
  - id: echo
    version: v1
    protocol: local
    max_concurrency: 8
    timeout_seconds: 10
    mock:
      duration_ms: 100
      tokens: 3
  - id: remote
    version: "1.0"
    protocol: http
    endpoint: http://127.0.0.1:9100
    enabled: false
    max_concurrency: 4
    queue_capacity: 10
    timeout_seconds: 30
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Agents) != 2 {
		t.Fatalf("agents = %d", len(cfg.Agents))
	}
	if cfg.Agents[0].IsEnabled() != true {
		t.Fatal("enabled must default to true")
	}
	if cfg.Agents[1].IsEnabled() != false {
		t.Fatal("explicit enabled=false not honoured")
	}
	if cfg.Agents[0].Mock.DurationMS != 100 || cfg.Agents[0].Mock.Tokens != 3 {
		t.Fatalf("mock = %+v", cfg.Agents[0].Mock)
	}
}

func TestDecodeRejectsUnknownFields(t *testing.T) {
	_, err := Decode(strings.NewReader("gateway:\n  listen_addr: \":8080\"\n  nope: 1\n"))
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("want unknown field error, got %v", err)
	}
}

func TestValidateReportsAllProblems(t *testing.T) {
	cfg := Config{
		Gateway: Gateway{
			ListenAddr:             "not-an-address",
			MaxConcurrency:         0,
			QueueCapacity:          0,
			DefaultTimeoutSeconds:  0,
			ShutdownDeadlineSecond: 0,
			MaxRequestBodyBytes:    0,
			SSEHeartbeatSeconds:    0,
			SSESubscriberBuffer:    0,
			EventRetentionMinutes:  0,
			LogLevel:               "verbose",
		},
		Agents: []Agent{
			{ID: "a", Version: "v1", Protocol: "http", MaxConcurrency: 1, TimeoutSeconds: 0},
			{ID: "a", Version: "v1", Protocol: "ftp", MaxConcurrency: 2, TimeoutSeconds: 1},
			{ID: "bad id", Version: "v1", Protocol: "local", MaxConcurrency: 99, TimeoutSeconds: 1},
			{ID: "b", Version: "v1", Protocol: "local", MaxConcurrency: 1, TimeoutSeconds: 1, QueueCapacity: -1,
				Mock: Mock{FailRate: 2, PanicRate: -1, DurationMS: -1}},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	msg := err.Error()
	for _, want := range []string{
		"listen_addr", "max_concurrency", "queue_capacity", "default_timeout_seconds",
		"shutdown_deadline_seconds", "max_request_body_bytes", "sse_heartbeat_seconds",
		"sse_subscriber_buffer", "event_retention_minutes", "log_level",
		"endpoint is required", "protocol must be one of", "duplicate agent a@v1",
		"invalid", "queue_capacity must be >= 1", "fail_rate", "panic_rate",
		"duration/jitter/tokens/interval",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("validation message missing %q:\n%s", want, msg)
		}
	}
}

func TestValidateAgentConcurrencyBound(t *testing.T) {
	cfg := Defaults()
	cfg.Agents = []Agent{{ID: "a", Version: "v1", Protocol: "local", MaxConcurrency: 501, TimeoutSeconds: 1}}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "[1,500]") {
		t.Fatalf("want bound error, got %v", err)
	}
}
