// Package config loads and validates the YAML configuration. Invalid
// configuration is a fatal startup error: the gateway never runs degraded.
package config

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Gateway holds process-wide runtime settings.
type Gateway struct {
	ListenAddr             string `yaml:"listen_addr"`
	MaxConcurrency         int    `yaml:"max_concurrency"`
	QueueCapacity          int    `yaml:"queue_capacity"`
	DefaultTimeoutSeconds  int    `yaml:"default_timeout_seconds"`
	ShutdownDeadlineSecond int    `yaml:"shutdown_deadline_seconds"`
	MaxRequestBodyBytes    int64  `yaml:"max_request_body_bytes"`
	SSEHeartbeatSeconds    int    `yaml:"sse_heartbeat_seconds"`
	SSESubscriberBuffer    int    `yaml:"sse_subscriber_buffer"`
	EventRetentionMinutes  int    `yaml:"event_retention_minutes"`
	EnablePprof            bool   `yaml:"enable_pprof"`
	LogLevel               string `yaml:"log_level"`
}

// Mock holds the behaviour knobs of the in-process mock agent (local protocol).
type Mock struct {
	DurationMS      int     `yaml:"duration_ms"`
	JitterMS        int     `yaml:"jitter_ms"`
	FailRate        float64 `yaml:"fail_rate"`
	PanicRate       float64 `yaml:"panic_rate"`
	Tokens          int     `yaml:"tokens"`
	TokenIntervalMS int     `yaml:"token_interval_ms"`
	FailureCode     string  `yaml:"failure_code"`
	FailureMessage  string  `yaml:"failure_message"`
}

// Agent is one configured agent.
type Agent struct {
	ID             string `yaml:"id"`
	Version        string `yaml:"version"`
	Protocol       string `yaml:"protocol"`
	Endpoint       string `yaml:"endpoint"`
	Enabled        *bool  `yaml:"enabled"`
	MaxConcurrency int    `yaml:"max_concurrency"`
	QueueCapacity  int    `yaml:"queue_capacity"`
	TimeoutSeconds int    `yaml:"timeout_seconds"`
	Mock           Mock   `yaml:"mock"`
}

// IsEnabled reports whether the agent is enabled (defaults to true).
func (a Agent) IsEnabled() bool {
	return a.Enabled == nil || *a.Enabled
}

// Config is the root document.
type Config struct {
	Gateway Gateway `yaml:"gateway"`
	Agents  []Agent `yaml:"agents"`
}

// Defaults returns the documented default configuration.
func Defaults() Config {
	return Config{
		Gateway: Gateway{
			ListenAddr:             ":8080",
			MaxConcurrency:         500,
			QueueCapacity:          1000,
			DefaultTimeoutSeconds:  120,
			ShutdownDeadlineSecond: 30,
			MaxRequestBodyBytes:    1 << 20,
			SSEHeartbeatSeconds:    15,
			SSESubscriberBuffer:    64,
			EventRetentionMinutes:  10,
			LogLevel:               "info",
		},
	}
}

// Load reads, decodes (strictly) and validates a YAML configuration file.
func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: open %q: %w", path, err)
	}
	defer f.Close()
	return Decode(f)
}

// Decode decodes and validates configuration from r.
func Decode(r io.Reader) (Config, error) {
	cfg := Defaults()
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("config: decode: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

var nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// Validate checks every documented invariant. All violations are reported.
func (c *Config) Validate() error {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	g := &c.Gateway
	if strings.TrimSpace(g.ListenAddr) == "" {
		add("gateway.listen_addr is required")
	} else if _, _, err := net.SplitHostPort(g.ListenAddr); err != nil {
		add("gateway.listen_addr %q is not a valid host:port: %v", g.ListenAddr, err)
	}
	if g.MaxConcurrency < 1 || g.MaxConcurrency > 10000 {
		add("gateway.max_concurrency must be in [1,10000], got %d", g.MaxConcurrency)
	}
	if g.QueueCapacity < 1 {
		add("gateway.queue_capacity must be >= 1, got %d", g.QueueCapacity)
	}
	if g.DefaultTimeoutSeconds < 1 {
		add("gateway.default_timeout_seconds must be >= 1, got %d", g.DefaultTimeoutSeconds)
	}
	if g.ShutdownDeadlineSecond < 1 {
		add("gateway.shutdown_deadline_seconds must be >= 1, got %d", g.ShutdownDeadlineSecond)
	}
	if g.MaxRequestBodyBytes < 1 {
		add("gateway.max_request_body_bytes must be >= 1, got %d", g.MaxRequestBodyBytes)
	}
	if g.SSEHeartbeatSeconds < 1 {
		add("gateway.sse_heartbeat_seconds must be >= 1, got %d", g.SSEHeartbeatSeconds)
	}
	if g.SSESubscriberBuffer < 1 {
		add("gateway.sse_subscriber_buffer must be >= 1, got %d", g.SSESubscriberBuffer)
	}
	if g.EventRetentionMinutes < 1 {
		add("gateway.event_retention_minutes must be >= 1, got %d", g.EventRetentionMinutes)
	}
	switch g.LogLevel {
	case "", "debug", "info", "warn", "error":
	default:
		add("gateway.log_level must be one of debug|info|warn|error, got %q", g.LogLevel)
	}

	seen := make(map[string]struct{}, len(c.Agents))
	for i := range c.Agents {
		a := &c.Agents[i]
		label := fmt.Sprintf("agents[%d]", i)
		if !nameRe.MatchString(a.ID) {
			add("%s.id %q is invalid (must match %s)", label, a.ID, nameRe.String())
		}
		if !nameRe.MatchString(a.Version) {
			add("%s.version %q is invalid (must match %s)", label, a.Version, nameRe.String())
		}
		if a.ID != "" && a.Version != "" {
			key := a.ID + "@" + a.Version
			if _, dup := seen[key]; dup {
				add("duplicate agent %s", key)
			}
			seen[key] = struct{}{}
			label = key
		}
		switch a.Protocol {
		case "http":
			if a.Endpoint == "" {
				add("%s: endpoint is required for protocol=http", label)
			} else if u, err := url.Parse(a.Endpoint); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				add("%s: endpoint %q must be an absolute http(s) URL", label, a.Endpoint)
			}
		case "local":
			// endpoint is ignored for local agents
		default:
			add("%s: protocol must be one of http|local, got %q", label, a.Protocol)
		}
		if a.MaxConcurrency < 1 || a.MaxConcurrency > g.MaxConcurrency {
			add("%s: max_concurrency must be in [1,%d], got %d", label, g.MaxConcurrency, a.MaxConcurrency)
		}
		if a.QueueCapacity < 0 {
			add("%s: queue_capacity must be >= 1 when set, got %d", label, a.QueueCapacity)
		}
		if a.TimeoutSeconds < 1 {
			add("%s: timeout_seconds must be >= 1, got %d", label, a.TimeoutSeconds)
		}
		if a.Mock.FailRate < 0 || a.Mock.FailRate > 1 {
			add("%s: mock.fail_rate must be in [0,1]", label)
		}
		if a.Mock.PanicRate < 0 || a.Mock.PanicRate > 1 {
			add("%s: mock.panic_rate must be in [0,1]", label)
		}
		if a.Mock.DurationMS < 0 || a.Mock.JitterMS < 0 || a.Mock.Tokens < 0 || a.Mock.TokenIntervalMS < 0 {
			add("%s: mock duration/jitter/tokens/interval must be >= 0", label)
		}
	}
	if len(problems) > 0 {
		return errors.New("invalid configuration:\n  - " + strings.Join(problems, "\n  - "))
	}
	return nil
}
