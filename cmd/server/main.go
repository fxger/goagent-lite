// Command server is the goagent-lite gateway entry point.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"goagent-lite/internal/agent"
	"goagent-lite/internal/agent/adapters"
	"goagent-lite/internal/api"
	"goagent-lite/internal/config"
	"goagent-lite/internal/event"
	"goagent-lite/internal/executor"
	"goagent-lite/internal/metrics"
	"goagent-lite/internal/repository"
	"goagent-lite/internal/scheduler"
	"goagent-lite/internal/task"
	"goagent-lite/internal/worker"
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		configPath = flag.String("config", "configs/agents.yaml", "path to the YAML configuration file")
		logLevel   = flag.String("log-level", "", "override the configured log level (debug|info|warn|error)")
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		return 1
	}
	level := cfg.Gateway.LogLevel
	if *logLevel != "" {
		level = *logLevel
	}
	logger := newLogger(level)
	slog.SetDefault(logger)

	registry, err := buildRegistry(cfg)
	if err != nil {
		logger.Error("build agent registry", "err", err)
		return 1
	}
	store := repository.NewMemory()
	events := event.New(cfg.Gateway.SSESubscriberBuffer)
	met := metrics.New()
	events.OnSlowConsumer = func() { met.SlowConsumers.Add(1) }

	exec := executor.New(store, events, registry, met, executor.Options{
		DefaultTimeout: time.Duration(cfg.Gateway.DefaultTimeoutSeconds) * time.Second,
		Logger:         logger,
	})
	sched := scheduler.New(cfg.Gateway.QueueCapacity)
	mgr, err := task.NewManager(task.ManagerOptions{
		QueueCapacity:         cfg.Gateway.QueueCapacity,
		DefaultTimeoutSeconds: cfg.Gateway.DefaultTimeoutSeconds,
		EventRetention:        time.Duration(cfg.Gateway.EventRetentionMinutes) * time.Minute,
		Logger:                logger,
	}, store, registry, sched, events, met, exec)
	if err != nil {
		logger.Error("create task manager", "err", err)
		return 1
	}

	pool := worker.New(cfg.Gateway.MaxConcurrency, sched, exec, met, logger)
	pool.Start()

	apiServer := api.New(mgr, logger, api.Options{
		MaxRequestBodyBytes: cfg.Gateway.MaxRequestBodyBytes,
		SSEHeartbeat:        time.Duration(cfg.Gateway.SSEHeartbeatSeconds) * time.Second,
		EnablePprof:         cfg.Gateway.EnablePprof,
		WorkersConfigured:   cfg.Gateway.MaxConcurrency,
	})
	httpServer := &http.Server{
		Addr:              cfg.Gateway.ListenAddr,
		Handler:           apiServer.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ln, err := net.Listen("tcp", cfg.Gateway.ListenAddr)
	if err != nil {
		logger.Error("listen", "addr", cfg.Gateway.ListenAddr, "err", err)
		pool.Stop()
		pool.Wait()
		mgr.Close()
		return 1
	}

	serveErr := make(chan error, 1)
	go func() {
		err := httpServer.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()
	logger.Info("goagent-lite started",
		"addr", "http://"+ln.Addr().String(),
		"workers", cfg.Gateway.MaxConcurrency,
		"queue_capacity", cfg.Gateway.QueueCapacity,
		"agents", len(registry.All()),
	)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		logger.Info("shutdown signal received", "signal", sig.String())
	case err := <-serveErr:
		if err != nil {
			logger.Error("http server failed", "err", err)
			pool.Stop()
			pool.Wait()
			mgr.Close()
			return 1
		}
	}

	// 1. Close the submission gate and end SSE streams.
	mgr.BeginShutdown()
	apiServer.BeginShutdown()

	// 2. Cancel queued tasks, wait for running ones until the deadline.
	deadline := time.Duration(cfg.Gateway.ShutdownDeadlineSecond) * time.Second
	shutdownCtx, cancel := context.WithTimeout(context.Background(), deadline)
	mgr.Shutdown(shutdownCtx)
	cancel()

	// 3. Stop workers and wait for them to exit.
	pool.Stop()
	waited := make(chan struct{})
	go func() {
		pool.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		logger.Warn("workers did not exit within the grace period")
	}

	// 4. Drain HTTP connections (SSE handlers already stopped).
	httpCtx, httpCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer httpCancel()
	if err := httpServer.Shutdown(httpCtx); err != nil {
		logger.Warn("http shutdown", "err", err)
	}
	mgr.Close()
	logger.Info("goagent-lite stopped")
	return 0
}

func buildRegistry(cfg config.Config) (*agent.Registry, error) {
	defs := make([]agent.Definition, 0, len(cfg.Agents))
	for _, a := range cfg.Agents {
		var adapter agent.AgentAdapter
		switch a.Protocol {
		case agent.ProtocolHTTP:
			adapter = adapters.NewHTTP(adapters.HTTPOptions{Endpoint: a.Endpoint, Buffer: 64})
		case agent.ProtocolLocal:
			adapter = adapters.NewLocal(adapters.LocalOptions{
				Duration:       time.Duration(a.Mock.DurationMS) * time.Millisecond,
				Jitter:         time.Duration(a.Mock.JitterMS) * time.Millisecond,
				FailRate:       a.Mock.FailRate,
				PanicRate:      a.Mock.PanicRate,
				Tokens:         a.Mock.Tokens,
				TokenInterval:  time.Duration(a.Mock.TokenIntervalMS) * time.Millisecond,
				FailureCode:    a.Mock.FailureCode,
				FailureMessage: a.Mock.FailureMessage,
			})
		default:
			return nil, fmt.Errorf("agent %s: unsupported protocol %q", a.ID, a.Protocol)
		}
		defs = append(defs, agent.Definition{
			ID:             a.ID,
			Version:        a.Version,
			Protocol:       a.Protocol,
			Endpoint:       a.Endpoint,
			Enabled:        a.IsEnabled(),
			MaxConcurrency: a.MaxConcurrency,
			QueueCapacity:  a.QueueCapacity,
			TimeoutSeconds: a.TimeoutSeconds,
			Adapter:        adapter,
		})
	}
	return agent.NewRegistry(defs)
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
