GO ?= go
BINARY ?= bin/goagent-lite
CONFIG ?= configs/agents.yaml
BENCH_TASKS ?= 500
BENCH_WORKERS ?= 500

.PHONY: all build run mock fmt vet test race bench cover tidy clean docker

all: fmt vet test

build:
	$(GO) build -trimpath -o $(BINARY) ./cmd/server

run:
	$(GO) run ./cmd/server -config $(CONFIG)

mock:
	$(GO) run ./examples/mock-agent -addr :9101

fmt:
	gofmt -l -w .

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

bench:
	GOAGENT_BENCH=1 BENCH_TASKS=$(BENCH_TASKS) BENCH_WORKERS=$(BENCH_WORKERS) \
		$(GO) test ./tests/benchmark -run TestLoad -v

cover:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -n 1

tidy:
	$(GO) mod tidy

clean:
	rm -rf bin coverage.out

docker:
	docker build -t goagent-lite:latest .
