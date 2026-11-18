#!/usr/bin/env bash
# Run the end-to-end load test. Override tasks / workers with env vars:
#   BENCH_TASKS=1000 BENCH_WORKERS=500 ./scripts/loadtest.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export GOAGENT_BENCH=1
export BENCH_TASKS="${BENCH_TASKS:-500}"
export BENCH_WORKERS="${BENCH_WORKERS:-500}"
go test ./tests/benchmark -run TestLoad -v
