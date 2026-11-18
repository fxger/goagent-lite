#!/usr/bin/env bash
# Start the reference remote mock agents used by configs/agents.yaml.
set -euo pipefail
cd "$(dirname "$0")/.."

go run ./examples/mock-agent -addr :9101 &
pid1=$!
go run ./examples/mock-agent -addr :9102 -fail-rate 0.1 &
pid2=$!

trap 'kill $pid1 $pid2 2>/dev/null || true' INT TERM EXIT
wait
