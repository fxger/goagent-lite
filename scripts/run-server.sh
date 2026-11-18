#!/usr/bin/env bash
# Build and run the gateway against the example config.
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/goagent-lite ./cmd/server
exec ./bin/goagent-lite -config "${CONFIG:-configs/agents.yaml}"
