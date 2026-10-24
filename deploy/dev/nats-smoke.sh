#!/bin/sh
# Runs the dev NATS permission checks against the compose server (E04-D017).
# make dev-up generates deploy/dev/data/nats and starts the server first.
set -eu
cd "$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)"
exec go run ./cmd/mftiko-nats-dev -out deploy/dev/data/nats -check
