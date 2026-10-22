#!/usr/bin/env bash
# Integration test: starts cmd/mq (in-memory broker) and cmd/pointsd, then runs
# cmd/publisher, which publishes a scenario of events and asserts the balances
# and the dead-letter topic. The exit code is the test result.
#
# pointsd logs go to logs/pointsd.log: `tail -f logs/pointsd.log` to follow.
set -euo pipefail
cd "$(dirname "$0")/.."

MQ_ADDR=${MQ_ADDR:-localhost:8085}
POINTSD_ADDR=${POINTSD_ADDR:-localhost:8080}
export PUBSUB_EMULATOR_HOST=$MQ_ADDR PUBSUB_PROJECT_ID=pointsd-local
export POINTSD_LOG_FILE=logs/pointsd.log

bin=$(mktemp -d)
mkdir -p logs && : > "$POINTSD_LOG_FILE"
pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; rm -rf "$bin"; }
trap cleanup EXIT

go build -o "$bin" ./cmd/mq ./cmd/pointsd ./cmd/publisher

wait_for() { # url
	for _ in $(seq 50); do curl -sf -o /dev/null "$1" 2>/dev/null && return 0; sleep 0.1; done
	echo "timed out waiting for $1" >&2; return 1
}

"$bin/mq" -addr "$MQ_ADDR" & pids+=($!)
for _ in $(seq 50); do (exec 3<>"/dev/tcp/${MQ_ADDR%:*}/${MQ_ADDR#*:}") 2>/dev/null && break; sleep 0.1; done
"$bin/publisher" -setup

"$bin/pointsd" -addr "$POINTSD_ADDR" 2>/dev/null & pids+=($!)
wait_for "http://$POINTSD_ADDR/healthz"

"$bin/publisher" -pointsd "http://$POINTSD_ADDR"
