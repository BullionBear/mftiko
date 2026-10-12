#!/usr/bin/env bash
# Drives the MD feed-path acceptance in deploy/accept (README.md is the runbook).
#
#   accept.sh up                 build bin/linux, NATS creds, start the stack, migrate, etcd auth, SYM
#   accept.sh agent              start an md-agent (log /work/agent-<n>.log); refuses if one runs
#   accept.sh stop-agent         SIGTERM the md-agent; its shims and conn workers stay
#   accept.sh bg NAME ARGS...    run mftiko-md-accept ARGS in the background, log /work/NAME.log
#   accept.sh stop NAME          SIGINT that background run, wait, and fail if it failed
#   accept.sh ARGS...            run mftiko-md-accept ARGS in the runner (demand, acks, late, cut, ...)
#   accept.sh ps                 the runner's processes
#   accept.sh sh                 a shell in the runner
#   accept.sh down               stop everything and drop the volumes; data/work stays
#
# ACCEPT_VENUE (default BinanceUM) is the venue SYM refreshes at up; ACCEPT_SYM_FEEDS (default
# BinanceUM/Perp,BinanceUM/Future) the tables it serves. The conn worker loads every category of
# its venue, so list them all. ACCEPT_TICKER is shown after the refresh.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
compose=(docker compose -f "$here/compose.yaml")
venue=${ACCEPT_VENUE:-BinanceUM}
feeds=${ACCEPT_SYM_FEEDS:-BinanceUM/Perp,BinanceUM/Future}
ticker=${ACCEPT_TICKER:-BinanceUM_Perp_BTCUSDT}

run() { "${compose[@]}" exec -T runner "$@"; }
tool() { run /mftiko/mftiko-md-accept "$@"; }
running() { run pgrep -f "^$1" >/dev/null 2>&1; }

up() {
	local arch=${ACCEPT_GOARCH:-$(go env GOARCH)}
	echo "building linux/$arch into bin/linux"
	(cd "$repo" && GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o bin/linux/ \
		./cmd/mftiko-md-agent ./cmd/mftiko-md-conn ./cmd/mftiko-shim ./cmd/mftiko-sym ./cmd/mftiko-md-accept)
	mkdir -p "$here/data/work"
	(cd "$repo" && go run ./cmd/mftiko-nats-dev -out "$here/data/nats" >/dev/null)
	"${compose[@]}" up -d --wait
	# mftiko-migrate is not there yet: apply the up migrations in order, once.
	if ! "${compose[@]}" exec -T postgres psql -qtA -U mftiko -d mftiko -c "select to_regclass('symbol_active')" | grep -q symbol_active; then
		"${compose[@]}" exec -T postgres sh -c \
			'for f in $(ls /migrations/*.up.sql | sort); do psql -q -v ON_ERROR_STOP=1 -U mftiko -d mftiko -f "$f" || exit 1; done'
	fi
	tool setup -instance dev -topology /work/topology.json
	tool creds -nats-dir /nats -out /work/observer.creds
	if ! running /mftiko/mftiko-sym; then
		run sh -c "nohup /mftiko/mftiko-sym -feeds '$feeds' >/work/sym.log 2>&1 &"
		sleep 2
	fi
	tool refresh -venue "$venue" -ticker "$ticker"
}

agent() {
	if running /mftiko/mftiko-md-agent; then
		echo "an md-agent is running" >&2
		exit 1
	fi
	local n
	n=$(run sh -c 'ls /work/agent-*.log 2>/dev/null | wc -l')
	n=$((n + 1))
	run sh -c "mkdir -p /var/md && nohup /mftiko/mftiko-md-agent -instance dev -topology /work/topology.json \
		-work-dir /var/md -shim /mftiko/mftiko-shim -conn /mftiko/mftiko-md-conn \
		-nats-creds /nats/creds/md-dev.creds -endpoints /endpoints/ws.json -rest-endpoints /endpoints/rest.json \
		>/work/agent-$n.log 2>&1 &"
	echo "md-agent started, log data/work/agent-$n.log"
}

case "${1:-}" in
up) up ;;
agent) agent ;;
stop-agent) run sh -c 'kill -TERM $(pgrep -f "^/mftiko/mftiko-md-agent")' && echo "SIGTERM sent to md-agent" ;;
bg)
	name=$2
	shift 2
	run sh -c "nohup /mftiko/mftiko-md-accept $* >/work/$name.log 2>&1 & echo \$! >/work/$name.pid"
	echo "started, log data/work/$name.log"
	;;
stop)
	# Wait for it to finish, then fail as it failed: watch reports gaps on its way out.
	run sh -c "pid=\$(cat /work/$2.pid) && kill -INT \$pid; while kill -0 \$pid 2>/dev/null; do sleep 0.2; done; rm /work/$2.pid"
	tail -n 3 "$here/data/work/$2.log"
	! grep -q "^mftiko-md-accept .*:" "$here/data/work/$2.log"
	;;
ps) run ps -o pid,ppid,args ;;
sh) "${compose[@]}" exec runner sh ;;
down) "${compose[@]}" down -v ;;
"" | -h | --help) sed -n '2,19p' "$0" ;;
*) tool "$@" ;;
esac
