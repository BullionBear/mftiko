#!/usr/bin/env bash
# Drives the MD, TD and end-to-end acceptance in deploy/accept (README.md is the runbook).
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
# TD (E6-12..E6-14), on the same stack:
#
#   accept.sh td-up              the TD instance's etcd user and topology, and its seal key
#   accept.sh td-agent           start a td-agent (log /work/td-agent-<n>.log); refuses if one runs
#   accept.sh stop-td-agent      SIGTERM the td-agent; its shims and account workers stay
#   accept.sh td ARGS...         run mftiko-td-accept ARGS in the runner (account, demand, submit, cut, ...)
#   accept.sh td-bg NAME ARGS... run mftiko-td-accept ARGS in the background, log /work/NAME.log
#
# E10-19, end to end through mftiko-api, on the same stack (logs in data/work/e2e/):
#
#   accept.sh e2e-up             hello in the registry, etcd users for both regions, the controller,
#                                sts1, sts2, the MD and TD agents, and API replicas a (:8080, published
#                                on 127.0.0.1:18080) and b (:8081); web/build is the console
#   accept.sh e2e ARGS...        run mftiko-api-accept ARGS in the runner (main, ticker, invalid, ...;
#                                cli main|faults|region STEP drives the mftiko CLI instead, E11-09)
#   accept.sh e2e-all            every flow, with the controller and etcd2 stopped and started around
#                                absent and region
#   accept.sh console-smoke      the Playwright smoke (web/e2e) against replica a, from this host
#   accept.sh controller | stop-controller
#   accept.sh sts-agent NAME | stop-sts-agent NAME
#   accept.sh api a|b | stop-api a|b
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
td_inst=${ACCEPT_TD_INSTANCE:-td1}

run() { "${compose[@]}" exec -T runner "$@"; }
tool() { run /mftiko/mftiko-md-accept "$@"; }
tdtool() { run /mftiko/mftiko-td-accept "$1" -instance "$td_inst" "${@:2}"; }
running() { run pgrep -f "^$1" >/dev/null 2>&1; }
e2etool() { run /mftiko/mftiko-api-accept "$@"; }

up() {
	local arch=${ACCEPT_GOARCH:-$(go env GOARCH)}
	echo "building linux/$arch into bin/linux"
	(cd "$repo" && GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o bin/linux/ \
		./cmd/mftiko-md-agent ./cmd/mftiko-md-conn ./cmd/mftiko-shim ./cmd/mftiko-sym ./cmd/mftiko-md-accept \
		./cmd/mftiko-td-agent ./cmd/mftiko-td-account ./cmd/mftiko-td-accept ./cmd/mftiko-seal \
		./cmd/mftiko-controller ./cmd/mftiko-sts-agent ./cmd/mftiko-api ./cmd/mftiko-api-accept ./cmd/mftiko-pack ./cmd/mftiko &&
		GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o bin/linux/examples/ ./examples/strategies/hello)
	mkdir -p "$here/data/work"
	(cd "$repo" && go run ./cmd/mftiko-nats-dev -out "$here/data/nats" >/dev/null)
	"${compose[@]}" up -d --wait
	# mftiko-migrate is not implemented yet: apply each up migration once, in order, recorded in
	# accept_migrations. A file that fails with "already exists" was applied before that table was
	# kept (a stack from E4 or E6): it is recorded and skipped.
	"${compose[@]}" exec -T postgres sh -c '
		q() { psql -qtA -v ON_ERROR_STOP=1 -U mftiko -d mftiko "$@"; }
		q -c "create table if not exists accept_migrations (name text primary key)"
		for f in $(ls /migrations/*.up.sql | sort); do
			n=$(basename "$f")
			[ -z "$(q -c "select 1 from accept_migrations where name = '"'"'$n'"'"'")" ] || continue
			if ! out=$(q --single-transaction -f "$f" 2>&1); then
				echo "$out" | grep -q "already exists" || { echo "$n: $out" >&2; exit 1; }
				echo "$n: applied before, recorded"
			else
				echo "$n: applied"
			fi
			q -c "insert into accept_migrations values ('"'"'$n'"'"')"
		done'
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

td_up() {
	tdtool setup -topology /work/td-topology.json
	run sh -c 'test -f /work/td-seal.key || /mftiko/mftiko-seal keygen -key /work/td-seal.key'
}

td_agent() {
	if running /mftiko/mftiko-td-agent; then
		echo "a td-agent is running" >&2
		exit 1
	fi
	local n account
	n=$(run sh -c 'ls /work/td-agent-*.log 2>/dev/null | wc -l')
	n=$((n + 1))
	account=$(sed -n 's/.*"account": "\(A[A-Z0-9]*\)".*/\1/p' "$here/data/nats/manifest.json")
	# -rest-endpoints is keyed by account kind; the venue names in rest.json are the kinds' too. The
	# private stream and the trade websocket follow from the testnet REST host. OKX has no testnet
	# host: -simulated is its demo trading (the x-simulated-trading header and the demo private
	# socket). The other venues ignore it.
	run sh -c "mkdir -p /var/td && nohup /mftiko/mftiko-td-agent -instance $td_inst -topology /work/td-topology.json \
		-work-dir /var/td -shim /mftiko/mftiko-shim -account /mftiko/mftiko-td-account \
		-nats-account $account -nats-signing-key /nats/keys/mftiko-signing.nk -seal-key /work/td-seal.key \
		-rest-endpoints /endpoints/rest.json -simulated >/work/td-agent-$n.log 2>&1 &"
	echo "td-agent started, log data/work/td-agent-$n.log"
}

# nth NAME prints the next free log number of NAME under /work/e2e.
nth() { echo $(($(run sh -c "ls /work/e2e/$1-*.log 2>/dev/null | wc -l") + 1)); }

controller() {
	if running /mftiko/mftiko-controller; then
		echo "a controller is running" >&2
		return 1
	fi
	local n
	n=$(nth controller)
	run sh -c "nohup /mftiko/mftiko-controller -in /work/e2e/deployment.json >/work/e2e/controller-$n.log 2>&1 &"
	echo "controller started, log data/work/e2e/controller-$n.log"
}

sts_agent() {
	local inst=$1 n account
	if running "/mftiko/mftiko-sts-agent -instance $inst "; then
		echo "an sts-agent $inst is running" >&2
		return 1
	fi
	n=$(nth "$inst")
	account=$(sed -n 's/.*"account": "\(A[A-Z0-9]*\)".*/\1/p' "$here/data/nats/manifest.json")
	run sh -c "mkdir -p /var/sts/$inst && nohup /mftiko/mftiko-sts-agent -instance $inst -topology /work/e2e/deployment.json \
		-work-dir /var/sts/$inst -data /var/sts/$inst/data -shim /mftiko/mftiko-shim -registry registry:5000 -plain-http \
		-nats-account $account -nats-signing-key /nats/keys/mftiko-signing.nk >/work/e2e/$inst-$n.log 2>&1 &"
	echo "sts-agent $inst started, log data/work/e2e/$inst-$n.log"
}

api_port() { case $1 in a) echo 8080 ;; b) echo 8081 ;; *) echo "api a|b" >&2 && return 1 ;; esac; }

api() {
	local r=$1 port n
	port=$(api_port "$r")
	if running "/mftiko/mftiko-api -in /work/e2e/deployment.json -config /work/e2e/api-$r.json"; then
		echo "API replica $r is running" >&2
		return 1
	fi
	n=$(nth "api-$r")
	run sh -c "MFTIKO_COOKIE_KEY=\$(cat /work/e2e/cookie-key) MFTIKO_POSTGRES_DSN=\$DATABASE_URL nohup /mftiko/mftiko-api \
		-in /work/e2e/deployment.json -config /work/e2e/api-$r.json -web /web/build >/work/e2e/api-$r-$n.log 2>&1 &"
	run sh -c "i=0; until wget -q -O /dev/null http://127.0.0.1:$port/readyz 2>/dev/null; do i=\$((i+1)); [ \$i -lt 60 ] || exit 1; sleep 0.5; done"
	echo "API replica $r ready on :$port, log data/work/e2e/api-$r-$n.log"
}

stop_api() {
	run sh -c "kill -TERM \$(pgrep -f '^/mftiko/mftiko-api -in /work/e2e/deployment.json -config /work/e2e/api-$1.json')"
	echo "SIGTERM sent to API replica $1"
}

e2e_up() {
	up
	(cd "$repo" && make web)
	run sh -c 'mkdir -p /work/e2e && /mftiko/mftiko-pack -bin /mftiko/examples/hello -name hello \
		-repo registry:5000/strategies/hello -tag accept -plain-http'
	td_up
	e2etool setup
	running /mftiko/mftiko-md-agent || agent
	running /mftiko/mftiko-td-agent || td_agent
	running /mftiko/mftiko-controller || controller
	running "/mftiko/mftiko-sts-agent -instance sts1 " || sts_agent sts1
	running "/mftiko/mftiko-sts-agent -instance sts2 " || sts_agent sts2
	# make web replaced web/build, which a running replica still holds open: restart them.
	for r in a b; do
		if running "/mftiko/mftiko-api -in /work/e2e/deployment.json -config /work/e2e/api-$r.json"; then
			stop_api "$r"
			run sh -c "while pgrep -f '^/mftiko/mftiko-api -in /work/e2e/deployment.json -config /work/e2e/api-$r.json' >/dev/null; do sleep 0.2; done"
		fi
		api "$r"
	done
	echo "console: http://127.0.0.1:${ACCEPT_API_PORT:-18080}/  token: data/work/e2e/token"
}

# e2e_all runs every flow in order and stops at the first that fails. The controller and etcd2 are
# stopped and started here, between the steps of absent and region.
e2e_all() {
	e2etool main
	e2etool ticker
	e2etool invalid
	e2etool replicas
	e2etool cli main
	e2etool cli faults
	run sh -c 'kill -TERM $(pgrep -f "^/mftiko/mftiko-controller")'
	echo "SIGTERM sent to the controller"
	e2etool absent create
	controller
	e2etool absent resume
	e2etool region before
	e2etool cli region before
	"${compose[@]}" stop etcd2
	e2etool region down
	e2etool cli region down
	"${compose[@]}" start etcd2
	# If the outage outlasted sts2's election session, it has exited (E13-D015) and systemd would
	# restart it; here, start it again.
	running "/mftiko/mftiko-sts-agent -instance sts2 " || sts_agent sts2
	e2etool region up
	e2etool cli region up
	echo "e2e: every flow passed"
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
	! grep -Eq "^mftiko-(md|td)-accept .*:" "$here/data/work/$2.log"
	;;
td-up) td_up ;;
td-agent) td_agent ;;
stop-td-agent) run sh -c 'kill -TERM $(pgrep -f "^/mftiko/mftiko-td-agent")' && echo "SIGTERM sent to td-agent" ;;
td)
	shift
	tdtool "$@"
	;;
td-bg)
	name=$2
	shift 2
	run sh -c "nohup /mftiko/mftiko-td-accept $1 -instance $td_inst ${*:2} >/work/$name.log 2>&1 & echo \$! >/work/$name.pid"
	echo "started, log data/work/$name.log"
	;;
e2e-up) e2e_up ;;
e2e)
	shift
	e2etool "$@"
	;;
e2e-all) e2e_all ;;
console-smoke)
	shift
	(cd "$repo/web" && npx playwright test "$@")
	;;
controller) controller ;;
stop-controller) run sh -c 'kill -TERM $(pgrep -f "^/mftiko/mftiko-controller")' && echo "SIGTERM sent to the controller" ;;
sts-agent) sts_agent "$2" ;;
stop-sts-agent) run sh -c "kill -TERM \$(pgrep -f '^/mftiko/mftiko-sts-agent -instance $2 ')" && echo "SIGTERM sent to sts-agent $2" ;;
api) api "$2" ;;
stop-api) stop_api "$2" ;;
ps) run ps -o pid,ppid,args ;;
sh) "${compose[@]}" exec runner sh ;;
down) "${compose[@]}" down -v ;;
"" | -h | --help) sed -n '2,42p' "$0" ;;
*) tool "$@" ;;
esac
