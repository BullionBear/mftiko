#!/usr/bin/env bash
# Drives the E12-12 testnet drill (#385) on two real hosts over ssh; README.md is the record.
#
# Host A is the home region (jp): etcd, NATS, Postgres, the registry, SYM, the API, the controller and
# the md, td and sts agents. Host B is the remote region (jp2): etcd and an sts agent. Both already run
# other services, so everything here has its own port and container, and network cuts are iptables
# rules matched on the mftiko users' UIDs (E12-D062).
#
#   drill.sh infra               users, nats-server and its config, the Postgres and registry containers
#   drill.sh release TAG DIR     copy DIR's tarball and SHA256SUMS to both hosts, mftiko-release fetch, install
#   drill.sh configure DIR       /etc/mftiko/* on both hosts from the bundle DIR (drill.sh bundle makes it)
#   drill.sh bundle DIR          deployment JSON, topologies, env files and creds for both hosts, in DIR
#   drill.sh status              units, NRestarts, readyz, metrics, instances
#   drill.sh cut WHAT | uncut    network cuts: etcd-md, etcd-sts-b, nats-md, nats-sts-b, exchange-td, ctl-jp2,
#                                registry-sts
#   drill.sh a|b CMD...          run CMD on host A or B as root
#
# DRILL_A, DRILL_B are the ssh targets (root@cp, root@sys); DRILL_A_IP, DRILL_B_IP their private
# addresses, which every service binds to.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
A=${DRILL_A:-root@cp}
B=${DRILL_B:-root@sys}
A_IP=${DRILL_A_IP:-100.108.10.2}
B_IP=${DRILL_B_IP:-100.127.116.76}
NATS_PORT=4223
NATS_HTTP=8223
PG_PORT=15432
REG_PORT=5005
API_PORT=18080
NATS_VER=v2.15.0

a() { ssh -o BatchMode=yes "$A" "$@"; }
b() { ssh -o BatchMode=yes "$B" "$@"; }

# users makes the service users and mftiko-ops (E12-D005) on one host, idempotently.
users() {
	local u
	echo 'getent group mftiko-ops >/dev/null || groupadd --system mftiko-ops'
	for u in "$@"; do
		echo "id $u >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin $u"
	done
}

infra() {
	a bash -s <<EOF
set -euo pipefail
$(users nats mftiko-md mftiko-td mftiko-sts mftiko-controller mftiko-api mftiko-sym)
if ! /usr/bin/nats-server --version 2>/dev/null | grep -q ${NATS_VER#v}; then
	d=\$(mktemp -d); cd \$d
	t=nats-server-$NATS_VER-linux-amd64.tar.gz
	curl -fsSLO https://github.com/nats-io/nats-server/releases/download/$NATS_VER/\$t
	curl -fsSLO https://github.com/nats-io/nats-server/releases/download/$NATS_VER/SHA256SUMS
	grep " \$t\\\$" SHA256SUMS | sha256sum -c -
	tar xzf \$t; install -m 0755 nats-server-$NATS_VER-linux-amd64/nats-server /usr/bin/nats-server
	cd /; rm -rf \$d
fi
install -d -o nats -g nats -m 0750 /var/lib/nats /var/lib/nats/resolver
install -d -m 0755 /etc/nats
install -d -m 0700 /root/e12-12
test -s /root/e12-12/pg-password || (umask 077; openssl rand -hex 24 >/root/e12-12/pg-password)
docker volume create mftiko-drill-pgdata >/dev/null
docker volume create mftiko-drill-regdata >/dev/null
docker inspect mftiko-drill-pg >/dev/null 2>&1 || docker run -d --name mftiko-drill-pg --restart unless-stopped \
	-e POSTGRES_USER=mftiko -e POSTGRES_DB=mftiko -e POSTGRES_PASSWORD="\$(cat /root/e12-12/pg-password)" \
	-p 127.0.0.1:$PG_PORT:5432 -p $A_IP:$PG_PORT:5432 -v mftiko-drill-pgdata:/var/lib/postgresql/data postgres:16-alpine >/dev/null
docker inspect mftiko-drill-registry >/dev/null 2>&1 || docker run -d --name mftiko-drill-registry --restart unless-stopped \
	-p 127.0.0.1:$REG_PORT:5000 -p $A_IP:$REG_PORT:5000 -v mftiko-drill-regdata:/var/lib/registry registry:2 >/dev/null
for i in \$(seq 30); do docker exec mftiko-drill-pg pg_isready -U mftiko -d mftiko >/dev/null 2>&1 && break; sleep 1; done
curl -fsS http://127.0.0.1:$REG_PORT/v2/ >/dev/null
echo "host A: users, nats-server \$(/usr/bin/nats-server --version), postgres :$PG_PORT, registry :$REG_PORT"
EOF
	b bash -s <<EOF
set -euo pipefail
$(users mftiko-sts)
install -d -m 0700 /root/e12-12
echo "host B: users"
EOF
}

# release TAG DIR puts DIR's tarball where mftiko-release fetch finds it, on both hosts, and installs it.
# The only difference from a deploy is MFTIKO_RELEASE_BASE: a file server on 127.0.0.1 instead of the
# GitHub Release (no tag is published for the drill, E12-D062). fetch still checks SHA256SUMS.
release() {
	local tag=$1 dir=$2 h tgz
	tgz=mftiko-server_${tag}_linux_amd64.tar.gz
	[[ -f "$dir/$tgz" && -f "$dir/SHA256SUMS" ]] || { echo "drill.sh: no $tgz in $dir" >&2; return 1; }
	for h in "$A" "$B"; do
		ssh -o BatchMode=yes "$h" "install -d -m 0700 /root/e12-12/releases/$tag"
		scp -q "$dir/$tgz" "$dir/SHA256SUMS" "$h:/root/e12-12/releases/$tag/"
		ssh -o BatchMode=yes "$h" bash -s <<EOF
set -euo pipefail
cd /root/e12-12/releases
python3 -m http.server --bind 127.0.0.1 18765 >/dev/null 2>&1 & srv=\$!
trap 'kill \$srv' EXIT
sleep 1
# fetch with the installed tool; install with the tarball's own, so a release whose installer was
# fixed is installed by the fix (dd73a2d left its release 0700, #385).
d=\$(mktemp -d); tar -xzf $tag/$tgz -C \$d bin/mftiko-release; new=\$d/bin/mftiko-release
bin=/opt/mftiko/current/bin/mftiko-release
[[ -x \$bin ]] || bin=\$new
if [[ -d /opt/mftiko/releases/$tag ]]; then echo "\$(hostname): $tag already installed"; exit 0; fi
MFTIKO_RELEASE_BASE=http://127.0.0.1:18765 \$bin fetch $tag
\$new install
\$new list
ls -ld /opt/mftiko/releases/$tag
rm -rf \$d
EOF
	done
}

deployment() {
	cat <<EOF
{
  "regions": {
    "jp":  {"endpoints": ["http://$A_IP:2379"]},
    "jp2": {"endpoints": ["http://$B_IP:2379"]}
  },
  "instances": {"controller": "jp", "md-jp-1": "jp", "td-jp-1": "jp", "sts-jp-1": "jp", "sts-jp2-1": "jp2"},
  "planes": {"md-jp-1": "md", "td-jp-1": "td", "sts-jp-1": "sts", "sts-jp2-1": "sts"}
}
EOF
}

# configure writes every /etc/mftiko/<service>/ file (E12-D004) on both hosts. NATS is the layout
# mftiko-nats-dev wrote to NATSDIR, plus a SYM user. Secrets are made on host A and never printed:
# etcd passwords by mftiko-etcd-roles -apply, the seal key by mftiko-seal, the API token and cookie key
# here. Host B gets only sts-jp2-1's files. Running it again keeps every secret that exists.
configure() {
	local natsdir=$1 rel=/opt/mftiko/releases/$2/bin
	a "install -d -m 0700 /root/e12-12/nats && rm -rf /root/e12-12/nats/*"
	scp -q -r "$natsdir"/. "$A:/root/e12-12/nats/"
	deployment | a "cat >/root/e12-12/deployment.json"
	scp -q "$here/../accept/endpoints/ws.json" "$here/../accept/endpoints/rest.json" "$A:/root/e12-12/"
	a bash -s <<EOF
set -euo pipefail
umask 077
cd /root/e12-12
acct=\$(sed -n 's/.*"account": "\(A[A-Z0-9]*\)".*/\1/p' nats/manifest.json)
dir() { install -d -o root -g "\$2" -m 0750 /etc/mftiko/\$1; }
pub() { install -o root -g "\$3" -m 0640 "\$1" /etc/mftiko/\$2/\$(basename "\$1"); }
sec() { install -o "\$3" -g "\$3" -m 0600 "\$1" /etc/mftiko/\$2/\$4; }
pw() { cat /etc/mftiko/etcd/\$1; }

# deployment JSON and etcd users (E12-D049): -apply adds md-jp-1 and td-jp-1, keeps the rest.
dir controller mftiko-controller
cp /etc/mftiko/controller/deployment.json /root/e12-12/deployment.before.json 2>/dev/null || true
install -o root -g mftiko-controller -m 0640 deployment.json /etc/mftiko/controller/deployment.json
$rel/mftiko-etcd-roles -in deployment.json -apply -secrets /etc/mftiko/etcd | grep -v '^#.*not in the table' || true

# NATS: the generated config, on this host's private address and its own ports.
install -m 0644 nats/operator.jwt /etc/nats/operator.jwt
sed -e 's|^host: .*|host: "$A_IP"|' -e 's|^port: .*|port: $NATS_PORT|' -e 's|^http_port: .*|http: "127.0.0.1:$NATS_HTTP"|' \
	-e 's|^server_name: .*|server_name: "mftiko-drill"|' -e 's|dir: "/data/resolver"|dir: "/var/lib/nats/resolver"|' \
	nats/nats-server.conf >/etc/nats/nats-server.conf
chmod 0644 /etc/nats/nats-server.conf
install -o nats -g nats -m 0640 nats/resolver/*.jwt /var/lib/nats/resolver/
nats=nats://$A_IP:$NATS_PORT
reg=$A_IP:$REG_PORT

for s in md-agent:mftiko-md td-agent:mftiko-td sts-agent:mftiko-sts api:mftiko-api sym:mftiko-sym; do
	dir \${s%%:*} \${s#*:}
done
pub deployment.json md-agent mftiko-md && mv /etc/mftiko/md-agent/deployment.json /etc/mftiko/md-agent/topology.json
pub deployment.json td-agent mftiko-td && mv /etc/mftiko/td-agent/deployment.json /etc/mftiko/td-agent/topology.json
pub deployment.json sts-agent mftiko-sts && mv /etc/mftiko/sts-agent/deployment.json /etc/mftiko/sts-agent/topology.json
pub deployment.json api mftiko-api
pub ws.json md-agent mftiko-md
pub rest.json md-agent mftiko-md
pub rest.json td-agent mftiko-td

sec nats/creds/md-md-jp-1.creds md-agent mftiko-md nats.creds
cat >/etc/mftiko/md-agent/env <<ENV
MFTIKO_ETCD_PASSWORD=\$(pw md-jp-1)
MFTIKO_INSTANCE=md-jp-1
MFTIKO_REGISTRY=\$reg
MFTIKO_PLAIN_HTTP=true
MFTIKO_REGISTRY_CREDS=
NATS_URL=\$nats
MFTIKO_NATS_CREDS=/etc/mftiko/md-agent/nats.creds
MFTIKO_MD_ENDPOINTS=/etc/mftiko/md-agent/ws.json
MFTIKO_MD_REST_ENDPOINTS=/etc/mftiko/md-agent/rest.json
ENV

sec nats/keys/mftiko-signing.nk td-agent mftiko-td nats-signing.nk
test -s /etc/mftiko/td-agent/seal.key || $rel/mftiko-seal keygen -key /etc/mftiko/td-agent/seal.key >/dev/null
chown mftiko-td:mftiko-td /etc/mftiko/td-agent/seal.key && chmod 0600 /etc/mftiko/td-agent/seal.key
cat >/etc/mftiko/td-agent/env <<ENV
MFTIKO_ETCD_PASSWORD=\$(pw td-jp-1)
MFTIKO_INSTANCE=td-jp-1
MFTIKO_REGISTRY=\$reg
MFTIKO_PLAIN_HTTP=true
MFTIKO_REGISTRY_CREDS=
NATS_URL=\$nats
MFTIKO_NATS_ACCOUNT=\$acct
MFTIKO_TD_ENDPOINTS=
MFTIKO_TD_REST_ENDPOINTS=/etc/mftiko/td-agent/rest.json
ENV

sec nats/keys/mftiko-signing.nk sts-agent mftiko-sts nats-signing.nk
cat >/etc/mftiko/sts-agent/env <<ENV
MFTIKO_ETCD_PASSWORD=\$(pw sts-jp-1)
MFTIKO_INSTANCE=sts-jp-1
MFTIKO_REGISTRY=\$reg
MFTIKO_PLAIN_HTTP=true
MFTIKO_REGISTRY_CREDS=
MFTIKO_STS_DATA_REPOSITORY=data/drill
NATS_URL=\$nats
MFTIKO_NATS_ACCOUNT=\$acct
ENV

grep -q '^MFTIKO_ETCD_PASSWORD=' /etc/mftiko/controller/env

test -s api-token || openssl rand -hex 32 >api-token
test -s cookie-key || openssl rand -hex 32 >cookie-key
sha=\$(tr -d '\n' <api-token | sha256sum | cut -d' ' -f1)
pk=\$($rel/mftiko-seal pubkey -key /etc/mftiko/td-agent/seal.key)
cat >/root/e12-12/api.json <<JSON
{
  "users": [{"name": "drill", "token_sha256": "\$sha"}],
  "repositories": ["strategies/drill"],
  "registry": "\$reg",
  "plain_http": true,
  "td_keys": {"td-jp-1": "\$pk"},
  "retention": "168h",
  "unavailable_after": "15s",
  "listen": "$A_IP:$API_PORT"
}
JSON
install -o root -g mftiko-api -m 0640 /root/e12-12/api.json /etc/mftiko/api/api.json
dsn="postgres://mftiko:\$(cat pg-password)@127.0.0.1:$PG_PORT/mftiko?sslmode=disable"
cat >/etc/mftiko/api/env <<ENV
MFTIKO_ETCD_PASSWORD=\$(pw api)
MFTIKO_COOKIE_KEY=\$(cat cookie-key)
MFTIKO_POSTGRES_DSN=\$dsn
ENV

sec nats/creds/sym.creds sym mftiko-sym nats.creds
cat >/etc/mftiko/sym/env <<ENV
DATABASE_URL=\$dsn
NATS_URL=\$nats
MFTIKO_SYM_FEEDS=BinanceUM/Perp,BinanceUM/Future
ENV
chmod 0600 /etc/mftiko/*/env

# mftiko-release switch runs mftiko-migrate with the inherited DATABASE_URL (E12-D024).
echo "DATABASE_URL=\$dsn" >/root/e12-12/release.env

# Host B: sts-jp2-1 only. Its DATABASE_URL reaches this host's Postgres over the private network.
rm -rf b && install -d b/sts-agent
cp deployment.json b/sts-agent/topology.json
cp nats/keys/mftiko-signing.nk b/sts-agent/nats-signing.nk
cat >b/sts-agent/env <<ENV
MFTIKO_ETCD_PASSWORD=\$(pw sts-jp2-1)
MFTIKO_INSTANCE=sts-jp2-1
MFTIKO_REGISTRY=\$reg
MFTIKO_PLAIN_HTTP=true
MFTIKO_REGISTRY_CREDS=
MFTIKO_STS_DATA_REPOSITORY=data/drill
NATS_URL=\$nats
MFTIKO_NATS_ACCOUNT=\$acct
ENV
echo "DATABASE_URL=postgres://mftiko:\$(cat pg-password)@$A_IP:$PG_PORT/mftiko?sslmode=disable" >b/release.env
tar -C b -czf b.tgz . && rm -rf b
echo "host A: /etc/mftiko configured"
EOF
	scp -q -3 "$A:/root/e12-12/b.tgz" "$B:/root/e12-12/b.tgz"
	a "rm -f /root/e12-12/b.tgz"
	b bash -s <<'EOF'
set -euo pipefail
umask 077
cd /root/e12-12 && rm -rf b && install -d b && tar -xzf b.tgz -C b && rm b.tgz
install -d -o root -g mftiko-sts -m 0750 /etc/mftiko/sts-agent
install -o root -g mftiko-sts -m 0640 b/sts-agent/topology.json /etc/mftiko/sts-agent/topology.json
install -o mftiko-sts -g mftiko-sts -m 0600 b/sts-agent/nats-signing.nk /etc/mftiko/sts-agent/nats-signing.nk
install -o root -g root -m 0600 b/sts-agent/env /etc/mftiko/sts-agent/env
install -m 0600 b/release.env /root/e12-12/release.env
rm -rf b
echo "host B: /etc/mftiko/sts-agent configured"
EOF
}

# status prints what each drill step checks: units and their restarts, readiness, the mftiko gauges,
# and the shim and worker processes with their PIDs, so a step shows whether a worker survived it.
status() {
	local units='nats mftiko-sym mftiko-api mftiko-controller mftiko-md-agent mftiko-td-agent mftiko-sts-agent'
	a "for u in $units; do printf '%-18s %-9s %s restarts, main pid %s\n' \$u \$(systemctl is-active \$u) \$(systemctl show -p NRestarts --value \$u) \$(systemctl show -p MainPID --value \$u); done
		printf 'readyz %s\n' \$(curl -s -o /dev/null -w '%{http_code}' http://$A_IP:$API_PORT/readyz)
		curl -s http://$A_IP:$API_PORT/metrics | grep '^mftiko_' | grep -v ' 0\$'
		ps -eo pid,user:12,etimes,args | grep -E '[m]ftiko-shim|[r]ootfs/bin/' | cut -c1-120"
	echo "-- host B"
	b "u=mftiko-sts-agent; printf '%-18s %-9s %s restarts, main pid %s\n' \$u \$(systemctl is-active \$u) \$(systemctl show -p NRestarts --value \$u) \$(systemctl show -p MainPID --value \$u)
		ps -eo pid,user:12,etimes,args | grep -E '[m]ftiko-shim|[r]ootfs/bin/' | cut -c1-120"
}

# orders SINCE summarises both drill sessions' strategy logs from SINCE (RFC 3339 UTC) with orders.py.
orders() {
	local h
	for h in "$A" "$B"; do
		scp -q "$here/orders.py" "$h:/root/e12-12/orders.py"
		ssh -o BatchMode=yes "$h" "python3 /root/e12-12/orders.py $1 \$(ls -t /var/lib/mftiko/sts-agent/log/sts/*.stdout | head -1)"
	done
}

# Network cuts are REJECT rules in OUTPUT matched on a service's UID, so nothing else on the host is
# touched. An agent and its workers share a UID (E12-D005), so a cut covers both. Every rule carries
# the comment mftiko-drill, which uncut removes.
rule() { echo "iptables -I OUTPUT -m owner --uid-owner $1 $2 -m comment --comment mftiko-drill -j REJECT --reject-with tcp-reset"; }
cut() {
	case ${1:-} in
	etcd-md) a "$(rule mftiko-md "-p tcp -d $A_IP --dport 2379")" ;;
	etcd-sts-b) b "$(rule mftiko-sts "-p tcp -d $B_IP --dport 2379")" ;;
	nats-md) a "$(rule mftiko-md "-p tcp -d $A_IP --dport $NATS_PORT")" ;;
	nats-sts-b) b "$(rule mftiko-sts "-p tcp -d $A_IP --dport $NATS_PORT")" ;;
	exchange-td) a "$(rule mftiko-td "-p tcp ! -d 100.64.0.0/10 --dport 443")" ;;
	ctl-jp2) a "$(rule mftiko-controller "-p tcp -d $B_IP --dport 2379")" ;;
	# Docker DNATs host A's own connections to a published port before filter OUTPUT sees them: match
	# the original destination there.
	registry-sts) a "$(rule mftiko-sts "-p tcp -m conntrack --ctorigdst $A_IP --ctorigdstport $REG_PORT")" &&
		b "$(rule mftiko-sts "-p tcp -d $A_IP --dport $REG_PORT")" ;;
	*) echo "drill.sh: cut etcd-md|etcd-sts-b|nats-md|nats-sts-b|exchange-td|ctl-jp2|registry-sts" >&2 && return 1 ;;
	esac
	echo "cut $1 at $(date -u +%FT%TZ)"
}
uncut() {
	local h
	for h in "$A" "$B"; do
		ssh -o BatchMode=yes "$h" 'while n=$(iptables -L OUTPUT --line-numbers | awk "/mftiko-drill/{print \$1; exit}"); [ -n "$n" ]; do iptables -D OUTPUT "$n"; done'
	done
	echo "uncut at $(date -u +%FT%TZ)"
}

case "${1:-}" in
infra) infra ;;
status) status ;;
orders) orders "$2" ;;
cut) cut "${2:-}" ;;
uncut) uncut ;;
configure) configure "$2" "$3" ;;
release) release "$2" "$3" ;;
a) shift && a "$@" ;;
b) shift && b "$@" ;;
"" | -h | --help) sed -n '2,20p' "$0" ;;
*) echo "drill.sh: unknown command $1" >&2 && exit 1 ;;
esac
