#!/usr/bin/env bash
# Writes what the dev stack's mftiko-api reads into deploy/dev/data/api (gitignored), for `make dev-up`:
#
#   deployment.json   one region "dev" (the compose etcd) with controller, md1, td1 and sts1 (E13-D012)
#   api.json          user "dev", the registry from compose, td1's public key, listen :8080 (E10-D023)
#   token             the dev user's token (E10-D009); sign in to the console with it
#   cookie-key        the console cookie's HMAC key, passed as $MFTIKO_COOKIE_KEY
#   log-token         the session-log bearer the API sends to sts1 (E14-D001); -log-token
#   log-token.sha256  its SHA-256, which sts1 checks (-log-token-sha256)
#   td1-seal.key      td1's private key (mftiko-seal); accounts created in dev are sealed to it
#
# The tokens, the cookie key and the seal key are made once and kept; the JSON files and the hash are
# rewritten every time. The stack runs the controller and sts1 (E15-10). Nothing runs md1 or td1: they
# are there so the console's forms have instances to name, and a session with feeds or accounts waits
# for them. deploy/accept runs those agents (README there).
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
out=$here/data/api
mkdir -p "$out"
chmod 700 "$out"

secret() {
	[ -s "$out/$1" ] && return
	(umask 077 && openssl rand -base64 32 >"$out/$1")
}
secret token
secret cookie-key
secret log-token
tr -d '\n' <"$out/log-token" | shasum -a 256 | cut -d' ' -f1 >"$out/log-token.sha256"

seal=(go run "$repo/cmd/mftiko-seal")
[ -s "$out/td1-seal.key" ] || (cd "$repo" && "${seal[@]}" keygen -key "$out/td1-seal.key" >/dev/null)
pub=$(cd "$repo" && "${seal[@]}" pubkey -key "$out/td1-seal.key")
hash=$(tr -d '\n' <"$out/token" | shasum -a 256 | cut -d' ' -f1)

cat >"$out/deployment.json" <<EOF
{
  "regions": {"dev": {"endpoints": ["http://etcd:2379"]}},
  "instances": {"controller": "dev", "md1": "dev", "td1": "dev", "sts1": "dev"},
  "planes": {"md1": "md", "td1": "td", "sts1": "sts"}
}
EOF
(umask 077 && cat >"$out/api.json" <<EOF
{
  "users": [{"name": "dev", "token_sha256": "$hash"}],
  "allow": ["registry:5000"],
  "plain_http_hosts": ["registry:5000"],
  "td_keys": {"td1": "$pub"},
  "listen": ":8080"
}
EOF
)
echo "dev API config in deploy/dev/data/api; token: deploy/dev/data/api/token"
