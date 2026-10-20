#!/usr/bin/env bash
# Pack linux/amd64 conn and account workers and push them (D985–D990, docs/release.md).
#
# REGISTRY is the prefix: 127.0.0.1:5000 or ghcr.io/bullionbear.
# TAGS is a space-separated list. Every tag of one binary must return the same digest.
# PLAIN_HTTP=1 speaks HTTP (the registry:2 service). Otherwise HTTPS.
#
# A ghcr.io prefix uses GITHUB_ACTOR and GITHUB_TOKEN to push and to pull back.
# This script only pushes. It does not set package visibility.
set -euo pipefail

if [[ -z "${REGISTRY:-}" ]]; then
  echo "REGISTRY is empty" >&2
  exit 1
fi
REGISTRY=${REGISTRY%/}
if [[ -z "${TAGS//[[:space:]]/}" ]]; then
  echo "TAGS is empty" >&2
  exit 1
fi
read -r -a tags <<<"$TAGS"

plain=0
if [[ "${PLAIN_HTTP:-}" == 1 ]]; then
  plain=1
fi
host=${REGISTRY%%/*}

root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

wait_registry() {
  local scheme=https
  if [[ "$plain" == 1 ]]; then
    scheme=http
  fi
  local url="${scheme}://${host}/v2/"
  local deadline=$((SECONDS + 60))
  local code
  while true; do
    code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "$url" || true)
    case "$code" in
      200|401) return 0 ;;
    esac
    if ((SECONDS >= deadline)); then
      printf 'registry %s not ready (last HTTP %s)\n' "$url" "$code" >&2
      exit 1
    fi
    sleep 1
  done
}

# A static binary is "not a dynamic executable" (ldd exits non-zero) or "statically linked".
require_static() {
  local bin=$1
  local out
  if out=$(ldd "$bin" 2>&1); then
    if [[ "$out" == *"statically linked"* ]]; then
      return 0
    fi
    printf '%s is dynamically linked\n%s\n' "$bin" "$out" >&2
    exit 1
  fi
  if [[ "$out" == *"not a dynamic executable"* || "$out" == *"statically linked"* ]]; then
    return 0
  fi
  printf '%s is dynamically linked\n%s\n' "$bin" "$out" >&2
  exit 1
}

if [[ "$host" == "ghcr.io" ]]; then
  if [[ -z "${GITHUB_TOKEN:-}" || -z "${GITHUB_ACTOR:-}" ]]; then
    echo "ghcr.io push needs GITHUB_TOKEN and GITHUB_ACTOR" >&2
    exit 1
  fi
  mkdir -p "$tmp/docker"
  chmod 700 "$tmp/docker"
  auth=$(printf '%s:%s' "$GITHUB_ACTOR" "$GITHUB_TOKEN" | base64 -w 0)
  printf '{"auths":{"ghcr.io":{"auth":"%s"}}}\n' "$auth" > "$tmp/docker/config.json"
  unset auth
  chmod 600 "$tmp/docker/config.json"
  export DOCKER_CONFIG=$tmp/docker
fi

version=$(git describe --tags --always --dirty --match='v[0-9]*.[0-9]*.[0-9]*' 2>/dev/null || echo dev)
commit=$(git rev-parse --short HEAD)
built=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
pkg=github.com/BullionBear/mftiko
ldflags="-X ${pkg}/internal/platform/buildinfo.Version=${version} -X ${pkg}/internal/platform/buildinfo.CommitHash=${commit} -X ${pkg}/internal/platform/buildinfo.BuildTime=${built}"

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$ldflags" -o "$tmp/mftiko-md-conn" ./cmd/mftiko-md-conn
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$ldflags" -o "$tmp/mftiko-td-account" ./cmd/mftiko-td-account
require_static "$tmp/mftiko-md-conn"
require_static "$tmp/mftiko-td-account"
go build -ldflags "$ldflags" -o "$tmp/mftiko-pack" ./cmd/mftiko-pack

wait_registry

pack() {
  local bin=$1 kind=$2 name=$3
  local repo="${REGISTRY}/${name}"
  local -a args=(-bin "$bin" -kind "$kind" -name "$name" -repo "$repo")
  if [[ "$plain" == 1 ]]; then
    args+=(-plain-http)
  fi
  local digest="" tag out got
  for tag in "${tags[@]}"; do
    out=$("$tmp/mftiko-pack" "${args[@]}" -tag "$tag")
    got=${out//[[:space:]]/}
    if [[ ! "$got" =~ ^sha256:[0-9a-f]{64}$ ]]; then
      printf 'pack %s tag %s printed %q\n' "$name" "$tag" "$out" >&2
      exit 1
    fi
    if [[ -z "$digest" ]]; then
      digest=$got
    elif [[ "$got" != "$digest" ]]; then
      printf 'pack %s tag %s digest %s, want %s\n' "$name" "$tag" "$got" "$digest" >&2
      exit 1
    fi
  done
  printf '%s %s\n' "$name" "$digest"
  python3 - "$REGISTRY" "$plain" "$name" "$kind" "$digest" "${tags[@]}" <<'PY'
import gzip, hashlib, io, json, os, sys, tarfile, urllib.error, urllib.request

registry, plain, name, kind, digest, *tags = sys.argv[1:]
host = registry.split("/", 1)[0]
scheme = "http" if plain == "1" else "https"
if "/" in registry:
    api_repo = registry.split("/", 1)[1] + "/" + name
else:
    api_repo = name
base = f"{scheme}://{host}/v2/{api_repo}"
accept = "application/vnd.oci.image.manifest.v1+json"
want = {
    "md.conn": ["worker.v1", "mdctl.v1"],
    "td.account": ["worker.v1", "tdctl.v1"],
}[kind]

token = ""
if host == "ghcr.io":
    user = os.environ["GITHUB_ACTOR"]
    password = os.environ["GITHUB_TOKEN"]
    mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    mgr.add_password(None, "https://ghcr.io/", user, password)
    opener = urllib.request.build_opener(urllib.request.HTTPBasicAuthHandler(mgr))
    scope = f"repository:{api_repo}:pull"
    url = f"https://ghcr.io/token?service=ghcr.io&scope={scope}"
    try:
        with opener.open(url, timeout=30) as resp:
            data = json.load(resp)
    except urllib.error.HTTPError as err:
        sys.exit(f"registry token HTTP {err.code}")
    token = data.get("token") or data.get("access_token") or ""
    if not token:
        sys.exit("registry token response has no token")

def get(url, manifest):
    req = urllib.request.Request(url)
    if manifest:
        req.add_header("Accept", accept)
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read()
            headers = {k.lower(): v for k, v in resp.headers.items()}
            return headers, body
    except urllib.error.HTTPError as err:
        sys.exit(f"GET {url} HTTP {err.code}")

def check_manifest(ref, expect_digest):
    headers, body = get(f"{base}/manifests/{ref}", True)
    got = "sha256:" + hashlib.sha256(body).hexdigest()
    if got != expect_digest:
        sys.exit(f"{ref} body digest {got}, want {expect_digest}")
    header = headers.get("docker-content-digest", "").strip().strip('"')
    if header != expect_digest:
        sys.exit(f"{ref} Docker-Content-Digest {header!r}, want {expect_digest}")
    doc = json.loads(body)
    if doc.get("mediaType") != accept or "manifests" in doc or doc.get("schemaVersion") != 2:
        sys.exit(f"{ref} is not a single image manifest")
    if "annotations" in doc:
        sys.exit(f"{ref} carries annotations")
    layers = doc.get("layers") or []
    if len(layers) != 1 or layers[0].get("mediaType") != "application/vnd.oci.image.layer.v1.tar+gzip":
        sys.exit(f"{ref} layers {layers!r}")
    cfg = doc.get("config") or {}
    if cfg.get("mediaType") != "application/vnd.mftiko.worker.config.v1+json":
        sys.exit(f"{ref} config media type {cfg.get('mediaType')!r}")
    return doc

def blob(desc):
    headers, body = get(f"{base}/blobs/{desc['digest']}", False)
    got = "sha256:" + hashlib.sha256(body).hexdigest()
    if got != desc["digest"] or desc.get("size") != len(body):
        sys.exit(f"blob {desc['digest']} is {got} size {len(body)}")
    return body

manifest = check_manifest(digest, digest)
for tag in tags:
    check_manifest(tag, digest)
config = json.loads(blob(manifest["config"]))
if config.get("schema") != "mftiko.worker.v1" or config.get("kind") != kind or config.get("name") != name:
    sys.exit(f"config identity {config.get('schema')!r} {config.get('kind')!r} {config.get('name')!r}")
if config.get("runtime") != "go" or config.get("os") != "linux" or config.get("arch") != "amd64":
    sys.exit(f"config platform {config.get('runtime')!r} {config.get('os')!r} {config.get('arch')!r}")
if config.get("entrypoint") != ["bin/worker"]:
    sys.exit(f"entrypoint {config.get('entrypoint')!r}")
if config.get("contracts") != want:
    sys.exit(f"contracts {config.get('contracts')!r}")
if config.get("sdk") != {"lang": "", "version": ""}:
    sys.exit(f"sdk {config.get('sdk')!r}")
layer = blob(manifest["layers"][0])
with tarfile.open(fileobj=io.BytesIO(gzip.decompress(layer)), mode="r:") as tf:
    members = tf.getmembers()
if [m.name for m in members] != ["bin/worker"] or not members[0].isfile():
    sys.exit("layer is not a single regular file bin/worker")
PY
}

pack "$tmp/mftiko-md-conn" md.conn mftiko-md-conn
pack "$tmp/mftiko-td-account" td.account mftiko-td-account
