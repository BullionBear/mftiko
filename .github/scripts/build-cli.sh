#!/usr/bin/env bash
# Build the mftiko CLI for each release platform into OUT (E11-D035, E15-D009, docs/release.md).
#
# OUT is the directory to write; the default is bin/dist. It gets one
# mftiko_<version>_<os>_<arch>.tar.gz per platform, each holding the binaries
# mftiko and mftiko-pack, and SHA256SUMS over the tarballs. make cli-dist, CI and the release
# workflow all run this script.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"

out=${OUT:-bin/dist}
platforms=(darwin/arm64 darwin/amd64 linux/amd64 linux/arm64)

version=$(git describe --tags --always --dirty --match='v[0-9]*.[0-9]*.[0-9]*' 2>/dev/null || echo dev)
commit=$(git rev-parse --short HEAD)
built=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
pkg=github.com/BullionBear/mftiko
ldflags="-s -w -X ${pkg}/internal/platform/buildinfo.Version=${version} -X ${pkg}/internal/platform/buildinfo.CommitHash=${commit} -X ${pkg}/internal/platform/buildinfo.BuildTime=${built}"

sha256() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$@"
  else
    shasum -a 256 "$@"
  fi
}

rm -rf "$out"
mkdir -p "$out"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

host="$(go env GOOS)/$(go env GOARCH)"
for p in "${platforms[@]}"; do
  os=${p%/*}
  arch=${p#*/}
  dir="$tmp/$os-$arch"
  mkdir -p "$dir"
  CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags "$ldflags" -o "$dir/mftiko" ./cmd/mftiko
  CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags "$ldflags" -o "$dir/mftiko-pack" ./cmd/mftiko-pack
  # The binaries this machine can run must report the version they were stamped with.
  if [[ "$p" == "$host" ]]; then
    first=$("$dir/mftiko" version | head -1)
    if [[ "$first" != "mftiko ${version} "* ]]; then
      echo "$p: mftiko version printed \"$first\", want version $version" >&2
      exit 1
    fi
    pack=$("$dir/mftiko-pack" -version)
    if [[ "$pack" != "${version} "* ]]; then
      echo "$p: mftiko-pack -version printed \"$pack\", want version $version" >&2
      exit 1
    fi
  fi
  tar -C "$dir" -czf "$out/mftiko_${version}_${os}_${arch}.tar.gz" mftiko mftiko-pack
done

(cd "$out" && sha256 mftiko_*.tar.gz > SHA256SUMS)
echo "mftiko $version:"
ls -l "$out"
