#!/usr/bin/env bash
# Build the host release tarball (E12-D002, E12-D022, E12-D026, docs/release.md).
#
# OUT is the directory to write; the default is bin/dist. The tarball is
# mftiko-server_<version>_linux_amd64.tar.gz. SHA256SUMS in OUT covers every
# *.tar.gz already there, so a release job that built the CLI tarballs first
# gets one checksum file for both (E12-D002). make server-dist, CI and the
# release workflow run this script. It does not upload.
#
# web/build must already exist (make web). deploy/systemd is packed as
# systemd/; unit contents are E12-05.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"

out=${OUT:-bin/dist}
[[ -f web/build/index.html ]] || {
	echo "build-server: web/build/index.html missing; run make web" >&2
	exit 1
}

version=$(git describe --tags --always --dirty --match='v[0-9]*.[0-9]*.[0-9]*' 2>/dev/null || echo dev)
commit=$(git rev-parse --short HEAD)
built=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
pkg=github.com/BullionBear/mftiko
ldflags="-s -w -X ${pkg}/internal/platform/buildinfo.Version=${version} -X ${pkg}/internal/platform/buildinfo.CommitHash=${commit} -X ${pkg}/internal/platform/buildinfo.BuildTime=${built}"

# E12-D002, plus mftiko-release (E12-D022). Not the CLI, workers, or dev tools.
bins=(
	mftiko-md-agent
	mftiko-td-agent
	mftiko-sts-agent
	mftiko-shim
	mftiko-controller
	mftiko-api
	mftiko-sym
	mftiko-mdfetch
	mftiko-migrate
	mftiko-etcd-roles
	mftiko-seal
)

sha256() {
	if command -v sha256sum >/dev/null; then
		sha256sum "$@"
	else
		shasum -a 256 "$@"
	fi
}

mkdir -p "$out"
rm -f "$out"/mftiko-server_*.tar.gz
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
mkdir -p "$stage/bin" "$stage/web" "$stage/systemd"

args=()
for b in "${bins[@]}"; do
	args+=("./cmd/$b")
done
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "$ldflags" -o "$stage/bin/" "${args[@]}"
cp deploy/mftiko-release "$stage/bin/mftiko-release"
chmod 0755 "$stage/bin/mftiko-release"
cp -a web/build/. "$stage/web/"
shopt -s nullglob
# E12-D023 installs these suffixes only. The adoption test and its compose
# override live next to the units and are not part of the release.
for f in deploy/systemd/*; do
	base=$(basename "$f")
	case "$base" in
	*.service|*.socket|*.timer|*.target|*.path|*.mount|*.slice|*.service.d|*.socket.d|*.timer.d)
		cp -a "$f" "$stage/systemd/$base"
		;;
	esac
done

if [[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]]; then
	for b in "${bins[@]}"; do
		got=$("$stage/bin/$b" -version)
		want="${version} (commit "
		if [[ "$got" != "$want"* ]]; then
			echo "build-server: $b -version printed:" >&2
			printf '%s\n' "$got" >&2
			echo "build-server: want a line starting with ${version} (commit " >&2
			exit 1
		fi
	done
else
	echo "build-server: skipping -version execution on $(uname -s)/$(uname -m)" >&2
fi

name="mftiko-server_${version}_linux_amd64.tar.gz"
tar -C "$stage" -czf "$out/$name" bin web systemd

expected=("${bins[@]}" mftiko-release)
declare -A wantbin=()
for b in "${expected[@]}"; do
	wantbin[$b]=1
done
declare -A seen=()
has_web=0
while IFS= read -r p; do
	case "$p" in
	bin | bin/ | web | web/ | systemd | systemd/) continue ;;
	bin/*)
		base=${p#bin/}
		[[ "$base" == */* ]] && {
			echo "build-server: unexpected path $p" >&2
			exit 1
		}
		[[ -n "${wantbin[$base]:-}" ]] || {
			echo "build-server: unexpected binary $p" >&2
			exit 1
		}
		seen[$base]=1
		;;
	web/*)
		[[ "$p" == web/index.html ]] && has_web=1
		;;
	systemd/*) ;;
	*)
		echo "build-server: unexpected path $p" >&2
		exit 1
		;;
	esac
done < <(tar -tzf "$out/$name")
for b in "${expected[@]}"; do
	[[ -n "${seen[$b]:-}" ]] || {
		echo "build-server: tarball missing bin/$b" >&2
		exit 1
	}
done
[[ "$has_web" -eq 1 ]] || {
	echo "build-server: tarball missing web/index.html" >&2
	exit 1
}
# An empty systemd/ directory is still a member. Unit files are optional until E12-05.
if ! tar -tzf "$out/$name" | grep -q '^systemd/'; then
	echo "build-server: tarball missing systemd/" >&2
	exit 1
fi

(
	cd "$out"
	# shellcheck disable=SC2086
	sha256 *.tar.gz | LC_ALL=C sort -k2 >SHA256SUMS
)
echo "mftiko-server $version:"
tar -tzf "$out/$name"
ls -l "$out"
