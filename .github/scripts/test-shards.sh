#!/usr/bin/env bash
# CI test shards (.github/test-shards.txt).
#
#   matrix              JSON array of shard names, in file order
#   packages <shard>    package paths for that shard, one per line
#   check               the file is a partition of `go list ./...`
#   self-test           check, plus the rest-shard cases
#
# The shard named rest has no packages. Its set is every package in this
# module that the other lines do not name, so a new package cannot be skipped.
# A package path is one relative directory (./deploy), not a ./... pattern.
# SHARD_FILE overrides the table (absolute, or relative to the repo root).
set -euo pipefail

export LC_ALL=C

script=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
file=${SHARD_FILE:-.github/test-shards.txt}

NAMES=()
PKGS=()
SEEN_PKGS=

usage() {
	echo "usage: test-shards.sh matrix|packages <shard>|check|self-test" >&2
	exit 2
}

trim() {
	local s=$1
	s=${s#"${s%%[![:space:]]*}"}
	s=${s%"${s##*[![:space:]]}"}
	printf '%s' "$s"
}

load_table() {
	NAMES=()
	PKGS=()
	SEEN_PKGS=$'\n'
	if [[ ! -f "$file" ]]; then
		echo "missing shard file: $file" >&2
		exit 1
	fi
	local line no=0 name rest p i
	local -a plist
	while IFS= read -r line || [[ -n "$line" ]]; do
		no=$((no + 1))
		line=${line%$'\r'}
		line=$(trim "$line")
		[[ -z "$line" || "$line" == \#* ]] && continue
		name=${line%%[[:space:]]*}
		rest=${line#"$name"}
		rest=$(trim "$rest")
		if [[ ! "$name" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]]; then
			echo "$file:$no: bad shard name: $name" >&2
			exit 1
		fi
		for i in "${!NAMES[@]}"; do
			if [[ "${NAMES[$i]}" == "$name" ]]; then
				echo "$file:$no: duplicate shard: $name" >&2
				exit 1
			fi
		done
		if [[ "$name" == rest ]]; then
			if [[ -n "$rest" ]]; then
				echo "$file:$no: rest takes no packages; it is every package not listed on another line" >&2
				exit 1
			fi
			NAMES+=("$name")
			PKGS+=("")
			continue
		fi
		if [[ -z "$rest" ]]; then
			echo "$file:$no: shard $name needs at least one package" >&2
			exit 1
		fi
		read -r -a plist <<<"$rest"
		for p in "${plist[@]}"; do
			if [[ "$p" != ./* || "$p" == *..* ]]; then
				echo "$file:$no: package must be a relative path without ..: $p" >&2
				exit 1
			fi
			if [[ ! "$p" =~ ^\./[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$ ]]; then
				echo "$file:$no: package must be one package path, not a pattern: $p" >&2
				exit 1
			fi
			if [[ "$SEEN_PKGS" == *$'\n'"$p"$'\n'* ]]; then
				echo "$file:$no: duplicate package: $p" >&2
				exit 1
			fi
			SEEN_PKGS+="$p"$'\n'
		done
		NAMES+=("$name")
		PKGS+=("${plist[*]}")
	done <"$file"
	local nrest=0
	for name in "${NAMES[@]}"; do
		if [[ "$name" == rest ]]; then
			nrest=$((nrest + 1))
		fi
	done
	if [[ "$nrest" -ne 1 ]]; then
		echo "$file: want exactly one rest shard, found $nrest" >&2
		exit 1
	fi
}

to_rel() {
	local imp=$1 mod=$2
	if [[ "$imp" == "$mod" ]]; then
		printf '.\n'
		return 0
	fi
	local prefix="$mod/"
	if [[ "$imp" == "$prefix"* ]]; then
		printf './%s\n' "${imp#"$prefix"}"
		return 0
	fi
	echo "package is outside module $mod: $imp" >&2
	return 1
}

to_imp() {
	local rel=$1 mod=$2
	if [[ "$rel" == . ]]; then
		printf '%s\n' "$mod"
		return 0
	fi
	if [[ "$rel" == ./* ]]; then
		printf '%s/%s\n' "$mod" "${rel#./}"
		return 0
	fi
	echo "bad relative path: $rel" >&2
	return 1
}

listed_args() {
	LISTED_ARGS=()
	local i
	local -a plist
	for i in "${!NAMES[@]}"; do
		[[ "${NAMES[$i]}" == rest ]] && continue
		read -r -a plist <<<"${PKGS[$i]}"
		LISTED_ARGS+=("${plist[@]}")
	done
}

# Sets MOD, ALL_IMPORTS, LISTED_IMPORTS, REST_IMPORTS. LISTED_IMPORTS and
# ALL_IMPORTS are unsorted go list output; REST_IMPORTS is sorted.
compute_partition() {
	load_table
	listed_args
	MOD=$(go list -m)
	ALL_IMPORTS=$(go list ./...)
	if [[ -z "$ALL_IMPORTS" ]]; then
		echo "go list ./... returned no packages" >&2
		exit 1
	fi
	if [[ ${#LISTED_ARGS[@]} -eq 0 ]]; then
		LISTED_IMPORTS=""
	else
		local errf
		errf=$(mktemp)
		if ! LISTED_IMPORTS=$(go list "${LISTED_ARGS[@]}" 2>"$errf"); then
			echo "go list failed for listed packages:" >&2
			printf '  %s\n' "${LISTED_ARGS[@]}" >&2
			cat "$errf" >&2
			rm -f "$errf"
			exit 1
		fi
		rm -f "$errf"
	fi
	local n_args=${#LISTED_ARGS[@]} n_lines=0 n_unique=0
	if [[ -n "$LISTED_IMPORTS" ]]; then
		n_lines=$(printf '%s\n' "$LISTED_IMPORTS" | wc -l)
		n_lines=${n_lines//[[:space:]]/}
		n_unique=$(printf '%s\n' "$LISTED_IMPORTS" | sort -u | wc -l)
		n_unique=${n_unique//[[:space:]]/}
	fi
	if [[ "$n_lines" -ne "$n_args" || "$n_unique" -ne "$n_args" ]]; then
		echo "a listed path did not resolve to exactly one module package" >&2
		printf '%s\n' "$LISTED_IMPORTS" >&2
		exit 1
	fi
	local all_sorted listed_sorted
	all_sorted=$(printf '%s\n' "$ALL_IMPORTS" | sort -u)
	if [[ -z "$LISTED_IMPORTS" ]]; then
		listed_sorted=""
		REST_IMPORTS=$all_sorted
	else
		listed_sorted=$(printf '%s\n' "$LISTED_IMPORTS" | sort -u)
		local missing
		missing=$(comm -23 <(printf '%s\n' "$listed_sorted") <(printf '%s\n' "$all_sorted"))
		if [[ -n "$missing" ]]; then
			echo "listed package is not in this module:" >&2
			printf '%s\n' "$missing" >&2
			exit 1
		fi
		REST_IMPORTS=$(comm -23 <(printf '%s\n' "$all_sorted") <(printf '%s\n' "$listed_sorted"))
	fi
}

print_rest() {
	compute_partition
	if [[ -z "$REST_IMPORTS" ]]; then
		return 0
	fi
	local imp rel
	local -a rel_lines=()
	while IFS= read -r imp; do
		[[ -z "$imp" ]] && continue
		rel=$(to_rel "$imp" "$MOD") || exit 1
		rel_lines+=("$rel")
	done <<<"$REST_IMPORTS"
	printf '%s\n' "${rel_lines[@]}" | sort
}

packages_cmd() {
	local shard=$1
	load_table
	local i
	for i in "${!NAMES[@]}"; do
		if [[ "${NAMES[$i]}" == "$shard" ]]; then
			if [[ "$shard" == rest ]]; then
				print_rest
			else
				local -a plist
				read -r -a plist <<<"${PKGS[$i]}"
				printf '%s\n' "${plist[@]}"
			fi
			return 0
		fi
	done
	echo "unknown shard: $shard" >&2
	exit 1
}

matrix_cmd() {
	load_table
	local out="[" sep="" name
	for name in "${NAMES[@]}"; do
		out+="${sep}\"${name}\""
		sep=,
	done
	out+="]"
	printf '%s\n' "$out"
}

count_lines() {
	if [[ -z "${1:-}" ]]; then
		printf '0\n'
		return 0
	fi
	local n
	n=$(printf '%s\n' "$1" | wc -l)
	printf '%s\n' "${n//[[:space:]]/}"
}

check_cmd() {
	compute_partition
	local n_all n_listed n_rest
	n_all=$(count_lines "$ALL_IMPORTS")
	n_listed=$(count_lines "$LISTED_IMPORTS")
	n_rest=$(count_lines "$REST_IMPORTS")
	local sum=$((n_listed + n_rest))
	if [[ "$sum" -ne "$n_all" ]]; then
		echo "shard partition is $n_listed + $n_rest, go list ./... is $n_all" >&2
		exit 1
	fi
	echo "test shards: ${#NAMES[@]} shards, $n_listed listed, $n_rest rest, $n_all packages"
}

# What `packages` prints, converted back to import paths, is exactly `go list ./...`.
verify_printed_partition() {
	local mod all_sorted json shard rels rel imp
	local -a got=() shard_names=()
	mod=$(go list -m)
	all_sorted=$(go list ./... | sort -u)
	json=$("$script" matrix)
	json=${json#\[}
	json=${json%\]}
	json=${json//\"/}
	IFS=',' read -r -a shard_names <<<"$json"
	for shard in "${shard_names[@]}"; do
		rels=$("$script" packages "$shard")
		while IFS= read -r rel; do
			[[ -z "$rel" ]] && continue
			imp=$(to_imp "$rel" "$mod") || exit 1
			got+=("$imp")
		done <<<"$rels"
	done
	local got_sorted n_all n_got
	got_sorted=$(printf '%s\n' "${got[@]}" | sort -u)
	if [[ "$all_sorted" != "$got_sorted" ]]; then
		echo "self-test: printed packages are not exactly go list ./..." >&2
		diff -u <(printf '%s\n' "$all_sorted") <(printf '%s\n' "$got_sorted") >&2 || true
		exit 1
	fi
	n_all=$(count_lines "$all_sorted")
	n_got=${#got[@]}
	if [[ "$n_got" -ne "$n_all" ]]; then
		echo "self-test: $n_got package lines, $n_all packages" >&2
		exit 1
	fi
}

expect_matrix_fail() {
	local label=$1 content=$2 needle=$3
	printf '%s\n' "$content" >"$tmp"
	local status=0 err
	err=$(SHARD_FILE="$tmp" "$script" matrix 2>&1) || status=$?
	if [[ "$status" -eq 0 ]]; then
		echo "self-test: $label should have failed" >&2
		exit 1
	fi
	if ! printf '%s\n' "$err" | grep -F "$needle" >/dev/null; then
		echo "self-test: $label failed without [$needle]:" >&2
		printf '%s\n' "$err" >&2
		exit 1
	fi
}

self_test() {
	"$script" check
	verify_printed_partition

	local mod parent child other p q
	local -a all=()
	mod=$(go list -m)
	mapfile -t all < <(go list ./...)
	if [[ ${#all[@]} -lt 2 ]]; then
		echo "self-test: need at least two packages" >&2
		exit 1
	fi
	parent=""
	child=""
	for p in "${all[@]}"; do
		for q in "${all[@]}"; do
			if [[ "$q" == "$p/"* ]]; then
				parent=$p
				child=$q
				break 2
			fi
		done
	done
	if [[ -z "$parent" ]]; then
		echo "self-test: no package is a parent of another" >&2
		exit 1
	fi
	other=""
	for p in "${all[@]}"; do
		if [[ "$p" != "$parent" && "$p" != "$parent/"* ]]; then
			other=$p
			break
		fi
	done
	if [[ -z "$other" ]]; then
		echo "self-test: no package outside $parent" >&2
		exit 1
	fi
	local prel crel orel
	prel=$(to_rel "$parent" "$mod")
	crel=$(to_rel "$child" "$mod")
	orel=$(to_rel "$other" "$mod")

	tmp=$(mktemp)
	trap 'rm -f "$tmp"' RETURN

	cat >"$tmp" <<EOF
# comment

parent $prel

rest
EOF
	local out named
	out=$(SHARD_FILE="$tmp" "$script" packages rest)
	if ! printf '%s\n' "$out" | grep -Fx "$crel" >/dev/null; then
		echo "self-test: child $crel was dropped from rest" >&2
		exit 1
	fi
	if ! printf '%s\n' "$out" | grep -Fx "$orel" >/dev/null; then
		echo "self-test: unlisted $orel was dropped from rest" >&2
		exit 1
	fi
	if printf '%s\n' "$out" | grep -Fx "$prel" >/dev/null; then
		echo "self-test: listed $prel leaked into rest" >&2
		exit 1
	fi
	named=$(SHARD_FILE="$tmp" "$script" packages parent)
	if [[ "$named" != "$prel" ]]; then
		echo "self-test: parent shard printed [$named], want $prel" >&2
		exit 1
	fi
	local mj
	mj=$(SHARD_FILE="$tmp" "$script" matrix)
	if [[ "$mj" != '["parent","rest"]' ]]; then
		echo "self-test: matrix printed $mj" >&2
		exit 1
	fi

	# rest does not have to be the last line.
	cat >"$tmp" <<EOF
rest
parent $prel
EOF
	SHARD_FILE="$tmp" "$script" check >/dev/null

	cat >"$tmp" <<EOF
pair $prel $orel
rest
EOF
	out=$(SHARD_FILE="$tmp" "$script" packages rest)
	if printf '%s\n' "$out" | grep -Fx "$prel" >/dev/null || printf '%s\n' "$out" | grep -Fx "$orel" >/dev/null; then
		echo "self-test: a package on a multi-package line leaked into rest" >&2
		exit 1
	fi
	if ! printf '%s\n' "$out" | grep -Fx "$crel" >/dev/null; then
		echo "self-test: child of a multi-package line was dropped from rest" >&2
		exit 1
	fi
	named=$(SHARD_FILE="$tmp" "$script" packages pair)
	if [[ "$named" != "$prel"$'\n'"$orel" ]]; then
		echo "self-test: pair shard printed:" >&2
		printf '%s\n' "$named" >&2
		exit 1
	fi

	expect_matrix_fail "missing rest" "only $prel" "exactly one rest"
	expect_matrix_fail "two rests" $'rest\nrest' "duplicate shard"
	expect_matrix_fail "duplicate package" $'a '"$prel"$'\nb '"$prel"$'\nrest' "duplicate package"
	expect_matrix_fail "parent path" $'a ./internal/../deploy\nrest' "without .."
	expect_matrix_fail "pattern" $'a ./deploy/*\nrest' "not a pattern"
	expect_matrix_fail "rest packages" "rest $prel" "rest takes no packages"
	expect_matrix_fail "shard name" $'foo/bar '"$prel"$'\nrest' "bad shard name"
	expect_matrix_fail "bare path" $'a internal/api\nrest' "relative path"

	printf '%s\n' "bad ./no/such/pkg" "rest" >"$tmp"
	mj=$(SHARD_FILE="$tmp" "$script" matrix)
	if [[ "$mj" != '["bad","rest"]' ]]; then
		echo "self-test: unknown package should still be a valid matrix, got $mj" >&2
		exit 1
	fi
	local status=0 err
	err=$(SHARD_FILE="$tmp" "$script" check 2>&1) || status=$?
	if [[ "$status" -eq 0 ]]; then
		echo "self-test: unknown package passed check" >&2
		exit 1
	fi
	if ! printf '%s\n' "$err" | grep -F './no/such/pkg' >/dev/null; then
		echo "self-test: unknown package failed without its path:" >&2
		printf '%s\n' "$err" >&2
		exit 1
	fi

	status=0
	err=$(SHARD_FILE="$tmp" "$script" packages no-such 2>&1) || status=$?
	if [[ "$status" -eq 0 ]] || ! printf '%s\n' "$err" | grep -F 'unknown shard' >/dev/null; then
		echo "self-test: unknown shard printed:" >&2
		printf '%s\n' "$err" >&2
		exit 1
	fi

	echo "test-shards self-test ok"
}

cmd=${1:-}
case "$cmd" in
matrix) matrix_cmd ;;
packages)
	[[ $# -eq 2 ]] || usage
	packages_cmd "$2"
	;;
check) check_cmd ;;
self-test) self_test ;;
*) usage ;;
esac
