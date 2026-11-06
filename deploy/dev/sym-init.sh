#!/usr/bin/env bash
# Writes the dev SYM's gRPC token file into deploy/dev/data/sym (gitignored), for `make dev-up`:
#
#   token        the bearer a client sends: authorization: Bearer $(cat deploy/dev/data/sym/token)
#   grpc-tokens  "dev <token>", the -grpc-token-file of the sym service (E22-D007)
#
# The token is made once and kept; grpc-tokens is rewritten every time. The directory is 0755 and the
# files 0644: the container reads them as root, and nothing here is a production secret.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
out=$here/data/sym
mkdir -p "$out"

[ -s "$out/token" ] || openssl rand -hex 32 >"$out/token"
printf '# name token (E22-D007). Written by deploy/dev/sym-init.sh.\ndev %s\n' "$(tr -d '\n' <"$out/token")" >"$out/grpc-tokens"
echo "dev SYM gRPC on 127.0.0.1:7450; token: deploy/dev/data/sym/token"
