#!/bin/sh
# Checks the dev stack's strategy log alerts end to end (E16-04, DECISIONS E16-D032–E16-D035): R1, R2 and
# R5 once each and MftikoAgentDown once, all through the one Alertmanager to the webhook receiver, and the
# three inhibited while sts1 is stopped. make dev-up and make migrate-up first; make dev-alert-smoke
# builds bin/mftiko and bin/mftiko-pack. A run takes about two minutes, and up to six more when
# Alertmanager has not yet resolved an earlier stop of sts1.
set -eu
cd "$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)"
exec go run ./cmd/mftiko-alert-smoke "$@"
