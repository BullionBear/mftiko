# mftiko — layout and targets are described in README.md.

SHELL := /bin/bash
.DEFAULT_GOAL := all

GO      ?= go
PACKAGE := github.com/BullionBear/mftiko

VERSION := $(shell git describe --tags --always --dirty --match='v[0-9]*.[0-9]*.[0-9]*' 2>/dev/null || echo "dev")
COMMIT_HASH := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
# -u: UTC, avoid builder timezone skew
BUILD_TIMESTAMP := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

LDFLAGS := -X '$(PACKAGE)/internal/platform/buildinfo.Version=$(VERSION)' \
           -X '$(PACKAGE)/internal/platform/buildinfo.CommitHash=$(COMMIT_HASH)' \
           -X '$(PACKAGE)/internal/platform/buildinfo.BuildTime=$(BUILD_TIMESTAMP)'

BIN      := bin
GEN      := contract/gen
TOOLS    := $(abspath $(BIN)/tools)
WEB      := web
NODE_BIN := $(abspath $(WEB)/node_modules/.bin)

# PostgreSQL for migrate-*. The default is the local database from deploy/dev/compose.yaml, which has
# no password (trust auth on 127.0.0.1). Every other environment sets DATABASE_URL itself.
DATABASE_URL ?= postgres://mftiko@localhost:5432/mftiko?sslmode=disable
COMPOSE      := docker compose -f deploy/dev/compose.yaml
DEV_NATS     := deploy/dev/data/nats

PROTOC_GEN_GO_VERSION := v1.36.12
# v1.72.0 is the newest buf that `go install`s with this module's Go 1.26.0.
# v1.73.0 needs Go 1.26.7, and CI sets GOTOOLCHAIN=local (E10-D043).
BUF_VERSION          := v1.72.0
OAPI_CODEGEN_VERSION := v2.8.0
OASDIFF_VERSION      := v1.34.1

# What proto-breaking compares against: a buf input. CI points it at the pull request's base.
BUF_AGAINST ?= .git\#branch=main

# What openapi-breaking compares against: a git commit-ish. CI points it at the pull request's base.
OAS_BASE ?= main

# api.v1 is final (E10-D018, E10-D116): a breaking change against OAS_BASE fails the build. Empty prints the report only.
OASDIFF_FAIL_ON ?= ERR

# buf, protoc-gen-go, oapi-codegen and oasdiff are installed into bin/tools (E10-D043, E10-D044).
# Redocly CLI and openapi-typescript come from web/node_modules.
export PATH := $(TOOLS):$(NODE_BIN):$(PATH)

.PHONY: all help tools proto proto-lint proto-breaking openapi openapi-lint openapi-breaking gen-check build cli-dist server-dist examples test test-race sdk-conformance vet fmt check \
        web-install web web-check web-dev dev-up dev-down dev-nats-smoke accept-up accept-down migrate-up migrate-status \
        migrate-new clean

all: proto openapi build examples web ## Generate code, build every binary, the example strategies and the web console

help: ## List targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-20s %s\n",$$1,$$2}'

# ---------- codegen ----------

tools: $(TOOLS)/protoc-gen-go $(TOOLS)/buf $(TOOLS)/oapi-codegen $(TOOLS)/oasdiff ## Install Go codegen tools into bin/tools

$(TOOLS)/protoc-gen-go:
	mkdir -p $(TOOLS)
	GOBIN=$(TOOLS) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

$(TOOLS)/buf:
	mkdir -p $(TOOLS)
	GOBIN=$(TOOLS) $(GO) install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

$(TOOLS)/oapi-codegen:
	mkdir -p $(TOOLS)
	GOBIN=$(TOOLS) $(GO) install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION)

$(TOOLS)/oasdiff:
	mkdir -p $(TOOLS)
	GOBIN=$(TOOLS) $(GO) install github.com/oasdiff/oasdiff@$(OASDIFF_VERSION)

proto: tools ## Regenerate contract/gen/mftiko from contract/proto/; commit the result
	rm -rf $(GEN)/mftiko
	buf generate
	$(GO) get google.golang.org/protobuf@$(PROTOC_GEN_GO_VERSION)
	$(GO) mod tidy

proto-lint: tools ## buf lint (STANDARD)
	buf lint

proto-breaking: tools ## buf breaking against main (or BUF_AGAINST): compatible changes only within a major (PROTOCOL §1)
	buf breaking --against '$(BUF_AGAINST)'

openapi-lint: web-install ## Redocly lint for contract A (E10-D036, E10-D047)
	redocly lint --config contract/openapi/redocly.yaml contract/openapi/api/v1.yaml

openapi: tools web-install ## Lint contract A and regenerate Go and TypeScript; commit the result
	$(MAKE) --no-print-directory openapi-lint
	rm -rf $(GEN)/api $(WEB)/src/lib/gen
	mkdir -p $(GEN)/api/v1 $(WEB)/src/lib/gen/api
	oapi-codegen --config contract/openapi/oapi-codegen-types.yaml contract/openapi/api/v1.yaml
	oapi-codegen --config contract/openapi/oapi-codegen-server.yaml contract/openapi/api/v1.yaml
	oapi-codegen --config contract/openapi/oapi-codegen-client.yaml contract/openapi/api/v1.yaml
	openapi-typescript contract/openapi/api/v1.yaml --output $(WEB)/src/lib/gen/api/v1.ts
	$(GO) mod tidy

# E10-D116: fails on an ERR-level breaking change (OASDIFF_FAIL_ON). Before E10-20 it only printed the report (E10-D046).
# A missing spec on the base is skipped. A base that is not a commit is an error.
openapi-breaking: tools ## oasdiff breaking against OAS_BASE; fails on a breaking change (E10-D116)
	@set -euo pipefail; \
	base='$(OAS_BASE)'; \
	spec=contract/openapi/api/v1.yaml; \
	if ! git rev-parse --verify --quiet "$${base}^{commit}" >/dev/null 2>&1; then \
	  echo "openapi-breaking: $$base is not a commit" >&2; \
	  exit 1; \
	fi; \
	if ! git cat-file -e "$$base:$$spec" 2>/dev/null; then \
	  echo "openapi-breaking: $$base has no $$spec; skipping oasdiff (E10-D046)"; \
	  exit 0; \
	fi; \
	args=(breaking --color never "$$base:$$spec" "$$spec"); \
	if [ -n '$(OASDIFF_FAIL_ON)' ]; then \
	  args+=(--fail-on '$(OASDIFF_FAIL_ON)'); \
	fi; \
	if [ -f contract/openapi/api-v1-err-ignore.txt ]; then \
	  args+=(--err-ignore contract/openapi/api-v1-err-ignore.txt); \
	fi; \
	oasdiff "$${args[@]}"

# Regenerates, then compares with what was there before: fails if contract/gen/, web/src/lib/gen/, go.mod or go.sum
# were not what make proto and make openapi produce. The fresh output stays in place, ready to commit.
gen-check: tools web-install ## Fail if the generated code (or go.mod) is stale
	@before=$$(mktemp -d) && trap 'rm -rf "$$before"' EXIT && \
	cp -R $(GEN) "$$before/gen" && cp -R $(WEB)/src/lib/gen "$$before/webgen" && cp go.mod go.sum "$$before/" && \
	$(MAKE) --no-print-directory proto openapi >/dev/null && \
	if diff -r "$$before/gen" $(GEN) >/dev/null && diff -r "$$before/webgen" $(WEB)/src/lib/gen >/dev/null && \
	   cmp -s "$$before/go.mod" go.mod && cmp -s "$$before/go.sum" go.sum; then \
	  echo "generated code is up to date"; \
	else \
	  echo "generated code is stale: run make proto and make openapi, then commit the result" >&2; \
	  diff -ru "$$before/gen" $(GEN) | head -60 >&2; \
	  diff -ru "$$before/webgen" $(WEB)/src/lib/gen | head -60 >&2; \
	  diff -u "$$before/go.mod" go.mod >&2; \
	  exit 1; \
	fi

# ---------- Go ----------

build: ## Build every command into bin/
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/ ./cmd/...

cli-dist: ## The mftiko CLI release tarballs and SHA256SUMS in bin/dist/ (E11-D035, docs/cli.md)
	bash .github/scripts/build-cli.sh

server-dist: ## Host server tarball and SHA256SUMS in bin/dist/ (E12-D002, E12-D026). Requires web/build from make web
	bash .github/scripts/build-server.sh

examples: ## Build the example strategies into bin/examples/
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/examples/ ./examples/strategies/...

test: ## go test
	$(GO) test ./...

# PKGS limits test-race. CI passes one shard; the default is the whole module.
# An empty list would make `go test` run the current directory, so refuse it.
PKGS ?= ./...

test-race: ## go test with the race detector, uncached. PKGS limits packages (default ./...)
	@test -n '$(PKGS)' || { echo "test-race: PKGS is empty" >&2; exit 1; }
	$(GO) test -race -count=1 $(PKGS)

sdk-conformance: ## Full Go SDK conformance via examples/strategies/conform (E08-D064)
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/mftiko-conformance ./cmd/mftiko-conformance
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/examples/conform ./examples/strategies/conform
	$(BIN)/mftiko-conformance -- $(BIN)/examples/conform

vet: ## go vet
	$(GO) vet ./...

fmt: ## go fmt
	$(GO) fmt ./...

# Local `make check` runs these in order. CI (.github/workflows/ci.yml) runs the
# same targets from the lint, build, and test jobs. Keep this list and those
# jobs in step: a target added here needs a step there. test-race in CI is one
# shard from .github/test-shards.txt and still passes -race -count=1.
CHECKS := proto-lint proto-breaking openapi-lint openapi-breaking gen-check vet build cli-dist test-race sdk-conformance web-check web server-dist

check: $(CHECKS) ## Lint, build, and test targets CI runs

# ---------- web ----------

web-install: $(WEB)/node_modules ## npm ci for the web console

$(WEB)/node_modules: $(WEB)/package.json $(WEB)/package-lock.json
	cd $(WEB) && npm ci
	@touch $@

web: web-install ## Build the web console into web/build/
	cd $(WEB) && npm run build

web-check: web-install ## svelte-check
	cd $(WEB) && npm run check

web-dev: web-install ## Dev server; proxies /api to mftiko-api on :8080
	cd $(WEB) && npm run dev

# ---------- database ----------

# The api service runs bin/linux/mftiko-api in a Linux container, for this machine's architecture.
dev-up: web ## Dev NATS credentials and API config, a Linux mftiko-api and the console, then start PostgreSQL, etcd, NATS, a registry and the API (console on :8080)
	$(GO) run ./cmd/mftiko-nats-dev -out $(DEV_NATS)
	deploy/dev/api-init.sh
	GOOS=linux GOARCH=$$($(GO) env GOARCH) CGO_ENABLED=0 $(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/linux/ ./cmd/mftiko-api
	$(COMPOSE) up -d --wait

dev-down: ## Stop local PostgreSQL, etcd, NATS, the registry and the API. Keeps their data, including the gitignored NATS and API directories
	$(COMPOSE) down

dev-nats-smoke: ## Run the dev NATS permission checks against the compose server
	deploy/dev/nats-smoke.sh

accept-up: ## Start the MD acceptance stack (deploy/accept/README.md): Linux binaries, etcd, NATS, PostgreSQL, SYM
	deploy/accept/accept.sh up

accept-down: ## Stop the MD acceptance stack and drop its volumes
	deploy/accept/accept.sh down

migrate-up: build ## Apply every pending migration to DATABASE_URL
	$(BIN)/mftiko-migrate -database '$(DATABASE_URL)' up

migrate-status: build ## Show applied and pending migrations
	$(BIN)/mftiko-migrate -database '$(DATABASE_URL)' status

migrate-new: ## Create the next up/down pair: make migrate-new NAME=add_cash_flows
	@test -n "$(NAME)" || { echo "usage: make migrate-new NAME=<snake_case_name>"; exit 1; }
	@last=$$(ls migrations/*.up.sql 2>/dev/null | sed -E 's#.*/([0-9]+)_.*#\1#' | sort -n | tail -1); \
	next=$$(printf '%04d' $$((10#$${last:-0} + 1))); \
	touch migrations/$${next}_$(NAME).up.sql migrations/$${next}_$(NAME).down.sql; \
	echo "created migrations/$${next}_$(NAME).{up,down}.sql"

# ---------- misc ----------

clean: ## Remove build outputs (the generated code is committed and stays)
	rm -rf $(BIN) $(WEB)/build $(WEB)/.svelte-kit
