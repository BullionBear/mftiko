# mftiko — layout and targets are described in README.md.

SHELL := /bin/bash
.DEFAULT_GOAL := all

GO      ?= go
PACKAGE := github.com/BullionBear/mftiko

VERSION := $(shell git describe --tags --always --dirty --match='v[0-9]*.[0-9]*.[0-9]*' 2>/dev/null || echo "dev")
COMMIT_HASH := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
# -u: UTC, avoid builder timezone skew
BUILD_TIMESTAMP := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

LDFLAGS := -X '$(PACKAGE)/internal/buildinfo.Version=$(VERSION)' \
           -X '$(PACKAGE)/internal/buildinfo.CommitHash=$(COMMIT_HASH)' \
           -X '$(PACKAGE)/internal/buildinfo.BuildTime=$(BUILD_TIMESTAMP)'

BIN      := bin
TOOLS    := $(abspath $(BIN)/tools)
WEB      := web
NODE_BIN := $(abspath $(WEB)/node_modules/.bin)

# PostgreSQL for migrate-*. The default is the local database from deploy/dev/compose.yaml, which has
# no password (trust auth on 127.0.0.1). Every other environment sets DATABASE_URL itself.
DATABASE_URL ?= postgres://mftiko@localhost:5432/mftiko?sslmode=disable
COMPOSE      := docker compose -f deploy/dev/compose.yaml

PROTOC_GEN_GO_VERSION := v1.36.12

# What proto-breaking compares against: a buf input. CI points it at the pull request's base.
BUF_AGAINST ?= .git\#branch=main

# buf and protoc-gen-es come from web/node_modules (npm devDependencies);
# protoc-gen-go is installed into bin/tools.
export PATH := $(TOOLS):$(NODE_BIN):$(PATH)

.PHONY: all help tools proto proto-lint proto-breaking gen-check build examples test test-race vet fmt check \
        web-install web web-check web-dev db-up db-down migrate-up migrate-down migrate-status \
        migrate-new clean

all: proto build examples web ## Generate code, build every binary, the example strategies and the web console

help: ## List targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n",$$1,$$2}'

# ---------- codegen ----------

tools: $(TOOLS)/protoc-gen-go web-install ## Install codegen tools

$(TOOLS)/protoc-gen-go:
	GOBIN=$(TOOLS) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

proto: tools ## Regenerate gen/ (Go) and web/src/lib/gen/ (TypeScript) from proto/; commit the result
	rm -rf gen $(WEB)/src/lib/gen
	buf generate
	$(GO) get google.golang.org/protobuf@$(PROTOC_GEN_GO_VERSION)
	$(GO) mod tidy

proto-lint: web-install ## buf lint (STANDARD)
	buf lint

proto-breaking: web-install ## buf breaking against main (or BUF_AGAINST): compatible changes only within a major (PROTOCOL §1)
	buf breaking --against '$(BUF_AGAINST)'

# Regenerates, then compares with what was there before: fails if gen/, web/src/lib/gen/, go.mod or go.sum were not
# what make proto makes from proto/. The fresh output stays in place, ready to commit.
gen-check: tools ## Fail if the generated code (or go.mod) is stale
	@before=$$(mktemp -d) && trap 'rm -rf "$$before"' EXIT && \
	cp -R gen "$$before/gen" && cp -R $(WEB)/src/lib/gen "$$before/webgen" && cp go.mod go.sum "$$before/" && \
	$(MAKE) --no-print-directory proto >/dev/null && \
	if diff -r "$$before/gen" gen >/dev/null && diff -r "$$before/webgen" $(WEB)/src/lib/gen >/dev/null && \
	   cmp -s "$$before/go.mod" go.mod && cmp -s "$$before/go.sum" go.sum; then \
	  echo "generated code is up to date"; \
	else \
	  echo "generated code is stale: run make proto and commit the result" >&2; \
	  diff -ru "$$before/gen" gen | head -60 >&2; \
	  diff -ru "$$before/webgen" $(WEB)/src/lib/gen | head -60 >&2; \
	  diff -u "$$before/go.mod" go.mod >&2; \
	  exit 1; \
	fi

# ---------- Go ----------

build: ## Build every command into bin/
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/ ./cmd/...

examples: ## Build the example strategies into bin/examples/
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/examples/ ./examples/strategies/...

test: ## go test
	$(GO) test ./...

test-race: ## go test with the race detector, uncached
	$(GO) test -race -count=1 ./...

vet: ## go vet
	$(GO) vet ./...

fmt: ## go fmt
	$(GO) fmt ./...

# CI runs these one step each (.github/workflows/ci.yml), in this order: keep the two lists the same.
CHECKS := proto-lint proto-breaking gen-check vet build test-race web-check

check: $(CHECKS) ## Everything CI runs

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

db-up: ## Start a local PostgreSQL (deploy/dev/compose.yaml)
	$(COMPOSE) up -d postgres

db-down: ## Stop the local PostgreSQL (keeps its volume)
	$(COMPOSE) down

migrate-up: build ## Apply every pending migration to DATABASE_URL
	$(BIN)/mftiko-migrate -database '$(DATABASE_URL)' up

migrate-down: build ## Roll back the last migration
	$(BIN)/mftiko-migrate -database '$(DATABASE_URL)' down 1

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
