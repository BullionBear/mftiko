# mftiko — layout and targets are described in README.md.

SHELL := /bin/bash
.DEFAULT_GOAL := all

GO       ?= go
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  := -X github.com/BullionBear/mftiko/internal/buildinfo.Version=$(VERSION)

BIN      := bin
TOOLS    := $(abspath $(BIN)/tools)
WEB      := web
NODE_BIN := $(abspath $(WEB)/node_modules/.bin)

# PostgreSQL for migrate-*. The default is the local database from deploy/dev/compose.yaml, which has
# no password (trust auth on 127.0.0.1). Every other environment sets DATABASE_URL itself.
DATABASE_URL ?= postgres://mftiko@localhost:5432/mftiko?sslmode=disable
COMPOSE      := docker compose -f deploy/dev/compose.yaml

PROTOC_GEN_GO_VERSION := v1.36.12

# buf and protoc-gen-es come from web/node_modules (npm devDependencies);
# protoc-gen-go is installed into bin/tools.
export PATH := $(TOOLS):$(NODE_BIN):$(PATH)

.PHONY: all help tools proto proto-lint proto-breaking build examples test vet fmt check \
        web-install web web-check web-dev db-up db-down migrate-up migrate-down migrate-status \
        migrate-new clean

all: proto build examples web ## Generate code, build every binary, the example strategies and the web console

help: ## List targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n",$$1,$$2}'

# ---------- codegen ----------

tools: $(TOOLS)/protoc-gen-go web-install ## Install codegen tools

$(TOOLS)/protoc-gen-go:
	GOBIN=$(TOOLS) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

proto: tools ## Generate gen/ (Go) and web/src/lib/gen/ (TypeScript) from proto/
	rm -rf gen $(WEB)/src/lib/gen
	buf generate
	$(GO) get google.golang.org/protobuf@$(PROTOC_GEN_GO_VERSION)
	$(GO) mod tidy

proto-lint: web-install ## buf lint (STANDARD)
	buf lint

proto-breaking: web-install ## buf breaking against main: compatible changes only within a major (PROTOCOL §1)
	buf breaking --against '.git#branch=main'

# ---------- Go ----------

build: ## Build every command into bin/
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN)/ ./cmd/...

examples: ## Build the example strategies into bin/examples/
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN)/examples/ ./examples/strategies/...

test: ## go test
	$(GO) test ./...

vet: ## go vet
	$(GO) vet ./...

fmt: ## go fmt
	$(GO) fmt ./...

check: proto-lint vet test web-check ## Everything CI runs

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

clean: ## Remove build outputs and generated code
	rm -rf $(BIN) gen $(WEB)/build $(WEB)/.svelte-kit $(WEB)/src/lib/gen
