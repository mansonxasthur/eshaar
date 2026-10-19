.DEFAULT_GOAL := help

BINARIES := dispatcher sender webhook janitor migrate loadtest

.PHONY: help fmt fmt-check vet lint test verify build tidy hooks up down migrate stack-up stack-down loadtest loadtest-report

help: ## List available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "%-12s %s\n", $$1, $$2}'

fmt: ## Format all Go code
	gofmt -w .
	golangci-lint fmt ./...

fmt-check: ## Fail if any file is not formatted
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	@golangci-lint fmt --diff ./... >/dev/null

vet: ## Run go vet
	go vet ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

test: ## Run all tests with the race detector
	go test -race -count=1 ./...

verify: fmt-check vet lint test ## Everything that must pass before a commit

build: ## Build all entry points into bin/
	@for b in $(BINARIES); do \
		echo "build $$b"; \
		CGO_ENABLED=0 go build -o bin/$$b ./cmd/$$b || exit 1; \
	done

tidy: ## Tidy go.mod
	go mod tidy

hooks: ## Install the versioned git hooks
	git config core.hooksPath .githooks

COMPOSE_DOWN_FLAGS ?=
COMPOSE := docker compose -f infra/docker-compose.yml
LOCAL_DATABASE_URL ?= postgres://eshaar:eshaar@localhost:5432/eshaar?sslmode=disable

up: ## Start local Postgres and Kafka and create the topic
	$(COMPOSE) up -d --wait postgres kafka
	$(COMPOSE) run --rm kafka-init

down: ## Stop the local environment (add -v via COMPOSE_DOWN_FLAGS to drop data)
	$(COMPOSE) down $(COMPOSE_DOWN_FLAGS)

migrate: ## Apply migrations to the local Postgres
	DATABASE_URL='$(LOCAL_DATABASE_URL)' go run ./cmd/migrate

stack-up: ## Build and start Postgres, Kafka and the five Eshaar processes
	$(COMPOSE) up -d --build --wait postgres kafka
	$(COMPOSE) run --rm kafka-init
	$(COMPOSE) --profile stack up -d --build dispatcher sender webhook janitor
	@for i in $$(seq 1 30); do curl -sf http://localhost:8080/healthz >/dev/null && exit 0; sleep 1; done; \
		echo "dispatcher did not answer /healthz"; exit 1

stack-down: ## Stop the full stack (add -v via COMPOSE_DOWN_FLAGS to drop data)
	$(COMPOSE) --profile stack down $(COMPOSE_DOWN_FLAGS)

LOADTEST_ARGS ?=
loadtest: ## Run the load test against the stack (LOADTEST_ARGS="-rates 500,1000 -step 20s")
	LOADTEST_API_KEYS=$${LOADTEST_API_KEYS:-loadtest-1,loadtest-2,loadtest-3,loadtest-4} go run ./cmd/loadtest $(LOADTEST_ARGS)

loadtest-report: ## What the sender did with a run's notifications (RUN=<run id>)
	@test -n "$(RUN)" || { echo "usage: make loadtest-report RUN=<run id>"; exit 1; }
	$(COMPOSE) exec -T postgres psql -U eshaar -d eshaar -v run='$(RUN)' < infra/loadtest-report.sql
