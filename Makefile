.DEFAULT_GOAL := help

BINARIES := dispatcher sender webhook janitor

.PHONY: help fmt fmt-check vet lint test verify build tidy hooks

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
