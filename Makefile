# Breakwater developer tasks. Go and GNU make are the only requirements;
# every target is runnable from a clean checkout.

VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: fmt vet lint test race cover build gateway mock up down clean

fmt: ## Rewrite all Go sources into canonical format.
	gofmt -w .

vet: ## Run the standard static analysis.
	go vet ./...

lint: ## Run golangci-lint (same version as CI).
	golangci-lint run ./...

race: ## Run the full suite under the race detector (the CI bar).
	go test -race -count=1 -timeout 300s ./...

test: ## Run the full suite without the race detector.
	go test -count=1 ./...

cover: ## Report per-package statement coverage.
	go test -cover ./...

cover-html: ## Write an HTML coverage detail and open it in a browser.
	go test -coverprofile=/tmp/breakwater-cover.out ./...
	go tool cover -html=/tmp/breakwater-cover.out

build: ## Build both binaries with the version injected.
	go build -ldflags "$(LDFLAGS)" -o bin/breakwater ./cmd/breakwater
	go build -ldflags "$(LDFLAGS)" -o bin/mockllm ./cmd/mockllm

gateway: ## Run the gateway from source (memory backends).
	go run ./cmd/breakwater

mock: ## Run the mock upstream from source.
	go run ./cmd/mockllm

up: ## Start the local compose stack (redis, postgres, mockllm, gateway).
	docker compose -f deploy/docker-compose.yml up -d --build

down: ## Stop and remove the local compose stack.
	docker compose -f deploy/docker-compose.yml down

clean: ## Remove local build output.
	rm -rf bin
