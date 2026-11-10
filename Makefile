.DEFAULT_GOAL := help

-include .env

SQLC          := go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0

PG_ADMIN_URL        ?= postgres://localhost:5432/postgres?sslmode=disable
BEACON_DATABASE_URL ?= postgres://localhost:5432/beacon?sslmode=disable
export BEACON_DATABASE_URL

.PHONY: help setup hooks db-setup db-up dev dev-vite dev-go seed gen fmt check test build frontend build-linux deploy clean

help: ## List targets
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

setup: hooks ## Install the pnpm packages the frontend build needs
	pnpm install

hooks: ## Run make check before every git push
	git config core.hooksPath .githooks

db-setup: ## Create the beacon database on PG_ADMIN_URL
	psql "$(PG_ADMIN_URL)" -tAc "SELECT 1 FROM pg_database WHERE datname = 'beacon'" | grep -q 1 \
		|| psql "$(PG_ADMIN_URL)" -qc "CREATE DATABASE beacon"

db-up: ## Start PostgreSQL 17 in Docker instead of a local server
	docker compose up -d --wait db

dev: ## Vite + Go with live reload; open http://localhost:7350
	@$(MAKE) -j2 dev-vite dev-go

dev-vite:
	pnpm exec vite

dev-go: export BEACON_DEV = 1
dev-go:
	go tool templ generate -watch -proxy=http://127.0.0.1:8178 -proxyport=7350 -open-browser=false -cmd="go run ./cmd/beacon serve"

seed: ## Replace the demo app in the local database with generated events
	go run ./cmd/seed

gen: ## Generate Go from the .templ and .sql files
	go tool templ generate
	$(SQLC) generate

fmt: ## Format Go, templ and TypeScript
	go tool templ fmt .
	$(GOLANGCI_LINT) fmt
	cd client/go && $(GOLANGCI_LINT) fmt
	cd client/rust && cargo fmt
	pnpm run fix

test: ## Go tests (need PostgreSQL, see BEACON_TEST_DATABASE_URL)
	go test ./...
	cd client/go && go test -race ./...
	pnpm run test:client
	cd client/rust && cargo test

check: gen ## Everything that must pass before work is done
	go tool templ fmt -fail .
	go vet ./...
	$(GOLANGCI_LINT) run
	go test ./...
	cd client/go && go vet ./... && $(GOLANGCI_LINT) run && go test -race ./...
	cd client/rust && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
	pnpm run check
	pnpm run test:client
	pnpm run build:client
	pnpm run build

frontend: ## Build the frontend into internal/web/dist for embedding
	pnpm run build

build: gen frontend ## bin/beacon for this machine
	go build -o bin/beacon ./cmd/beacon

build-linux: gen frontend ## bin/linux-amd64/beacon, static
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/linux-amd64/beacon ./cmd/beacon

deploy: build-linux ## Ship to DEPLOY_HOST (user@server) and restart
	deploy/deploy.sh "$(DEPLOY_HOST)"

clean: ## Remove build output
	rm -rf bin
	find internal/web/dist -mindepth 1 ! -name .gitkeep -delete
