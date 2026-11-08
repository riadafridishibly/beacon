.DEFAULT_GOAL := help

-include .env

SQLC          := go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0

PG_ADMIN_URL        ?= postgres://localhost:5432/postgres?sslmode=disable
BEACON_DATABASE_URL ?= postgres://localhost:5432/beacon?sslmode=disable
export BEACON_DATABASE_URL

.PHONY: help db-setup db-up dev gen fmt check test build build-linux clean

help: ## List targets
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

db-setup: ## Create the beacon database on PG_ADMIN_URL
	psql "$(PG_ADMIN_URL)" -tAc "SELECT 1 FROM pg_database WHERE datname = 'beacon'" | grep -q 1 \
		|| psql "$(PG_ADMIN_URL)" -qc "CREATE DATABASE beacon"

db-up: ## Start PostgreSQL 17 in Docker instead of a local server
	docker compose up -d --wait db

dev: ## Run the server
	go run ./cmd/beacon serve

gen: ## Generate Go from the .sql files
	$(SQLC) generate

fmt: ## Format Go
	$(GOLANGCI_LINT) fmt

test: ## Go tests (need PostgreSQL, see BEACON_TEST_DATABASE_URL)
	go test ./...

check: gen ## Everything that must pass before work is done
	go vet ./...
	$(GOLANGCI_LINT) run
	go test ./...

build: ## bin/beacon for this machine
	go build -o bin/beacon ./cmd/beacon

build-linux: ## bin/linux-amd64/beacon, static
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/linux-amd64/beacon ./cmd/beacon

clean: ## Remove build output
	rm -rf bin
