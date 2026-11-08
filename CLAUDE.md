# CLAUDE.md

beacon is a generic telemetry server: one Go binary and PostgreSQL. Keep it
free of product names; an app is just an id in the `apps` table.

Design notes and decisions live in GitHub issues (`gh issue ...`), not in
the repo. Don't add docs files.

## Commands

- `make db-setup`: create the `beacon` database (once). `make db-up` starts Postgres in Docker instead.
- `make dev`: run the server on 127.0.0.1:8080.
- `make gen`: regenerate Go from `internal/db/queries/*.sql`. Run it after editing SQL.
- `make check`: gen, go vet, golangci-lint, go test. **Work is done only when this passes.**

## Layout

```
cmd/beacon/        serve, migrate, app create|key|list
internal/config/   BEACON_* environment
internal/db/       Connect, Migrate, migrations/, queries/, sqlc output (don't edit generated files)
internal/apps/     apps, ingest keys, Resolver (key -> app id, cached)
internal/ingest/   POST /v1/events
internal/geo/      country and city from a MaxMind format database
internal/web/      middleware: Logger, Recoverer, RealIP
internal/testdb/   per-test databases copied from a migrated template
```

## Rules

- Tests run against real Postgres via `testdb.New(t)`, never mocks. Call `t.Parallel()`.
- Never edit a shipped migration; add a new one.
- Go code calls sqlc methods and never builds SQL strings.
- Never store client IP addresses. Geo lookup happens at ingest and the address is dropped.

## Comments and prose

Keep a comment only when it says something the code does not: why, an
invariant, or an outside constraint. Delete comments that restate the code,
argue against alternatives nobody proposed, list what something doesn't do,
or recount history. An exported name gets a one or two line doc comment
starting with its name. Write plain sentences. Commits, issues and PRs too.
