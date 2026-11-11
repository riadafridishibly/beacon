# CLAUDE.md

beacon is a generic telemetry server: one Go binary and PostgreSQL. Keep it
free of product names; an app is just an id in the `apps` table.

The dashboard is server-rendered: templ, htmx, Tailwind 4 with daisyUI 5,
built with Vite into `internal/web/dist` and embedded. Node is needed only
to build.

Design notes and decisions live in GitHub issues (`gh issue ...`), not in
the repo. Don't add docs files.

## Commands

- `make setup`: install pnpm packages and the pre-push hook (once). There is no hosted CI; the hook runs `make check` on a clean tree before every push.
- `make db-setup`: create the `beacon` database (once). `make db-up` starts Postgres in Docker instead.
- `make dev`: Vite plus the server with live reload; open http://localhost:7350.
- `make seed`: add or replace a `demo` app with 90 days of generated events.
- `make gen`: regenerate Go from `*.templ` and `internal/db/queries/*.sql`. Run it after editing either.
- `make check`: gen, templ fmt, go vet, golangci-lint, go test, tsc, biome, frontend build. **Work is done only when this passes.**

## Layout

```
cmd/beacon/          serve, migrate, app create|key|list|revoke, user create|reset
cmd/seed/            generated demo data for development
client/go/           Go client, its own module, standard library only
client/ts/           TypeScript client, no runtime dependencies; storage and gzip are pluggable
client/rust/         Rust client, the app supplies the HTTP transport
client/conformance/  cases every client and the server must pass
frontend/src/        main.ts (htmx setup), app.css (Tailwind + daisyUI theme)
internal/config/     BEACON_* environment
internal/db/         Connect, Migrate, migrations/, queries/, sqlc output (don't edit generated files)
internal/apps/       apps, ingest keys, Resolver (key -> app id, cached)
internal/ingest/     POST /v1/events
internal/retention/  daily deletion of events older than BEACON_RETENTION_DAYS
internal/geo/        country and city from a MaxMind format database
internal/auth/       dashboard accounts, argon2id passwords, sessions, login page
internal/dashboard/  dashboard handler and pages
internal/web/        middleware, CSP, Handle/Render, layout, embedded Vite build
internal/testdb/     per-test databases copied from a migrated template
```

## Rules

- Tests run against real Postgres via `testdb.New(t)`, never mocks. Call `t.Parallel()`.
- Never edit a shipped migration; add a new one.
- Go code calls sqlc methods and never builds SQL strings.
- A change to what POST /v1/events accepts updates `client/conformance/` and every client in the same change.
- Never store client IP addresses. Geo lookup happens at ingest and the address is dropped.
- The CSP forbids inline scripts, inline styles and eval: no `<script>` blocks, `style=""`, `onclick=""` or `hx-on:*` in templates.

## Comments and prose

Keep a comment only when it says something the code does not: why, an
invariant, or an outside constraint. Delete comments that restate the code,
argue against alternatives nobody proposed, list what something doesn't do,
or recount history. An exported name gets a one or two line doc comment
starting with its name. Write plain sentences. Commits, issues and PRs too.
