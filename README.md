<img src="internal/web/brand/logo.svg" alt="" width="64" height="64">

# beacon

A small self-hosted telemetry server. Apps send batches of events to
`POST /v1/events`; the server stores them in PostgreSQL, grouped by app.

```sh
make setup                          # pnpm install
make db-setup                       # or: make db-up
go run ./cmd/beacon user create you@example.com  # prints a dashboard password
make dev                            # open http://localhost:7350
```

Create apps in the dashboard, or with `beacon app create my-app "My App"`,
which prints the ingest key. `make build` builds the frontend and embeds it
in `bin/beacon`.

Design notes, the event format and planned work are in the GitHub issues.

MIT licensed; see LICENSE.
