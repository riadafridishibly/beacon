# beacon

A small self-hosted telemetry server. Apps send batches of events to
`POST /v1/events`; the server stores them in PostgreSQL, grouped by app.

```sh
make db-setup                       # or: make db-up
go run ./cmd/beacon app create my-app "My App"   # prints the ingest key
make dev                            # serves on 127.0.0.1:8080
```

Design notes, the event format and planned work are in the GitHub issues.
