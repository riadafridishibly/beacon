-- +goose Up
-- event_id is the client's id for an event, the same when the client resends
-- it. Ingest skips an event whose id the app already has.
ALTER TABLE events ADD COLUMN event_id text;
CREATE UNIQUE INDEX events_app_event_id ON events (app_id, event_id) WHERE event_id IS NOT NULL;

-- +goose Down
ALTER TABLE events DROP COLUMN event_id;
