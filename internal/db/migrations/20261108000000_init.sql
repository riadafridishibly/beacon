-- +goose Up
CREATE TABLE apps (
    id         text PRIMARY KEY CHECK (id ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
    name       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Clients embed the key, so it identifies an app rather than proving who
-- sends. Only its SHA-256 is stored.
CREATE TABLE app_keys (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    app_id     text NOT NULL REFERENCES apps (id) ON DELETE CASCADE,
    key_hash   bytea NOT NULL UNIQUE,
    prefix     text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);

CREATE TABLE events (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    app_id      text NOT NULL REFERENCES apps (id) ON DELETE CASCADE,
    name        text NOT NULL,
    ts          timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    device_id   text NOT NULL,
    user_id     text,
    session_id  text,
    app_version text,
    platform    text,
    os_version  text,
    country     text,
    city        text,
    value       double precision,
    props       jsonb
);

CREATE INDEX events_app_ts ON events (app_id, ts);
CREATE INDEX events_app_name_ts ON events (app_id, name, ts);
CREATE INDEX events_app_device_ts ON events (app_id, device_id, ts);
CREATE INDEX events_app_user_ts ON events (app_id, user_id, ts) WHERE user_id IS NOT NULL;

-- +goose Down
DROP TABLE events;
DROP TABLE app_keys;
DROP TABLE apps;
