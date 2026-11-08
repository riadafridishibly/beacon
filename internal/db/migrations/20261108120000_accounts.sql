-- +goose Up
-- Accounts sign in to the dashboard. They are unrelated to the user_id that
-- apps send with events.
CREATE TABLE accounts (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email         text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- Only a hash of the session token is stored, so a leaked table can't be
-- used to sign in.
CREATE TABLE sessions (
    token_hash   bytea PRIMARY KEY,
    account_id   bigint NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL
);

CREATE INDEX sessions_account ON sessions (account_id);

-- +goose Down
DROP TABLE sessions;
DROP TABLE accounts;
