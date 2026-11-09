-- +goose Up
-- Keys are revoked by prefix, which has to name exactly one key.
CREATE UNIQUE INDEX app_keys_prefix ON app_keys (prefix);

-- +goose Down
DROP INDEX app_keys_prefix;
