-- name: CreateApp :one
INSERT INTO apps (id, name) VALUES (@id, @name)
RETURNING *;

-- name: ListApps :many
SELECT * FROM apps ORDER BY id;

-- name: CreateAppKey :one
INSERT INTO app_keys (app_id, key_hash, prefix) VALUES (@app_id, @key_hash, @prefix)
RETURNING id;

-- name: AppForKey :one
SELECT app_id FROM app_keys
WHERE key_hash = @key_hash AND revoked_at IS NULL;
