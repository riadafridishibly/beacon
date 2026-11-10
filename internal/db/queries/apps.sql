-- name: CreateApp :one
INSERT INTO apps (id, name) VALUES (@id, @name)
RETURNING *;

-- name: ListApps :many
SELECT * FROM apps ORDER BY id;

-- name: CreateAppKey :one
INSERT INTO app_keys (app_id, key_hash, prefix) VALUES (@app_id, @key_hash, @prefix)
RETURNING id;

-- name: ListAppKeys :many
SELECT prefix, created_at, revoked_at FROM app_keys
WHERE app_id = @app_id
ORDER BY revoked_at IS NOT NULL, created_at DESC, id DESC;

-- name: RevokeKey :one
UPDATE app_keys SET revoked_at = coalesce(revoked_at, now())
WHERE prefix = @prefix
RETURNING app_id;

-- name: AppForKey :one
SELECT app_id FROM app_keys
WHERE key_hash = @key_hash AND revoked_at IS NULL;

-- name: AppSummaries :many
SELECT a.id, a.name,
       count(e.id) AS events,
       count(DISTINCT e.device_id) AS devices,
       count(e.id) FILTER (WHERE e.name LIKE 'error.%') AS errors
FROM apps a
LEFT JOIN events e ON e.app_id = a.id AND e.ts >= @since
GROUP BY a.id
ORDER BY a.id;

-- name: GetApp :one
SELECT * FROM apps WHERE id = @id;

-- name: DeleteApp :execrows
DELETE FROM apps WHERE id = @id;
