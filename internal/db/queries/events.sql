-- name: InsertEvents :copyfrom
INSERT INTO events (
    app_id, name, ts, device_id, user_id, session_id,
    app_version, platform, os_version, country, city, value, props
) VALUES (
    @app_id, @name, @ts, @device_id, @user_id, @session_id,
    @app_version, @platform, @os_version, @country, @city, @value, @props
);

-- name: ListEvents :many
SELECT *
FROM events
WHERE app_id = @app_id
  AND (ts, id) < (@before_ts::timestamptz, @before_id::bigint)
  AND (sqlc.narg('name')::text IS NULL OR name = sqlc.narg('name'))
  AND (sqlc.narg('device_id')::text IS NULL OR device_id = sqlc.narg('device_id'))
  AND (sqlc.narg('user_id')::text IS NULL OR user_id = sqlc.narg('user_id'))
  AND (sqlc.narg('country')::text IS NULL OR country = sqlc.narg('country'))
  AND (sqlc.narg('app_version')::text IS NULL OR app_version = sqlc.narg('app_version'))
ORDER BY ts DESC, id DESC
LIMIT @page_limit;
