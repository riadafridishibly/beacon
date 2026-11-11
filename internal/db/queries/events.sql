-- name: InsertEvents :copyfrom
INSERT INTO events (
    app_id, name, ts, device_id, user_id, session_id,
    app_version, platform, os_version, country, city, value, props
) VALUES (
    @app_id, @name, @ts, @device_id, @user_id, @session_id,
    @app_version, @platform, @os_version, @country, @city, @value, @props
);

-- IngestEvents stores events, given as a JSON array of objects, and skips
-- those whose event_id the app already has.
-- name: IngestEvents :execrows
INSERT INTO events (
    app_id, event_id, name, ts, device_id, user_id, session_id,
    app_version, platform, os_version, country, city, value, props
)
SELECT @app_id::text, r.event_id, r.name, r.ts, r.device_id, r.user_id, r.session_id,
       r.app_version, r.platform, r.os_version, sqlc.narg('country')::text, sqlc.narg('city')::text,
       r.value, r.props
FROM jsonb_to_recordset(@events::jsonb) AS r (
    event_id text, name text, ts timestamptz, device_id text, user_id text, session_id text,
    app_version text, platform text, os_version text, value float8, props jsonb
)
ON CONFLICT (app_id, event_id) WHERE event_id IS NOT NULL DO NOTHING;

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

-- DeleteEventsBefore deletes up to batch_size of an app's events older than
-- before. Small batches keep each transaction short while ingest runs.
-- name: DeleteEventsBefore :execrows
DELETE FROM events
WHERE id IN (
    SELECT e.id FROM events e
    WHERE e.app_id = @app_id AND e.ts < @before
    LIMIT @batch_size
);
