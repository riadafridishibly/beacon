-- name: OverviewTotals :one
SELECT count(DISTINCT device_id) AS devices,
       count(DISTINCT user_id) AS users,
       count(*) AS events,
       count(*) FILTER (WHERE name = 'app.open') AS opens,
       count(*) FILTER (WHERE name LIKE 'error.%') AS errors
FROM events
WHERE app_id = @app_id AND ts >= @since AND ts < @until;

-- name: OverviewDaily :many
SELECT (ts AT TIME ZONE 'UTC')::date AS day,
       count(DISTINCT device_id) AS devices,
       count(DISTINCT user_id) AS users,
       count(*) FILTER (WHERE name = 'app.open') AS opens,
       count(*) FILTER (WHERE name LIKE 'error.%') AS errors,
       count(value) FILTER (WHERE name = 'app.open') AS timed_opens,
       coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY value) FILTER (WHERE name = 'app.open'), 0)::double precision AS open_p50,
       coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY value) FILTER (WHERE name = 'app.open'), 0)::double precision AS open_p95
FROM events
WHERE app_id = @app_id AND ts >= @since AND ts < @until
GROUP BY day
ORDER BY day;

-- Heartbeat and end values are seconds since the session's previous report,
-- so a session's length is their sum, and a session that never sent
-- session.end still counts. It belongs to the day of its first event.
-- name: OverviewUsage :many
WITH sessions AS (
    SELECT (min(ts) AT TIME ZONE 'UTC')::date AS day, sum(value) AS seconds
    FROM events
    WHERE app_id = @app_id AND ts >= @since AND ts < @until
      AND name IN ('session.start', 'session.heartbeat', 'session.end')
      AND session_id IS NOT NULL
      AND (value IS NULL OR value >= 0)
    GROUP BY device_id, session_id
    HAVING count(value) > 0
)
SELECT day, sum(seconds)::double precision AS seconds
FROM sessions
GROUP BY day
ORDER BY day;

-- name: OverviewTopErrors :many
SELECT name,
       coalesce(left(props->>'message', 200), '')::text AS message,
       count(DISTINCT device_id) AS devices,
       count(DISTINCT user_id) AS users,
       max(ts)::timestamptz AS last_seen
FROM events
WHERE app_id = @app_id AND ts >= @since AND ts < @until AND name LIKE 'error.%'
GROUP BY name, message
ORDER BY devices DESC, last_seen DESC, name, message
LIMIT 10;

-- name: OverviewBreakdown :many
SELECT coalesce(CASE @dimension::text
           WHEN 'country' THEN country
           WHEN 'platform' THEN platform
           WHEN 'app_version' THEN app_version
       END, '')::text AS label,
       count(DISTINCT device_id) AS devices
FROM events
WHERE app_id = @app_id AND ts >= @since AND ts < @until
GROUP BY label
ORDER BY devices DESC, label
LIMIT 8;
