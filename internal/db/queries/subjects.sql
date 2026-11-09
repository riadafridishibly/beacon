-- name: ListDevices :many
WITH page AS (
    SELECT device_id AS id, max(ts)::timestamptz AS last_seen
    FROM events
    WHERE app_id = @app_id
    GROUP BY device_id
    HAVING (max(ts), device_id) < (@before_ts::timestamptz, @before_id::text)
    ORDER BY max(ts) DESC, device_id DESC
    LIMIT @page_limit
)
SELECT p.id, p.last_seen, latest.app_version, latest.country, errors.n AS errors
FROM page p
CROSS JOIN LATERAL (
    SELECT e.app_version, e.country
    FROM events e
    WHERE e.app_id = @app_id AND e.device_id = p.id
    ORDER BY e.ts DESC, e.id DESC
    LIMIT 1
) latest
CROSS JOIN LATERAL (
    SELECT count(*) AS n
    FROM events e
    WHERE e.app_id = @app_id AND e.device_id = p.id AND e.name LIKE 'error.%'
) errors
ORDER BY p.last_seen DESC, p.id DESC;

-- name: ListUsers :many
WITH page AS (
    SELECT user_id::text AS id, max(ts)::timestamptz AS last_seen
    FROM events
    WHERE app_id = @app_id AND user_id IS NOT NULL
    GROUP BY user_id
    HAVING (max(ts), user_id) < (@before_ts::timestamptz, @before_id::text)
    ORDER BY max(ts) DESC, user_id DESC
    LIMIT @page_limit
)
SELECT p.id, p.last_seen, latest.app_version, latest.country, errors.n AS errors
FROM page p
CROSS JOIN LATERAL (
    SELECT e.app_version, e.country
    FROM events e
    WHERE e.app_id = @app_id AND e.user_id = p.id
    ORDER BY e.ts DESC, e.id DESC
    LIMIT 1
) latest
CROSS JOIN LATERAL (
    SELECT count(*) AS n
    FROM events e
    WHERE e.app_id = @app_id AND e.user_id = p.id AND e.name LIKE 'error.%'
) errors
ORDER BY p.last_seen DESC, p.id DESC;

-- name: DeviceSummary :one
SELECT s.events, s.errors, s.first_seen, s.last_seen,
       latest.app_version, latest.platform, latest.os_version, latest.country, latest.city
FROM (
    SELECT count(*) AS events,
           count(*) FILTER (WHERE a.name LIKE 'error.%') AS errors,
           min(a.ts)::timestamptz AS first_seen,
           max(a.ts)::timestamptz AS last_seen
    FROM events a
    WHERE a.app_id = @app_id AND a.device_id = @id
    GROUP BY a.device_id
) s
CROSS JOIN LATERAL (
    SELECT e.app_version, e.platform, e.os_version, e.country, e.city
    FROM events e
    WHERE e.app_id = @app_id AND e.device_id = @id
    ORDER BY e.ts DESC, e.id DESC
    LIMIT 1
) latest;

-- name: UserSummary :one
SELECT s.events, s.errors, s.first_seen, s.last_seen,
       latest.app_version, latest.platform, latest.os_version, latest.country, latest.city
FROM (
    SELECT count(*) AS events,
           count(*) FILTER (WHERE a.name LIKE 'error.%') AS errors,
           min(a.ts)::timestamptz AS first_seen,
           max(a.ts)::timestamptz AS last_seen
    FROM events a
    WHERE a.app_id = @app_id AND a.user_id = @id::text
    GROUP BY a.user_id
) s
CROSS JOIN LATERAL (
    SELECT e.app_version, e.platform, e.os_version, e.country, e.city
    FROM events e
    WHERE e.app_id = @app_id AND e.user_id = @id::text
    ORDER BY e.ts DESC, e.id DESC
    LIMIT 1
) latest;

-- name: UsersOfDevice :many
SELECT user_id::text AS id, max(ts)::timestamptz AS last_seen
FROM events
WHERE app_id = @app_id AND device_id = @id AND user_id IS NOT NULL
GROUP BY user_id
ORDER BY max(ts) DESC, user_id
LIMIT @row_limit;

-- name: DevicesOfUser :many
SELECT device_id AS id, max(ts)::timestamptz AS last_seen
FROM events
WHERE app_id = @app_id AND user_id = @id::text
GROUP BY device_id
ORDER BY max(ts) DESC, device_id
LIMIT @row_limit;
