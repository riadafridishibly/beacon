-- Props keys come from each name's latest events, because reading every
-- row's props costs a full table scan. Rows without props stay in the sample
-- so its size bounds the rows read.
-- name: ListEventNames :many
WITH page AS (
    SELECT n.name, count(*) AS events, max(n.ts)::timestamptz AS last_seen
    FROM events n
    WHERE n.app_id = @app_id
    GROUP BY n.name
    HAVING (count(*), n.name) < (@before_count::bigint, @before_name::text)
    ORDER BY count(*) DESC, n.name DESC
    LIMIT @page_limit
)
SELECT p.name, p.events, p.last_seen, keys.keys
FROM page p
CROSS JOIN LATERAL (
    SELECT coalesce(array_agg(DISTINCT k.key ORDER BY k.key), '{}')::text[] AS keys
    FROM (
        SELECT e.props
        FROM events e
        WHERE e.app_id = @app_id AND e.name = p.name
        ORDER BY e.ts DESC
        LIMIT @key_sample
    ) recent
    CROSS JOIN LATERAL jsonb_object_keys(recent.props) AS k(key)
) keys
ORDER BY p.events DESC, p.name DESC;

-- name: EventLastSeen :one
SELECT ts
FROM events
WHERE app_id = @app_id AND name = @name
ORDER BY ts DESC
LIMIT 1;

-- The row with a NULL day is the whole range.
-- name: EventDaily :many
SELECT (ts AT TIME ZONE 'UTC')::date AS day,
       count(*) AS events,
       count(value) AS with_value,
       coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY value), 0)::double precision AS p50,
       coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY value), 0)::double precision AS p95
FROM events
WHERE app_id = @app_id AND name = @name AND ts >= @since AND ts < @until
GROUP BY GROUPING SETS ((day), ())
ORDER BY day NULLS FIRST;

-- name: EventPropKeys :many
SELECT k.key::text AS key
FROM (
    SELECT e.props
    FROM events e
    WHERE e.app_id = @app_id AND e.name = @name AND e.ts >= @since AND e.ts < @until
    ORDER BY e.ts DESC
    LIMIT @key_sample
) recent
CROSS JOIN LATERAL jsonb_object_keys(recent.props) AS k(key)
GROUP BY k.key
ORDER BY count(*) DESC, k.key
LIMIT @key_limit;

-- name: EventTopValues :many
SELECT left(props ->> @key::text, 200)::text AS label, count(*) AS events
FROM events
WHERE app_id = @app_id AND name = @name AND ts >= @since AND ts < @until
  AND props ->> @key::text IS NOT NULL
GROUP BY label
ORDER BY events DESC, label
LIMIT 10;
