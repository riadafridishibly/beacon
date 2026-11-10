-- name: ListViews :many
SELECT * FROM app_views WHERE app_id = @app_id ORDER BY id;

-- name: GetView :one
SELECT * FROM app_views WHERE app_id = @app_id AND id = @id;

-- Returns no row when the app already has max_views views.
-- name: CreateView :one
INSERT INTO app_views (app_id, title, event, measures, group_by, filter, shape)
SELECT @app_id, @title, @event, @measures::text[], @group_by, @filter, @shape
WHERE (SELECT count(*) FROM app_views v WHERE v.app_id = @app_id) < @max_views::bigint
RETURNING id;

-- name: UpdateView :execrows
UPDATE app_views
SET title = @title, event = @event, measures = @measures::text[], group_by = @group_by, filter = @filter, shape = @shape
WHERE app_id = @app_id AND id = @id;

-- name: DeleteView :execrows
DELETE FROM app_views WHERE app_id = @app_id AND id = @id;

-- props_filter matches by containment, so its JSON types must match. The
-- column filters are NULL when unset.
-- name: ViewDaily :many
SELECT d.day, d.events, d.devices, d.with_value, d.total,
       coalesce(d.p[1], 0)::double precision AS p50,
       coalesce(d.p[2], 0)::double precision AS p95
FROM (
    SELECT (e.ts AT TIME ZONE 'UTC')::date AS day,
           count(*) AS events,
           count(DISTINCT e.device_id) AS devices,
           count(e.value) AS with_value,
           coalesce(sum(e.value), 0)::double precision AS total,
           percentile_cont(ARRAY[0.5, 0.95]::double precision[]) WITHIN GROUP (ORDER BY e.value) AS p
    FROM events e
    WHERE e.app_id = @app_id AND e.name = @name AND e.ts >= @since AND e.ts < @until
      AND (@props_filter::jsonb = '{}'::jsonb OR e.props @> @props_filter::jsonb)
      AND (sqlc.narg('platform')::text IS NULL OR e.platform = sqlc.narg('platform'))
      AND (sqlc.narg('app_version')::text IS NULL OR e.app_version = sqlc.narg('app_version'))
      AND (sqlc.narg('country')::text IS NULL OR e.country = sqlc.narg('country'))
    GROUP BY 1
) d
ORDER BY d.day;

-- Measures are computed only for the top groups by count, since a props key
-- can have a distinct value on every row. A missing value has an empty label.
-- name: ViewGroups :many
WITH matched AS (
    SELECT coalesce(CASE @dimension::text
               WHEN 'platform' THEN e.platform
               WHEN 'app_version' THEN e.app_version
               WHEN 'country' THEN e.country
               ELSE left(e.props ->> @prop_key::text, 200)
           END, '')::text AS label,
           e.device_id,
           e.value
    FROM events e
    WHERE e.app_id = @app_id AND e.name = @name AND e.ts >= @since AND e.ts < @until
      AND (@props_filter::jsonb = '{}'::jsonb OR e.props @> @props_filter::jsonb)
      AND (sqlc.narg('platform')::text IS NULL OR e.platform = sqlc.narg('platform'))
      AND (sqlc.narg('app_version')::text IS NULL OR e.app_version = sqlc.narg('app_version'))
      AND (sqlc.narg('country')::text IS NULL OR e.country = sqlc.narg('country'))
), top AS (
    SELECT m.label, count(*) AS events
    FROM matched m
    GROUP BY m.label
    ORDER BY 2 DESC, 1
    LIMIT @group_limit
), measured AS (
    SELECT m.label,
           count(DISTINCT m.device_id) AS devices,
           count(m.value) AS with_value,
           coalesce(sum(m.value), 0)::double precision AS total,
           percentile_cont(ARRAY[0.5, 0.95]::double precision[]) WITHIN GROUP (ORDER BY m.value) AS p
    FROM matched m
    WHERE m.label IN (SELECT top.label FROM top)
    GROUP BY m.label
)
SELECT t.label, t.events, ms.devices, ms.with_value, ms.total,
       coalesce(ms.p[1], 0)::double precision AS p50,
       coalesce(ms.p[2], 0)::double precision AS p95
FROM top t
JOIN measured ms ON ms.label = t.label
ORDER BY t.events DESC, t.label;

-- The top groups by count over the range get a row per day, and the other
-- groups share one row per day with other set. Rows come in the groups'
-- order by count, then by day.
-- name: ViewGroupsDaily :many
WITH matched AS (
    SELECT coalesce(CASE @dimension::text
               WHEN 'platform' THEN e.platform
               WHEN 'app_version' THEN e.app_version
               WHEN 'country' THEN e.country
               ELSE left(e.props ->> @prop_key::text, 200)
           END, '')::text AS label,
           (e.ts AT TIME ZONE 'UTC')::date AS day,
           e.device_id,
           e.value
    FROM events e
    WHERE e.app_id = @app_id AND e.name = @name AND e.ts >= @since AND e.ts < @until
      AND (@props_filter::jsonb = '{}'::jsonb OR e.props @> @props_filter::jsonb)
      AND (sqlc.narg('platform')::text IS NULL OR e.platform = sqlc.narg('platform'))
      AND (sqlc.narg('app_version')::text IS NULL OR e.app_version = sqlc.narg('app_version'))
      AND (sqlc.narg('country')::text IS NULL OR e.country = sqlc.narg('country'))
), top AS (
    SELECT m.label, row_number() OVER (ORDER BY count(*) DESC, m.label) AS rank
    FROM matched m
    GROUP BY m.label
    ORDER BY 2
    LIMIT @group_limit
)
SELECT g.label, g.other, g.day, g.events, g.devices, g.with_value, g.total,
       coalesce(g.p[1], 0)::double precision AS p50,
       coalesce(g.p[2], 0)::double precision AS p95
FROM (
    SELECT coalesce(t.label, '')::text AS label,
           t.rank,
           (t.rank IS NULL)::boolean AS other,
           m.day,
           count(*) AS events,
           count(DISTINCT m.device_id) AS devices,
           count(m.value) AS with_value,
           coalesce(sum(m.value), 0)::double precision AS total,
           percentile_cont(ARRAY[0.5, 0.95]::double precision[]) WITHIN GROUP (ORDER BY m.value) AS p
    FROM matched m
    LEFT JOIN top t ON t.label = m.label
    GROUP BY t.label, t.rank, m.day
) g
ORDER BY g.rank NULLS LAST, g.day;
