-- +goose Up
-- A view shows one or more measures that share a unit: counts (count,
-- distinct_devices), percentiles of value (p50, p95), or the sum alone.
-- Lines by day show one measure, since each group already has a line.
ALTER TABLE app_views ADD COLUMN measures text[];
UPDATE app_views SET measures = ARRAY[agg];
ALTER TABLE app_views
    ALTER COLUMN measures SET NOT NULL,
    DROP COLUMN agg,
    ADD CONSTRAINT app_views_measures CHECK (
        cardinality(measures) > 0
        AND (measures <@ '{count,distinct_devices}' OR measures <@ '{p50,p95}' OR measures = '{sum}')
    ),
    ADD CONSTRAINT app_views_line_measures CHECK (shape = 'table' OR cardinality(measures) = 1);

-- +goose Down
ALTER TABLE app_views ADD COLUMN agg text;
UPDATE app_views SET agg = measures[1];
ALTER TABLE app_views
    ALTER COLUMN agg SET NOT NULL,
    ADD CHECK (agg IN ('count', 'distinct_devices', 'sum', 'p50', 'p95')),
    DROP COLUMN measures;
