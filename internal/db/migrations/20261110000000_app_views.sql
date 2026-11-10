-- +goose Up
-- A view is an app-specific chart on the overview, defined in the dashboard.
-- group_by and the keys of filter are a column (platform, app_version,
-- country) or props.<key>. An empty group_by draws one series per day.
CREATE TABLE app_views (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    app_id     text NOT NULL REFERENCES apps (id) ON DELETE CASCADE,
    title      text NOT NULL,
    event      text NOT NULL,
    agg        text NOT NULL CHECK (agg IN ('count', 'distinct_devices', 'sum', 'p50', 'p95')),
    group_by   text NOT NULL DEFAULT '',
    filter     jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(filter) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX app_views_app ON app_views (app_id, id);

-- +goose Down
DROP TABLE app_views;
