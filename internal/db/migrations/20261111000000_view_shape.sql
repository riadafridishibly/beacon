-- +goose Up
-- A grouped view is a table of its top groups, or lines of them by day.
ALTER TABLE app_views
    ADD COLUMN shape text NOT NULL DEFAULT 'table' CHECK (shape IN ('table', 'line')),
    ADD CONSTRAINT app_views_line_grouped CHECK (shape = 'table' OR group_by <> '');

-- +goose Down
ALTER TABLE app_views DROP COLUMN shape;
