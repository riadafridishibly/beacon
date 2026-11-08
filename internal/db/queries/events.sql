-- name: InsertEvents :copyfrom
INSERT INTO events (
    app_id, name, ts, device_id, user_id, session_id,
    app_version, platform, os_version, country, city, value, props
) VALUES (
    @app_id, @name, @ts, @device_id, @user_id, @session_id,
    @app_version, @platform, @os_version, @country, @city, @value, @props
);
