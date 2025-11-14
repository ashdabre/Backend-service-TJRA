-- sql/schema.sql

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Raw events
CREATE TABLE IF NOT EXISTS events (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    site_id text NOT NULL,
    event_type text NOT NULL,
    path text,
    user_id text,
    occurred_at timestamptz NOT NULL
);

-- Keep track of one row per site/date/user to count unique users easily
CREATE TABLE IF NOT EXISTS daily_unique_users (
    site_id text NOT NULL,
    date date NOT NULL,
    user_id text NOT NULL,
    PRIMARY KEY (site_id, date, user_id)
);

-- daily stats per site (aggregated)
CREATE TABLE IF NOT EXISTS daily_stats (
    site_id text NOT NULL,
    date date NOT NULL,
    total_views bigint NOT NULL DEFAULT 0,
    unique_users bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (site_id, date)
);

-- daily path views per site
CREATE TABLE IF NOT EXISTS daily_path_views (
    site_id text NOT NULL,
    date date NOT NULL,
    path text NOT NULL,
    views bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (site_id, date, path)
);
