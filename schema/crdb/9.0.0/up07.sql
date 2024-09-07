CREATE TABLE IF NOT EXISTS inv_collection (
    id UUID PRIMARY KEY,
    time_started TIMESTAMPTZ NOT NULL,
    time_done TIMESTAMPTZ NOT NULL,
    collector TEXT NOT NULL
);
