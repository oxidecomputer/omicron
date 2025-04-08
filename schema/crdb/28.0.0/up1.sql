CREATE TABLE IF NOT EXISTS omicron.public.blueprint (
    id UUID PRIMARY KEY,
    parent_blueprint_id UUID,
    time_created TIMESTAMPTZ NOT NULL,
    creator TEXT NOT NULL,
    comment TEXT NOT NULL
);
