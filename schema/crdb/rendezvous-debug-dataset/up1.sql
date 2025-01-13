CREATE TABLE IF NOT EXISTS omicron.public.rendezvous_debug_dataset (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_tombstoned TIMESTAMPTZ,
    pool_id UUID NOT NULL,
    blueprint_id_when_recorded UUID NOT NULL
);
