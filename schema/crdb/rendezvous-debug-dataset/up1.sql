CREATE TABLE IF NOT EXISTS omicron.public.rendezvous_debug_dataset (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_tombstoned TIMESTAMPTZ,
    pool_id UUID NOT NULL,
    blueprint_id_when_created UUID NOT NULL,
    blueprint_id_when_tombstoned UUID,
    CONSTRAINT tombstoned_consistency CHECK (
        (time_tombstoned IS NULL
            AND blueprint_id_when_tombstoned IS NULL)
        OR
        (time_tombstoned IS NOT NULL
            AND blueprint_id_when_tombstoned IS NOT NULL)
    )
);
