CREATE TABLE IF NOT EXISTS omicron.public.rendezvous_local_storage_dataset (
    id UUID PRIMARY KEY,

    time_created TIMESTAMPTZ NOT NULL,

    time_tombstoned TIMESTAMPTZ,

    blueprint_id_when_created UUID NOT NULL,

    blueprint_id_when_tombstoned UUID,

    pool_id UUID NOT NULL,

    size_used INT NOT NULL,

    no_provision BOOL NOT NULL,

    CONSTRAINT tombstoned_consistency CHECK (
        (time_tombstoned IS NULL
            AND blueprint_id_when_tombstoned IS NULL)
        OR
        (time_tombstoned IS NOT NULL
            AND blueprint_id_when_tombstoned IS NOT NULL)
    )
);
