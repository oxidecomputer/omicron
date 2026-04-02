CREATE TABLE IF NOT EXISTS omicron.public.ereporter_restart (
    id UUID PRIMARY KEY,
    generation INT8 NOT NULL,
    reporter_type omicron.public.ereporter_type NOT NULL,
    slot_type omicron.public.sp_type NOT NULL,
    slot INT4 NOT NULL,
    time_first_seen TIMESTAMPTZ NOT NULL,
    CONSTRAINT reporter_identity_validity CHECK (
        (slot_type = 'sled' AND reporter_type = 'host') OR
        reporter_type != 'host'
    )
);
