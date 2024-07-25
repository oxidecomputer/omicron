CREATE TABLE IF NOT EXISTS omicron.public.snapshot_replacement_step (
    id UUID PRIMARY KEY,

    request_id UUID NOT NULL,

    request_time TIMESTAMPTZ NOT NULL,

    volume_id UUID NOT NULL,

    old_snapshot_volume_id UUID,

    replacement_state omicron.public.snapshot_replacement_step_state NOT NULL,

    operating_saga_id UUID
);
