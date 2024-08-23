CREATE TABLE IF NOT EXISTS omicron.public.region_snapshot_replacement (
    id UUID PRIMARY KEY,

    request_time TIMESTAMPTZ NOT NULL,

    old_dataset_id UUID NOT NULL,
    old_region_id UUID NOT NULL,
    old_snapshot_id UUID NOT NULL,

    old_snapshot_volume_id UUID,

    new_region_id UUID,

    replacement_state omicron.public.region_snapshot_replacement_state NOT NULL,

    operating_saga_id UUID
);
