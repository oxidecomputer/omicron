CREATE TABLE IF NOT EXISTS omicron.public.region_replacement (
    /* unique ID for this region replacement */
    id UUID PRIMARY KEY,

    request_time TIMESTAMPTZ NOT NULL,

    old_region_id UUID NOT NULL,

    volume_id UUID NOT NULL,

    old_region_volume_id UUID,

    new_region_id UUID,

    replacement_state omicron.public.region_replacement_state NOT NULL,

    operating_saga_id UUID
);
