CREATE TABLE IF NOT EXISTS omicron.public.local_storage_dataset_allocation (
    id UUID PRIMARY KEY,

    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    local_storage_dataset_id UUID NOT NULL,
    pool_id UUID NOT NULL,
    sled_id UUID NOT NULL,

    dataset_size INT8 NOT NULL
);
