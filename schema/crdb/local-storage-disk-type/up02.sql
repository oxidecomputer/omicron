CREATE TABLE IF NOT EXISTS omicron.public.disk_type_local_storage (
    disk_id UUID PRIMARY KEY,

    required_dataset_overhead INT8 NOT NULL,

    local_storage_dataset_allocation_id UUID
);
