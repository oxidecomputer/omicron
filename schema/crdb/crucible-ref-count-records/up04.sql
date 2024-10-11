CREATE TABLE IF NOT EXISTS omicron.public.volume_resource_usage (
    usage_id UUID NOT NULL,

    volume_id UUID NOT NULL,

    usage_type omicron.public.volume_resource_usage_type NOT NULL,

    region_id UUID,

    region_snapshot_dataset_id UUID,
    region_snapshot_region_id UUID,
    region_snapshot_snapshot_id UUID,

    PRIMARY KEY (usage_id)
);
