CREATE TABLE IF NOT EXISTS omicron.public.volume_resource_usage (
    usage_id UUID NOT NULL,

    volume_id UUID NOT NULL,

    usage_type omicron.public.volume_resource_usage_type NOT NULL,

    region_id UUID,

    region_snapshot_dataset_id UUID,
    region_snapshot_region_id UUID,
    region_snapshot_snapshot_id UUID,

    PRIMARY KEY (usage_id),

    CONSTRAINT exactly_one_usage_source CHECK (
     (
      (usage_type = 'read_only_region') AND
      (region_id IS NOT NULL) AND
      (
       region_snapshot_dataset_id IS NULL AND
       region_snapshot_region_id IS NULL AND
       region_snapshot_snapshot_id IS NULL
      )
     )
    OR
     (
      (usage_type = 'region_snapshot') AND
      (region_id IS NOT NULL) AND
      (
       region_snapshot_dataset_id IS NOT NULL AND
       region_snapshot_region_id IS NOT NULL AND
       region_snapshot_snapshot_id IS NOT NULL
      )
     )
    )
);
