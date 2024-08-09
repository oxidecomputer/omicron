CREATE INDEX IF NOT EXISTS lookup_region_snapshot_replacement_step_by_old_volume_id
    on omicron.public.region_snapshot_replacement_step (old_snapshot_volume_id);
