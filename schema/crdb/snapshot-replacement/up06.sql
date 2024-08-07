CREATE INDEX IF NOT EXISTS lookup_region_snapshot_replacement_step_by_state
    on omicron.public.region_snapshot_replacement_step (replacement_state);
