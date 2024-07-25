CREATE INDEX IF NOT EXISTS lookup_snapshot_replacement_step_by_state
    on omicron.public.snapshot_replacement_step (replacement_state);
