CREATE UNIQUE INDEX IF NOT EXISTS lookup_db_metadata_nexus_by_state on omicron.public.db_metadata_nexus (
    state,
    nexus_id
);
