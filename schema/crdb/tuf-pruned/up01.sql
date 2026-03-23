ALTER TABLE omicron.public.tuf_repo
    ADD COLUMN IF NOT EXISTS time_pruned TIMESTAMPTZ;
