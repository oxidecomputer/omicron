ALTER TABLE omicron.public.bp_omicron_physical_disk
    ADD COLUMN IF NOT EXISTS
    disposition_expunged_ready_for_cleanup BOOL NOT NULL DEFAULT false;
