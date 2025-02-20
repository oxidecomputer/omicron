ALTER TABLE omicron.public.bp_omicron_zone
    ADD COLUMN IF NOT EXISTS
    disposition_expunged_ready_for_cleanup BOOL NOT NULL DEFAULT false;
