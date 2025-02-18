ALTER TABLE omicron.public.bp_omicron_zone
    ADD COLUMN IF NOT EXISTS
    expunged_confirmed_shut_down BOOL NOT NULL DEFAULT false;
