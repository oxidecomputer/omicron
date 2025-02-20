ALTER TABLE omicron.public.bp_omicron_zone
    ADD COLUMN IF NOT EXISTS disposition_expunged_as_of_generation INT;
