ALTER TABLE omicron.public.bp_omicron_physical_disk
    ADD COLUMN IF NOT EXISTS disposition_expunged_as_of_generation INT;
