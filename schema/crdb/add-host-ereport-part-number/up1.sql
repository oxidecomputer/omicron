ALTER TABLE omicron.public.host_ereport
    ADD COLUMN IF NOT EXISTS part_number STRING(63);
