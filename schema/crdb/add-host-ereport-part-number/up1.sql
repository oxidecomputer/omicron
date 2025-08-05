ALTER TABLE omicron.common.host_ereport
    ADD COLUMN IF NOT EXISTS part_number STRING(63);
