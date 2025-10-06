ALTER TABLE omicron.public.db_metadata_nexus
    ADD COLUMN IF NOT EXISTS time_row_created TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS time_quiesced TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS time_active TIMESTAMPTZ
