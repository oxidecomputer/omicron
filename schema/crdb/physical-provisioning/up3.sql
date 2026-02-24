-- Add optional physical storage quota to silo_quotas.
-- NULL = no limit (default for existing silos)
-- 0 = zero physical storage usage allowed
-- >0 = byte limit for total physical storage

ALTER TABLE omicron.public.silo_quotas
    ADD COLUMN IF NOT EXISTS physical_storage_bytes INT8;
