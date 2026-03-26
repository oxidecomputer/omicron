-- Make columns NOT NULL after backfill
ALTER TABLE omicron.public.ip_pool_resource
    ALTER COLUMN pool_type SET NOT NULL,
    ALTER COLUMN ip_version SET NOT NULL;
