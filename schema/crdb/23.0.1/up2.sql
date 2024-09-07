ALTER TABLE omicron.public.ip_pool
    DROP COLUMN IF EXISTS silo_id,
    DROP COLUMN IF EXISTS is_default;
