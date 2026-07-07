ALTER TABLE omicron.public.ip_pool
    ADD COLUMN IF NOT EXISTS assignment omicron.public.ip_pool_assignment;
