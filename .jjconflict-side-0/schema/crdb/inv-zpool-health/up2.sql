ALTER TABLE omicron.public.inv_zpool
    ADD COLUMN IF NOT EXISTS health omicron.public.inv_zpool_health NOT NULL DEFAULT 'online';
