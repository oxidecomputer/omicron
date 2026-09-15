ALTER TABLE omicron.public.inv_omicron_sled_config
    ADD COLUMN IF NOT EXISTS update_disposition omicron.public.inv_sled_update_disposition NOT NULL DEFAULT 'available';
