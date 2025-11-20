ALTER TABLE omicron.public.inv_omicron_sled_config
    ADD COLUMN IF NOT EXISTS measurements STRING(64)[];

