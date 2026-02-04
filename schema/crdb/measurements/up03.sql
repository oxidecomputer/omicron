ALTER TABLE omicron.public.inv_omicron_sled_config
    -- the set of artifact hashes used with trust quorum, can be empty
    ADD COLUMN IF NOT EXISTS measurements STRING(64)[];

