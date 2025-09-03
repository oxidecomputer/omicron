ALTER TABLE omicron.public.inv_omicron_sled_config_zone ADD COLUMN IF NOT EXISTS nexus_debug_port INT4
    CHECK (nexus_debug_port IS NULL OR nexus_debug_port BETWEEN 0 AND 65535);
