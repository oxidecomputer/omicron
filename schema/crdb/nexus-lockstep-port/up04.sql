ALTER TABLE omicron.public.inv_omicron_sled_config_zone ADD COLUMN IF NOT EXISTS nexus_lockstep_port INT4
    CHECK (nexus_lockstep_port IS NULL OR nexus_lockstep_port BETWEEN 0 AND 65535);
