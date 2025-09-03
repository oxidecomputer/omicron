ALTER TABLE omicron.public.bp_omicron_zone ADD COLUMN IF NOT EXISTS nexus_debug_port INT4
    CHECK (nexus_debug_port IS NULL OR nexus_debug_port BETWEEN 0 AND 65535);
