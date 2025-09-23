ALTER TABLE omicron.public.bp_omicron_zone ADD COLUMN IF NOT EXISTS nexus_lockstep_port INT4
    CHECK (nexus_lockstep_port IS NULL OR nexus_lockstep_port BETWEEN 0 AND 65535);
