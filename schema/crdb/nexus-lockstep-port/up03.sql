ALTER TABLE omicron.public.bp_omicron_zone ADD CONSTRAINT IF NOT EXISTS nexus_lockstep_port_for_nexus_zones CHECK (
    (zone_type = 'nexus' AND nexus_lockstep_port IS NOT NULL)
    OR
    (zone_type != 'nexus' AND nexus_lockstep_port IS NULL)
)
