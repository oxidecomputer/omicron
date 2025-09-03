ALTER TABLE omicron.public.inv_omicron_sled_config_zone ADD CONSTRAINT IF NOT EXISTS nexus_debug_port_for_nexus_zones CHECK (
    (zone_type = 'nexus' AND nexus_debug_port IS NOT NULL)
    OR
    (zone_type != 'nexus' AND nexus_debug_port IS NULL)
)
