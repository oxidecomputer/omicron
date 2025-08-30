ALTER TABLE omicron.public.bp_omicron_zone ADD CONSTRAINT IF NOT EXISTS nexus_generation_for_nexus_zones CHECK (
    (zone_type = 'nexus' AND nexus_generation IS NOT NULL)
    OR
    (zone_type != 'nexus' AND nexus_generation IS NULL)
);
