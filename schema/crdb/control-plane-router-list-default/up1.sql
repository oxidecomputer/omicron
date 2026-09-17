-- No rows in the old schema means implicit default routing. Seed it once
-- during upgrade. A NULL marker or a custom assignment must be preserved.
SET LOCAL disallow_full_table_scans = 'off';
INSERT INTO omicron.public.control_plane_router_configuration
    (priority, router_configuration_id)
SELECT 1000, '001de000-defa-4000-8000-000000000000'::UUID
WHERE NOT EXISTS (
    SELECT 1 FROM omicron.public.control_plane_router_configuration
)
ON CONFLICT DO NOTHING;
