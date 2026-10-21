-- The old explicit-empty marker becomes an ordinary empty list.
SET LOCAL disallow_full_table_scans = 'off';
DELETE FROM omicron.public.control_plane_router_configuration
WHERE router_configuration_id IS NULL;
