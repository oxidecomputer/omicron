/*
 * Ensure we have no default pool for the internal silo.
 */
SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.ip_pool_resource
SET is_default = FALSE
WHERE
    resource_type = 'silo' AND
    resource_id = '001de000-5110-4000-8000-000000000001';
