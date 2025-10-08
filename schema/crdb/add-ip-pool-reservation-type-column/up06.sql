SET LOCAL disallow_full_table_scans = 'off';
DELETE FROM
ip_pool_resource
WHERE resource_type = 'silo' AND resource_id = '001de000-5110-4000-8000-000000000001';
