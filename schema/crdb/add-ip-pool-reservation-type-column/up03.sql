SET LOCAL disallow_full_table_scans = 'off';
UPDATE ip_pool
SET reservation_type = 'oxide_internal'
WHERE name = 'oxide-service-pool-v4' OR name = 'oxide-service-pool-v6';
