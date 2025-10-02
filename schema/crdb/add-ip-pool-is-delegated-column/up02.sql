SET LOCAL disallow_full_table_scans = 'off';
UPDATE ip_pool
SET is_delegated = TRUE
WHERE name = 'oxide-service-pool-v4' OR name = 'oxide-service-pool-v6';
