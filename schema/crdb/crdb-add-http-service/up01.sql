set local disallow_full_table_scans = off;

UPDATE bp_omicron_zone
SET second_service_ip = primary_service_ip,
    second_service_port = 32223
WHERE zone_type = 'cockroach_db';

set local disallow_full_table_scans = on;

