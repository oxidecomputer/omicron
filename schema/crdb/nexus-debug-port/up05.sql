SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.inv_omicron_sled_config_zone
SET nexus_debug_port = 12232
WHERE zone_type = 'nexus';
