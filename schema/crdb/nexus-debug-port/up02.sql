SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.bp_omicron_zone
SET nexus_debug_port = 12232
WHERE zone_type = 'nexus';
