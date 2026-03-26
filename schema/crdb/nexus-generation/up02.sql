SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.bp_omicron_zone
SET nexus_generation = 1
WHERE zone_type = 'nexus';
