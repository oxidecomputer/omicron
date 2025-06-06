-- Remove m2s from the physical disk table
SET LOCAL disallow_full_table_scans = off;
DELETE FROM omicron.public.physical_disk WHERE variant = 'm2';
