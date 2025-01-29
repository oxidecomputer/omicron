set local disallow_full_table_scans = off;

DELETE FROM omicron.public.crucible_dataset WHERE kind != 'crucible';
