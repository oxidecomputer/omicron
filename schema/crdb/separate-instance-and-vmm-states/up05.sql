SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.instance SET downlevel_state = state;
