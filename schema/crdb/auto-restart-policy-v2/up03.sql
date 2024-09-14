SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.instance
    SET old_auto_restart_policy = auto_restart_policy;
