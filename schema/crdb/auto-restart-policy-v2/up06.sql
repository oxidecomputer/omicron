SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.instance SET auto_restart_policy = CASE
    WHEN old_auto_restart_policy = 'all_failures' THEN 'best_effort'
    WHEN old_auto_restart_policy = 'sled_failrues_only' THEN 'best_effort'
    WHEN old_auto_restart_policy = 'never' THEN 'never'
    ELSE NULL
END;
