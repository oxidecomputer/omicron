SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.instance SET auto_restart_policy_temp = CASE
    WHEN auto_restart_policy = 'all_failures' THEN 'best_effort'
    WHEN auto_restart_policy = 'sled_failures_only' THEN 'best_effort'
    WHEN auto_restart_policy = 'never' THEN 'never'
    ELSE NULL
END;
