SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.instance SET auto_restart_policy = CASE
    WHEN auto_restart_policy_temp = 'best_effort' THEN 'best_effort'
    WHEN auto_restart_policy_temp = 'never' THEN 'never'
    ELSE NULL
END;
