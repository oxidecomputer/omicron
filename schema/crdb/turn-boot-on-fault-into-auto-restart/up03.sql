SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.instance SET auto_restart_policy = CASE
    WHEN boot_on_fault = true THEN 'all_failures'
    ELSE 'never'
END;
