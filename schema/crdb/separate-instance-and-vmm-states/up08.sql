SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.instance SET state = CASE
    WHEN downlevel_state = 'creating' THEN 'creating'
    WHEN downlevel_state = 'failed' THEN 'failed'
    WHEN downlevel_state = 'destroyed' THEN 'destroyed'
    WHEN active_propolis_id IS NOT NULL THEN 'vmm'
    ELSE 'no_vmm'
END;
