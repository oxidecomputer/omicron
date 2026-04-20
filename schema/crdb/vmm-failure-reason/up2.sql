SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.vmm
    SET failure_reason = 'unknown'
    WHERE state = 'failed'
      AND failure_reason IS NULL;
