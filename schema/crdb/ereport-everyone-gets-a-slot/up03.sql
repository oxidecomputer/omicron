SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.ereport
    SET slot_type = CASE reporter
        WHEN 'sp' THEN sp_type
        WHEN 'host' THEN 'sled'
    END;
