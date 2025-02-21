SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.bp_omicron_zone SET disposition = CASE
    WHEN disposition_temp = 'in_service' THEN 'in_service'
    WHEN disposition_temp = 'expunged' THEN 'expunged'
    ELSE NULL
END;
