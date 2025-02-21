SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.bp_omicron_zone SET disposition_temp = CASE
    WHEN disposition = 'in_service' THEN 'in_service'
    WHEN disposition = 'quiesced' THEN 'in_service'
    WHEN disposition = 'expunged' THEN 'expunged'
    ELSE NULL
END;
