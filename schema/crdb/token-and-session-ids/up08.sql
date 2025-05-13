set local disallow_full_table_scans = off;

UPDATE omicron.public.device_access_token
  SET id = gen_random_uuid()
  WHERE id IS NULL;
