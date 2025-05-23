set local disallow_full_table_scans = off;

UPDATE omicron.public.console_session
  SET id = gen_random_uuid()
  WHERE id IS NULL;
