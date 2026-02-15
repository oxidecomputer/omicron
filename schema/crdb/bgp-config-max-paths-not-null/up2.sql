-- Now that all existing rows have been backfilled, enforce NOT NULL.

ALTER TABLE omicron.public.bgp_config
  ALTER COLUMN max_paths SET NOT NULL;
