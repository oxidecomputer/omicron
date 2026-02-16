-- add a column for configuring maximum number of paths for ECMP

ALTER TABLE IF EXISTS omicron.public.bgp_config
  ADD COLUMN IF NOT EXISTS max_paths INT2 CHECK (max_paths > 0 AND max_paths <= 32);
