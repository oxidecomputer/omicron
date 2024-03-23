-- We are dropping `kind` so that we can drop the `sled_resource` kind; we'll
-- then recreate it (with some variants removed) and add this column back.
ALTER TABLE omicron.public.sled_resource DROP COLUMN IF EXISTS kind;
