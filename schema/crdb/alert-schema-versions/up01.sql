ALTER TABLE omicron.public.alert
ADD COLUMN IF NOT EXISTS schema_version INT8 NOT NULL DEFAULT 1;
