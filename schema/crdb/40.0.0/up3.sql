ALTER TABLE omicron.public.external_ip ADD COLUMN IF NOT EXISTS is_probe BOOL NOT NULL DEFAULT false;
