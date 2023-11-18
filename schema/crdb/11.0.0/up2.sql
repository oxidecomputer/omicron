ALTER TABLE omicron.public.rack ADD COLUMN IF NOT EXISTS reconfiguration_epoch INT8 NOT NULL DEFAULT 0;
