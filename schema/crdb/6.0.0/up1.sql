ALTER TABLE omicron.public.region_snapshot ADD COLUMN IF NOT EXISTS deleting BOOL NOT NULL DEFAULT false;
