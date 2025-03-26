ALTER TABLE omicron.public.region
  ADD COLUMN IF NOT EXISTS reservation_factor FLOAT NOT NULL DEFAULT 1.25;
