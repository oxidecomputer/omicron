ALTER TABLE omicron.public.region
  ADD COLUMN IF NOT EXISTS read_only BOOLEAN NOT NULL
  DEFAULT false;

