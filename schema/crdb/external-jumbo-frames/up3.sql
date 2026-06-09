ALTER TABLE omicron.public.instance
  ADD COLUMN IF NOT EXISTS enable_jumbo_frames BOOL NOT NULL DEFAULT FALSE;
