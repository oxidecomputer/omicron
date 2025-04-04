ALTER TABLE omicron.public.crucible_dataset
  ADD COLUMN IF NOT EXISTS no_provision BOOL NOT NULL DEFAULT false;
