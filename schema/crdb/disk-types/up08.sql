ALTER TABLE
  omicron.public.disk
ADD COLUMN IF NOT EXISTS
  disk_type omicron.public.disk_type NOT NULL
DEFAULT
  'crucible'
