ALTER TABLE
  omicron.public.silo_user
ADD COLUMN IF NOT EXISTS
  active BOOL
;
