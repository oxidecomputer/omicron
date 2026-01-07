ALTER TABLE
  omicron.public.disk
ADD CONSTRAINT IF NOT EXISTS
  check_slot_slot CHECK (slot >= 0 AND slot < 12);
