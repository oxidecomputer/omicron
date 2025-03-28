ALTER TABLE omicron.public.region
  ADD COLUMN IF NOT EXISTS reservation_percent omicron.public.region_reservation_percent NOT NULL DEFAULT '25';
