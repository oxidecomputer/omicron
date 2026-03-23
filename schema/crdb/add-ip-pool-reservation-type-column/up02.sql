ALTER TABLE omicron.public.ip_pool
ADD COLUMN IF NOT EXISTS reservation_type omicron.public.ip_pool_reservation_type NOT NULL
DEFAULT 'external_silos';
