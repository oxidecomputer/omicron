-- Create a temporary column in the ip_pool table with the temporary enum
--
-- We'll set everything to 'external_silos' now, and then update in the next
-- migration step.
ALTER TABLE omicron.public.ip_pool
ADD COLUMN IF NOT EXISTS
reservation_type_temp omicron.public.ip_pool_reservation_type_temp
NOT NULL
DEFAULT 'external_silos';
