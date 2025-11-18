-- Create a temporary enum with the new variants
CREATE TYPE IF NOT EXISTS
omicron.public.ip_pool_reservation_type_temp
AS ENUM ('external_silos', 'system_internal');
