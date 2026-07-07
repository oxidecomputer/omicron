-- Add CHECK constraint to ip_pool_range to ensure data integrity
-- Ensure first address is not greater than last address
ALTER TABLE omicron.public.ip_pool_range
  ADD CONSTRAINT IF NOT EXISTS check_address_order
  CHECK (first_address <= last_address);
