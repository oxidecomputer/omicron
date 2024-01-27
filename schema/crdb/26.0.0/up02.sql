-- Intentionally nullable for now as we need to backfill using the current
-- value of parent_id.
ALTER TABLE omicron.public.external_ip
ADD COLUMN IF NOT EXISTS state omicron.public.ip_attach_state;
