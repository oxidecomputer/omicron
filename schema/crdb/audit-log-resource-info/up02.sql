ALTER TABLE omicron.public.audit_log
ADD COLUMN IF NOT EXISTS resource_id UUID;
