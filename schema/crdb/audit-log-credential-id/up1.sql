ALTER TABLE omicron.public.audit_log
ADD COLUMN IF NOT EXISTS credential_id UUID;
