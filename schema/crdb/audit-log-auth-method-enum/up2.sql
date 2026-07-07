ALTER TABLE omicron.public.audit_log
    ADD COLUMN IF NOT EXISTS auth_method_temp omicron.public.audit_log_auth_method;
