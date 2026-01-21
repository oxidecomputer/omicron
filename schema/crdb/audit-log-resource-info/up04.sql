ALTER TABLE omicron.public.audit_log
ADD CONSTRAINT resource_info_only_on_success CHECK (
    result_kind = 'success' OR (resource_type IS NULL AND resource_id IS NULL)
);
