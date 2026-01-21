ALTER TABLE omicron.public.audit_log
ADD CONSTRAINT resource_type_and_id_consistent CHECK (
    (resource_type IS NULL AND resource_id IS NULL)
    OR (resource_type IS NOT NULL AND resource_id IS NOT NULL)
);
