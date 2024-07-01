/* Lookup migrations by instance ID */
CREATE INDEX IF NOT EXISTS lookup_migrations_by_instance_id ON omicron.public.migration (
    instance_id
);
