/*
 * Add check constraint ensuring that, if the pool is linked
 * to the internal Oxide services silo, it's not marked as
 * a default pool.
 */
ALTER TABLE
omicron.public.ip_pool_resource
ADD CONSTRAINT IF NOT EXISTS
internal_silo_has_no_default_pool CHECK (
    NOT (
        resource_type = 'silo' AND
        resource_id = '001de000-5110-4000-8000-000000000001' AND
        is_default
    )
);
