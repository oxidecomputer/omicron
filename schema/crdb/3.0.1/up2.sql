CREATE UNIQUE INDEX IF NOT EXISTS one_default_pool_per_scope ON omicron.public.ip_pool (
    COALESCE(silo_id, '00000000-0000-0000-0000-000000000000'::uuid)
) WHERE
    is_default = true AND time_deleted IS NULL;
