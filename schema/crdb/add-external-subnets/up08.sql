CREATE UNIQUE INDEX IF NOT EXISTS single_default_per_silo
ON omicron.public.subnet_pool_silo_link (silo_id, ip_version)
WHERE
    is_default = TRUE;
