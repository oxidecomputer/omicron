CREATE UNIQUE INDEX IF NOT EXISTS subnet_pool_name_key
ON omicron.public.subnet_pool (name)
WHERE
    time_deleted IS NULL;
