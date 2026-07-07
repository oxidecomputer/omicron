CREATE INDEX IF NOT EXISTS lookup_external_subnet_by_instance_id
ON omicron.public.external_subnet (instance_id)
WHERE
    instance_id IS NOT NULL AND
    time_deleted IS NULL;
