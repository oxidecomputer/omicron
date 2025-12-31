CREATE INDEX IF NOT EXISTS lookup_external_subnet_by_silo_id
ON omicron.public.external_subnet (silo_id)
WHERE
    time_deleted IS NOT NULL;
