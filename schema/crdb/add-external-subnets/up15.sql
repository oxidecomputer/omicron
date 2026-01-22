CREATE UNIQUE INDEX IF NOT EXISTS lookup_external_subnet_by_first_and_last_address
ON omicron.public.external_subnet (first_address, last_address)
WHERE
    time_deleted IS NULL;
