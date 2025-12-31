CREATE INDEX ON omicron.public.attached_external_subnet
lookup_attached_external_subnet_by_subnet_id (subnet_id)
WHERE
    time_deleted IS NOT NULL;
