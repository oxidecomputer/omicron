CREATE UNIQUE INDEX IF NOT EXISTS federation_identity_provider_silo_name
ON omicron.public.federation_identity_provider (silo_id, name)
WHERE time_deleted IS NULL;
