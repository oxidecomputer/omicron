CREATE INDEX IF NOT EXISTS federation_identity_provider_silo_id
ON omicron.public.federation_identity_provider (silo_id, id)
WHERE time_deleted IS NULL;
