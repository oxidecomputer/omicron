CREATE UNIQUE INDEX IF NOT EXISTS federation_trust_policy_silo_name
ON omicron.public.federation_trust_policy (silo_id, name)
WHERE time_deleted IS NULL;
