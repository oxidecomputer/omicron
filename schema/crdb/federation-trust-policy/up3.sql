CREATE INDEX IF NOT EXISTS federation_trust_policy_silo_id
ON omicron.public.federation_trust_policy (silo_id, id)
WHERE time_deleted IS NULL;
