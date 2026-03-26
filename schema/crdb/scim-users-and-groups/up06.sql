CREATE UNIQUE INDEX IF NOT EXISTS
  lookup_silo_user_by_silo_and_external_id
ON
  omicron.public.silo_user (silo_id, external_id)
WHERE
  time_deleted IS NULL AND
  (user_provision_type = 'api_only' OR user_provision_type = 'jit');
