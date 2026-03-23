CREATE UNIQUE INDEX IF NOT EXISTS
  lookup_silo_group_by_silo_and_external_id
ON
  omicron.public.silo_group (silo_id, external_id)
WHERE
  time_deleted IS NULL and
  (user_provision_type = 'api_only' OR user_provision_type = 'jit');
