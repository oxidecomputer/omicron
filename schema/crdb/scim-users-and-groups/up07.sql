CREATE UNIQUE INDEX IF NOT EXISTS
  lookup_silo_user_by_silo_and_user_name
ON
  omicron.public.silo_user (silo_id, user_name)
WHERE
  time_deleted IS NULL AND user_provision_type = 'scim';
