CREATE UNIQUE INDEX IF NOT EXISTS
  lookup_silo_group_by_silo_and_display_name
ON
  omicron.public.silo_group (silo_id, display_name)
WHERE
  time_deleted IS NULL AND user_provision_type = 'scim';
