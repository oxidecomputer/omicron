ALTER TABLE
  omicron.public.silo_user
ADD CONSTRAINT IF NOT EXISTS
  user_name_consistency CHECK (
    CASE user_provision_type
      WHEN 'scim' THEN user_name IS NOT NULL
    END
  )
;
