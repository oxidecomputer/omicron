ALTER TABLE
  omicron.public.silo_group
ADD CONSTRAINT IF NOT EXISTS
  display_name_consistency CHECK (
    CASE user_provision_type
      WHEN 'scim' THEN display_name IS NOT NULL
    END
  )
;
