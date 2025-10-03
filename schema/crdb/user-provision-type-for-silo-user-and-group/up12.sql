ALTER TABLE
 omicron.public.silo_group
ADD CONSTRAINT IF NOT EXISTS
 external_id_consistency
CHECK (
 CASE user_provision_type
   WHEN 'api_only' THEN external_id IS NOT NULL
   WHEN 'jit' THEN external_id IS NOT NULL
 END
)
