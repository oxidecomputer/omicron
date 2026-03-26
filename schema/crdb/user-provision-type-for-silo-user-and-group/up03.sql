ALTER TABLE
 omicron.public.silo_user
ADD CONSTRAINT IF NOT EXISTS user_provision_type_required_for_non_deleted CHECK (
 (user_provision_type IS NOT NULL AND time_deleted IS NULL)
 OR (time_deleted IS NOT NULL)
)
