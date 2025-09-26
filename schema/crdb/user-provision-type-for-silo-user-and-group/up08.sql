ALTER TABLE
 omicron.public.silo_group
ADD COLUMN IF NOT EXISTS
 user_provision_type omicron.public.user_provision_type
DEFAULT NULL;
