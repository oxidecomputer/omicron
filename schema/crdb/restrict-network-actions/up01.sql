-- Add restrict_network_actions column to silo table
-- When true, only Silo Admins can create/update/delete networking resources
ALTER TABLE omicron.public.silo ADD COLUMN restrict_network_actions BOOL NOT NULL DEFAULT FALSE;