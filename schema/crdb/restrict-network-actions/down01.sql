-- Remove restrict_network_actions column from silo table
ALTER TABLE omicron.public.silo DROP COLUMN restrict_network_actions;