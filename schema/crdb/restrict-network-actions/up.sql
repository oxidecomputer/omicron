-- Add restrict_network_actions column to silo table
-- This column controls whether networking actions (VPC, subnet, etc. create/update/delete)
-- are restricted to Silo Admins only.
-- When false (default), Project Collaborators can perform networking actions.

ALTER TABLE omicron.public.silo
    ADD COLUMN restrict_network_actions BOOL
        NOT NULL
        DEFAULT FALSE;
