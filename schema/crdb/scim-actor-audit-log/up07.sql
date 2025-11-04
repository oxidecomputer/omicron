CREATE INDEX IF NOT EXISTS lookup_role_assignment_by_identity_id
    ON omicron.public.role_assignment ( identity_id );
