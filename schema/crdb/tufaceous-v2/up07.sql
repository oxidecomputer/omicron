ALTER TABLE omicron.public.tuf_repo
    DROP COLUMN IF EXISTS targets_role_version,
    DROP COLUMN IF EXISTS valid_until;
