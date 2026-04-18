ALTER TABLE omicron.public.tuf_repo
    ALTER COLUMN sha256 DROP NOT NULL,
    DROP COLUMN IF EXISTS targets_role_version,
    DROP COLUMN IF EXISTS valid_until,
    ALTER COLUMN file_name DROP NOT NULL;
