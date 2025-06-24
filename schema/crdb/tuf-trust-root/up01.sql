CREATE TABLE IF NOT EXISTS omicron.public.tuf_trust_root (
    -- Identity metadata (resource)
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    root_role JSONB NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS tuf_trust_root_by_id
ON omicron.public.tuf_trust_root (id)
WHERE
    time_deleted IS NULL;
