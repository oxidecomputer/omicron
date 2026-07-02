CREATE TABLE IF NOT EXISTS omicron.public.inv_fmd_host_case (
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- guaranteed to match a row in this collection's `inv_sled_agent`
    sled_id UUID NOT NULL,
    case_id UUID NOT NULL,
    code TEXT NOT NULL,
    url TEXT NOT NULL,
    -- The full FMD fault event payload as JSON, if present. Stored as
    -- JSONB without parsing — Nexus does not interpret the FMD event
    -- schema. JSONB normalizes whitespace and key order, so the value is
    -- preserved structurally (not byte-for-byte) for downstream tooling
    -- (e.g. omdb).
    event JSONB,

    PRIMARY KEY (inv_collection_id, sled_id, case_id)
);
