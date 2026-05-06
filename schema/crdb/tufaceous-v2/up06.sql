ALTER TABLE omicron.public.tuf_artifact
    DROP COLUMN IF EXISTS name,
    DROP COLUMN IF EXISTS kind,
    DROP COLUMN IF EXISTS sign,
    DROP COLUMN IF EXISTS board;
