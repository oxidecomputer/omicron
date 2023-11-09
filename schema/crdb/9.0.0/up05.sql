CREATE TABLE IF NOT EXISTS omicron.public.sw_caboose (
    id UUID PRIMARY KEY,
    board TEXT NOT NULL,
    git_commit TEXT NOT NULL,
    name TEXT NOT NULL,
    -- The MGS response that provides this field indicates that it can be NULL.
    -- But that's only to support old software that we no longer support.
    version TEXT NOT NULL
);
