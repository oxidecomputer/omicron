-- Change the version column from STRING(63) to STRING(64) so it can fit a
-- hex-encoded SHA-256 hash.
ALTER TABLE omicron.public.tuf_artifact
ALTER COLUMN version TYPE STRING(64);
