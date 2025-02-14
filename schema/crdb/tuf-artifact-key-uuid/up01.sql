-- At the time of this migration, no racks are able to use the tuf_artifact
-- and tuf_repo_artifact tables outside of testing; even if they were, those
-- artifacts couldn't be durably stored. Drop these tables and recreate them.
DROP TABLE IF EXISTS omicron.public.tuf_artifact
