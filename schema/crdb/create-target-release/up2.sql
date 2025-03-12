-- Software releases that should be/have been deployed to the rack. The
-- current target release is the one with the largest generation number.
CREATE TABLE IF NOT EXISTS omicron.public.target_release (
    generation INT8 NOT NULL PRIMARY KEY,
    time_requested TIMESTAMPTZ NOT NULL,
    release_source omicron.public.target_release_source NOT NULL,
    tuf_repo_id UUID, -- "foreign key" into the `tuf_repo` table
    CONSTRAINT tuf_repo_for_system_version CHECK (
      (release_source != 'system_version' AND tuf_repo_id IS NULL) OR
      (release_source = 'system_version' AND tuf_repo_id IS NOT NULL)
    )
);
