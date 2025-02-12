-- The software release that should be deployed to the rack.
CREATE TABLE IF NOT EXISTS omicron.public.target_release (
    generation INT8 NOT NULL PRIMARY KEY,
    time_requested TIMESTAMPTZ NOT NULL,
    release_source omicron.public.target_release_source NOT NULL,
    system_version STRING(64), -- "foreign key" into the `tuf_repo` table
    CONSTRAINT system_version_for_release CHECK (
      (release_source != 'system_version') OR
      (release_source = 'system_version' AND system_version IS NOT NULL)
    )
);
