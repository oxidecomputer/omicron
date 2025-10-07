CREATE UNIQUE INDEX IF NOT EXISTS tuf_repo_not_pruned
    ON omicron.public.tuf_repo (id)
    WHERE time_pruned IS NULL;
