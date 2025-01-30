CREATE UNIQUE INDEX IF NOT EXISTS lookup_anti_affinity_group_by_project ON omicron.public.anti_affinity_group (
    project_id,
    name
) WHERE
    time_deleted IS NULL;

