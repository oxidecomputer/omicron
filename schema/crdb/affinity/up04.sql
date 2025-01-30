CREATE UNIQUE INDEX IF NOT EXISTS lookup_affinity_group_by_project ON omicron.public.affinity_group (
    project_id,
    name
) WHERE
    time_deleted IS NULL;


